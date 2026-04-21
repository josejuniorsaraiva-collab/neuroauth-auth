"""
app/routers/relay.py
Relay endpoints — frontend-facing API.

Routes:
  GET  /relay/profile?email=X          -> profile lookup (fallback alpha direto)
  GET  /relay/profile?procedimento=X   -> procedure lookup via Make webhook (legacy)
  POST /relay/notify                   -> persiste submissão no Sheets direto (sem Make.com)

Segurança: JWT obrigatório (Gate A) + rate limit + idempotência + trace_id.

NOTA: Make.com removido do fluxo crítico. /relay/notify agora grava direto
no Google Sheets via gspread (tab 24_RELAY_SUBMISSIONS). Make mantido apenas
como fallback para /relay/profile?procedimento (não-crítico).
"""
import logging
import hashlib
import time
import uuid
import json
import httpx
import gspread
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional
from slowapi import Limiter
from app.core.config import settings
from app.core.security import require_authorized

logger = logging.getLogger("neuroauth.relay")
router = APIRouter()


def _get_real_ip(request: Request) -> str:
    """Extrai IP real atrás do proxy Render."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


limiter = Limiter(key_func=_get_real_ip)

# ── Idempotência (in-memory, suficiente para single-instance Render) ──
_RELAY_IDEMPOTENCY: dict = {}
_RELAY_IDEM_WINDOW = 300  # 5 minutos


def _relay_idem_key(body: dict, user_email: str) -> str:
    """Hash determinístico do payload + user para detectar duplo envio.
    JSON canonizado (sort_keys + separators compactos) evita colisão por whitespace."""
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    raw = f"{canonical}|{user_email}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _relay_idem_check(key: str) -> dict | None:
    """Retorna resposta cacheada se dentro da janela, senão None."""
    now = time.time()
    # GC entradas expiradas
    expired = [k for k, (ts, _) in _RELAY_IDEMPOTENCY.items() if now - ts > _RELAY_IDEM_WINDOW]
    for k in expired:
        del _RELAY_IDEMPOTENCY[k]
    entry = _RELAY_IDEMPOTENCY.get(key)
    if entry and (now - entry[0]) < _RELAY_IDEM_WINDOW:
        return entry[1]
    return None


def _relay_idem_store(key: str, response: dict) -> None:
    _RELAY_IDEMPOTENCY[key] = (time.time(), response)


def _get_webhook_url(webhook_type: str) -> str:
    """Resolve webhook URL from settings."""
    webhook_map = {
        "profile": settings.MAKE_WEBHOOK_PROFILE,
        "general": settings.MAKE_WEBHOOK_GENERAL,
    }
    url = webhook_map.get(webhook_type, "")
    if not url:
        raise HTTPException(
            status_code=503,
            detail=f"Webhook '{webhook_type}' não configurado no backend.",
        )
    return url


# ── GET /relay/profile — DIRETO (sem Make.com) ──────────────
@router.get("/profile")
@limiter.limit("20/minute")
async def relay_profile(
    request: Request,
    email: Optional[str] = Query(None),
    procedimento: Optional[str] = Query(None),
    user: dict = Depends(require_authorized),
):
    """
    Busca perfil do médico. Sem Make.com — usa fallback alpha direto.
    Para procedimento, tenta Make como legacy (best-effort).
    Rate limit: 20/min por IP.
    """
    trace_id = f"RL-{str(uuid.uuid4())[:8].upper()}"

    if not email and not procedimento:
        raise HTTPException(status_code=400, detail="Informe email ou procedimento.")

    # ── Perfil por email: direto do backend (sem Make) ──
    if email:
        from app.core.security import AUTHORIZED_EMAILS
        email_lower = email.lower().strip()
        if email_lower in AUTHORIZED_EMAILS:
            logger.info("[%s] relay_profile: fallback alpha direto para %s", trace_id, email_lower)
            fallback_perfil = {
                "user_email":            email_lower,
                "medico_nome":           email_lower.split("@")[0],
                "perfil_tipo":           "medico",
                "ativo":                 True,
                "hospital_padrao":       "HSA Barbalha",
                "convenios_habilitados": "Unimed Cariri",
                "crm":                   "",
                "cbo":                   "225120",
            }
            return JSONResponse(status_code=200, content=fallback_perfil)
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Email {email} não autorizado no NEUROAUTH."
            )

    # ── Procedimento: tenta Make como legacy (best-effort) ──
    if procedimento:
        try:
            url = _get_webhook_url("profile")
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, params={"procedimento": procedimento})
            logger.info("[%s] relay_profile procedimento via Make -> %d", trace_id, resp.status_code)
            if resp.headers.get("content-type", "").startswith("application/json"):
                return JSONResponse(status_code=resp.status_code, content=resp.json())
            return JSONResponse(status_code=200, content={"found": False, "procedimento": procedimento})
        except Exception as exc:
            logger.warning("[%s] relay_profile procedimento fallback (Make indisponível): %s", trace_id, exc)
            return JSONResponse(
                status_code=200,
                content={"found": False, "procedimento": procedimento, "note": "Make.com indisponível"}
            )


# ── POST /relay/notify — DIRETO NO SHEETS (sem Make.com) ──
TAB_RELAY = "24_RELAY_SUBMISSIONS"
TAB_RELAY_HEADER = [
    "timestamp", "trace_id", "user_email", "procedimento", "cid",
    "convenio", "tipo_guia", "classification", "decision_status",
    "decision_run_id", "glosa_probability", "payload_json"
]


def _persist_relay_sheets(trace_id: str, user_email: str, body: dict) -> bool:
    """Grava submissão na tab 24_RELAY_SUBMISSIONS via gspread.
    Síncrono — chamado via background_task ou thread."""
    from app.services.sheets_store import _get_client
    try:
        gc = _get_client()
        ss = gc.open_by_key(settings.SPREADSHEET_ID)
        try:
            ws = ss.worksheet(TAB_RELAY)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(title=TAB_RELAY, rows=1000, cols=len(TAB_RELAY_HEADER))
            ws.append_row(TAB_RELAY_HEADER)
            logger.info("[%s] Tab %s criada automaticamente", trace_id, TAB_RELAY)

        row = [
            datetime.now(timezone.utc).isoformat(),
            trace_id,
            user_email,
            body.get("procedimento", ""),
            body.get("cid", body.get("cid_principal", "")),
            body.get("convenio", ""),
            body.get("tipo_guia", ""),
            body.get("_decide_classification", ""),
            body.get("_decide_status", ""),
            body.get("_decide_run_id", ""),
            str(body.get("glosa_probability", "")),
            json.dumps(body, ensure_ascii=False, default=str)[:40000],  # payload completo (limite cell)
        ]
        ws.append_row(row, value_input_option="RAW")
        logger.info("[%s] relay_notify gravado em %s (user=%s)", trace_id, TAB_RELAY, user_email)
        return True
    except Exception as exc:
        logger.error("[%s] relay_notify sheets error: %s", trace_id, exc)
        return False


@router.post("/notify")
@limiter.limit("10/minute")
async def relay_notify(request: Request, user: dict = Depends(require_authorized)):
    """
    Persiste submissão documental diretamente no Google Sheets.
    Sem dependência do Make.com. Requer JWT válido (Gate A).
    Rate limit: 10/min por IP. Idempotência: 5 min window.
    """
    trace_id = f"RL-{str(uuid.uuid4())[:8].upper()}"
    user_email = user.get("email", "?")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body JSON inválido.")

    # ── Idempotência: rejeita duplo envio dentro de 5 minutos ──
    idem_key = _relay_idem_key(body, user_email)
    cached = _relay_idem_check(idem_key)
    if cached:
        logger.info("[%s] IDEMPOTENCY_HIT key=%s user=%s — ignorando duplicata",
                    trace_id, idem_key, user_email)
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "idempotency": "duplicate_ignored", "trace_id": trace_id},
        )

    try:
        body["_trace_id"] = trace_id
        body["_relay_user"] = user_email
        body["_relay_ts"] = time.time()

        # Gravar direto no Sheets (síncrono em thread pool via run_in_executor)
        import asyncio
        loop = asyncio.get_event_loop()
        ok = await loop.run_in_executor(
            None, _persist_relay_sheets, trace_id, user_email, body
        )

        if not ok:
            logger.error("[%s] relay_notify falhou na persistência Sheets", trace_id)
            raise HTTPException(
                status_code=500,
                detail=f"Falha ao gravar submissão. trace_id={trace_id}"
            )

        response_content = {
            "status": "ok",
            "trace_id": trace_id,
            "persisted_to": TAB_RELAY,
        }

        _relay_idem_store(idem_key, response_content)
        logger.info("[%s] relay_notify OK direto Sheets (user=%s)", trace_id, user_email)

        return JSONResponse(status_code=200, content=response_content)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("[%s] relay_notify error: %s (user=%s)", trace_id, exc, user_email)
        raise HTTPException(status_code=500, detail=f"{str(exc)[:200]}. trace_id={trace_id}")
