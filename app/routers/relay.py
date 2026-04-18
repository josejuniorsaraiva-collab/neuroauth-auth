"""
app/routers/relay.py
Relay endpoints — frontend-facing API that proxies to Make.com webhooks.

Routes:
  GET  /relay/profile?email=X          -> profile lookup via Make webhook
  GET  /relay/profile?procedimento=X   -> procedure lookup via Make webhook
  POST /relay/notify                   -> forward submission payload to Make webhook

Segurança: JWT obrigatório (Gate A) + rate limit + idempotência + trace_id.
"""
import logging
import hashlib
import time
import uuid
import json
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional
from slowapi import Limiter
from slowapi.util import get_remote_address
from app.core.config import settings
from app.core.security import require_authorized

logger = logging.getLogger("neuroauth.relay")
router = APIRouter()
limiter = Limiter(key_func=get_remote_address)

# ── Idempotência (in-memory, suficiente para single-instance Render) ──
_RELAY_IDEMPOTENCY: dict = {}
_RELAY_IDEM_WINDOW = 300  # 5 minutos


def _relay_idem_key(body: dict, user_email: str) -> str:
    """Hash determinístico do payload + user para detectar duplo envio."""
    raw = json.dumps(body, sort_keys=True, default=str) + "|" + user_email
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


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


# ── GET /relay/profile ──────────────────────────────────────
@router.get("/profile")
@limiter.limit("20/minute")
async def relay_profile(
    request: Request,
    email: Optional[str] = Query(None),
    procedimento: Optional[str] = Query(None),
    user: dict = Depends(require_authorized),
):
    """
    Busca perfil do médico ou dados do procedimento via Make.com.
    Inclui fallback alpha para emails autorizados sem perfil no Sheets.
    Rate limit: 20/min por IP.
    """
    trace_id = f"RL-{str(uuid.uuid4())[:8].upper()}"
    url = _get_webhook_url("profile")
    params: dict = {}
    if email:
        params["email"] = email
    if procedimento:
        params["procedimento"] = procedimento
    if not params:
        raise HTTPException(status_code=400, detail="Informe email ou procedimento.")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)
        logger.info("[%s] relay_profile GET -> %d (user=%s, email=%s, proc=%s)",
                    trace_id, resp.status_code, user.get("email", "?"),
                    email or "-", procedimento or "-")

        # Fallback alpha: emails autorizados sem perfil no Sheets
        if email:
            from app.core.security import AUTHORIZED_EMAILS
            if email.lower() in AUTHORIZED_EMAILS:
                has_valid_profile = False
                if resp.headers.get("content-type", "").startswith("application/json"):
                    try:
                        body_json = resp.json()
                        has_valid_profile = bool(body_json.get("user_email"))
                    except Exception:
                        has_valid_profile = False

                if not has_valid_profile:
                    logger.info("Relay: fallback alpha para %s", email)
                    fallback_perfil = {
                        "user_email":            email,
                        "medico_nome":           email.split("@")[0],
                        "perfil_tipo":           "medico",
                        "ativo":                 True,
                        "hospital_padrao":       "HSA Barbalha",
                        "convenios_habilitados": "Unimed Cariri",
                        "crm":                   "",
                        "cbo":                   "225120",
                    }
                    return JSONResponse(status_code=200, content=fallback_perfil)

        return JSONResponse(
            status_code=resp.status_code,
            content=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text[:500]},
        )
    except httpx.TimeoutException:
        logger.error("Relay profile timeout")
        raise HTTPException(status_code=504, detail="Make.com timeout (30s)")
    except Exception as exc:
        logger.error("Relay profile error: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc))


# ── POST /relay/notify ──────────────────────────────────────
@router.post("/notify")
@limiter.limit("10/minute")
async def relay_notify(request: Request, user: dict = Depends(require_authorized)):
    """
    Encaminha payload de submissão para Make.com webhook (general).
    Requer JWT válido (Gate A). Rate limit: 10/min por IP.
    Idempotência: mesmo payload + user dentro de 5min é ignorado.
    """
    trace_id = f"RL-{str(uuid.uuid4())[:8].upper()}"
    user_email = user.get("email", "?")
    url = _get_webhook_url("general")

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
            content={**cached, "idempotency": "duplicate_ignored", "trace_id": trace_id},
        )

    try:
        # Propagar trace_id no payload para Make.com → Sheets
        body["_trace_id"] = trace_id
        body["_relay_user"] = user_email
        body["_relay_ts"] = time.time()

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=body)

        response_content = {
            "status": "ok" if resp.status_code == 200 else "error",
            "make_status_code": resp.status_code,
            "trace_id": trace_id,
            "detail": resp.text[:500],
        }
        logger.info("[%s] relay_notify POST -> %d (user=%s, idem_key=%s)",
                    trace_id, resp.status_code, user_email, idem_key)

        # Armazenar no cache de idempotência apenas se sucesso
        if resp.status_code == 200:
            _relay_idem_store(idem_key, response_content)

        return JSONResponse(status_code=resp.status_code, content=response_content)
    except httpx.TimeoutException:
        logger.error("[%s] relay_notify timeout (user=%s)", trace_id, user_email)
        raise HTTPException(status_code=504, detail=f"Make.com timeout (30s). trace_id={trace_id}")
    except Exception as exc:
        logger.error("[%s] relay_notify error: %s (user=%s)", trace_id, exc, user_email)
        raise HTTPException(status_code=502, detail=f"{str(exc)[:200]}. trace_id={trace_id}")
