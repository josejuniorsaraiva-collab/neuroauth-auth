"""
app/routers/decide.py
POST /decide — endpoint principal. Requer JWT válido.
Instrumentado com NeuroLog (Noite 6) para rastreabilidade completa.
Idempotência: payload_hash + user + janela 5min → rejeita duplicatas.
"""

from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from app.models.decide import DecideRequest, DecideResponse
from app.services.decision_engine import run_decision
from app.services.sheets_store import persist_decision, verify_persistence
from app.services.structured_logger import NeuroLog
from app.core.security import require_authorized
from app.core.config import settings
import httpx
import logging
import uuid
import hashlib
import time
from datetime import datetime, timezone

router = APIRouter()
logger = logging.getLogger("neuroauth.decide")

# ── Idempotency cache (in-memory, suficiente para single-instance Render) ──
# key: idempotency_key, value: (timestamp, DecideResponse.dict())
_IDEMPOTENCY_CACHE: dict = {}
IDEMPOTENCY_WINDOW_SEC = 300  # 5 minutos


def _compute_idempotency_key(req: DecideRequest, user_email: str) -> str:
    """Hash determinístico do payload + user para detectar duplo envio."""
    raw = f"{user_email}|{req.cid_principal}|{req.procedimento}|{req.convenio}|{req.indicacao_clinica}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _check_idempotency(key: str) -> dict | None:
    """Retorna resposta cacheada se dentro da janela, senão None."""
    now = time.time()
    # Limpar entradas antigas (garbage collect)
    expired = [k for k, (ts, _) in _IDEMPOTENCY_CACHE.items() if now - ts > IDEMPOTENCY_WINDOW_SEC]
    for k in expired:
        del _IDEMPOTENCY_CACHE[k]
    # Verificar cache
    entry = _IDEMPOTENCY_CACHE.get(key)
    if entry and (now - entry[0]) < IDEMPOTENCY_WINDOW_SEC:
        return entry[1]
    return None


def _store_idempotency(key: str, response_dict: dict) -> None:
    """Armazena resposta no cache de idempotência."""
    _IDEMPOTENCY_CACHE[key] = (time.time(), response_dict)


@router.post("", response_model=DecideResponse)
async def decide(
    req: DecideRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(require_authorized),
):
    # trace_id: usar do request (frontend v3) ou gerar
    trace_id = req.trace_id or f"TR-{str(uuid.uuid4())[:12].upper()}"
    t_start = datetime.now(timezone.utc)
    user_email = user.get("email", "unknown")

    # ── Idempotência: detectar duplo envio ──
    idem_key = _compute_idempotency_key(req, user_email)
    cached = _check_idempotency(idem_key)
    if cached:
        logger.info(
            f"[decide] IDEMPOTENCY_HIT key={idem_key} trace={trace_id} "
            f"returning cached run={cached.get('decision_run_id')}"
        )
        return DecideResponse(**cached)

    log = NeuroLog(
        trace_id=trace_id,
        episode_id=req.episodio_id,
        service_name="neuroauth.decide",
    )

    # Evento 1 — request_received
    payload_hash = hashlib.sha256(
        f"{req.cid_principal}{req.procedimento}{req.convenio}".encode()
    ).hexdigest()[:12]

    log.emit("request_received", status="ok", details={
        "payload_hash":    payload_hash,
        "idempotency_key": idem_key,
        "request_origin":  "api",
        "user_email":      user_email,
        "procedimento":    req.procedimento,
        "convenio":        req.convenio,
        "cid_principal":   req.cid_principal,
    })

    try:
        # Evento 2 — decision_started
        log.emit("decision_started", status="ok", details={
            "engine_version":  "v2.2",
            "ruleset_version": "lote02+noite4",
        })

        # Executar motor
        resultado: DecideResponse = run_decision(req)

        # Popular campos extras para frontend v3
        resultado.ok = True
        resultado.decision = resultado.classification
        resultado.trace_id = trace_id
        resultado.ts = resultado.timestamp
        resultado.motor_version = "1.0"

        # Atualizar run_id no log agora que foi gerado
        log.set_run_id(resultado.decision_run_id)

        # Evento 3 — rules_applied (pendências + pontos frágeis do motor)
        log.emit("rules_applied", status="ok", details={
            "rule_codes":        ["C001","C002","C003","CL001","CL002","CL003",
                                  "R001","R002","R003","R004",
                                  "CV001","CV002","CV003","CV004"],
            "rule_count":        14,
            "warnings_count":    len(resultado.pontos_frageis),
            "pendencias_count":  len(resultado.pendencias),
            "bloqueios_count":   len(resultado.bloqueios),
        })

        # Evento 4 — decision_result
        log.emit("decision_result", status="ok", details={
            "final_decision":      resultado.classification,
            "decision_status":     resultado.decision_status,
            "score":               resultado.score,
            "risco_glosa":         resultado.risco_glosa,
            "justificativa_resumo": resultado.justificativa[:120],
        })

        # Persistência em background com log estruturado
        background_tasks.add_task(
            _persist_and_verify,
            req=req,
            res=resultado,
            log=log,
        )

        if settings.MAKE_DOC_WEBHOOK:
            background_tasks.add_task(
                _dispatch_make_docs,
                req=req,
                res=resultado,
                user_email=user_email,
            )

        # ── Armazenar no cache de idempotência ──
        _store_idempotency(idem_key, resultado.model_dump())

        # Evento 8 — response_sent
        latency = int((datetime.now(timezone.utc) - t_start).total_seconds() * 1000)
        log.emit("response_sent", status="ok", details={
            "http_status":      200,
            "total_latency_ms": latency,
            "idempotency_key":  idem_key,
        }, latency_ms=latency)

        return resultado

    except Exception as exc:
        log.error(
            failed_stage="decide_endpoint",
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )
        logger.exception(f"[decide] Erro inesperado trace={trace_id}")
        raise HTTPException(status_code=500, detail="Erro interno no motor de decisão.")


async def _persist_and_verify(
    req: DecideRequest,
    res: DecideResponse,
    log: NeuroLog,
) -> None:
    """Background: persiste e verifica com logs estruturados."""
    try:
        # Evento 5 — persist_start
        log.emit("persist_start", status="ok", details={
            "target_ledger":  "21_DECISION_RUNS",
            "target_episode": "22_EPISODIOS",
            "run_id":         res.decision_run_id,
            "episode_id":     res.episodio_id,
        })

        ok = persist_decision(req, res)

        if not ok:
            log.error(
                failed_stage="persist_decision",
                error_type="PersistenceFailure",
                error_message="persist_decision retornou False — ver logs neuroauth.sheets",
            )
            return

        # Evento 6 — persist_success
        log.emit("persist_success", status="ok", details={
            "persisted_decision_run_id": res.decision_run_id,
            "target_ledger":   "21_DECISION_RUNS",
            "target_episode":  "22_EPISODIOS",
        })

        # Verificar escrita com retry
        verify = verify_persistence(req, res, max_retries=3)

        if verify["veredicto"] in ("OK", "OK_SEM_CORRELACAO"):
            # Evento 7 — verify_success
            log.emit("verify_success", status="ok", details={
                "verification_method":  "retry_read",
                "correlation_ok":       verify["veredicto"] == "OK",
                "final_episode_status": res.decision_status,
                "tentativas":           verify["tentativas"],
                "veredicto":            verify["veredicto"],
            })
        else:
            log.error(
                failed_stage="verify_persistence",
                error_type="VerificationFailure",
                error_message="verify_persistence retornou BLOQUEADO após retries",
                details={"detalhes": verify["detalhes"]},
            )

    except Exception as exc:
        log.error(
            failed_stage="persist_and_verify",
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )


async def _dispatch_make_docs(
    req: DecideRequest,
    res: DecideResponse,
    user_email: str,
) -> None:
    """Periférico: falha aqui não afeta a decisão."""
    try:
        payload = {
            "decision_run_id": res.decision_run_id,
            "episodio_id":     res.episodio_id,
            "classification":  res.classification,
            "decision_status": res.decision_status,
            "justificativa":   res.justificativa,
            "procedimento":    req.procedimento,
            "convenio":        req.convenio,
            "cid_principal":   req.cid_principal,
            "user_email":      user_email,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(settings.MAKE_DOC_WEBHOOK, json=payload)
    except Exception as e:
        logger.warning(
            f"[decide] Make dispatch falhou: {type(e).__name__}: {str(e)[:80]}"
        )
