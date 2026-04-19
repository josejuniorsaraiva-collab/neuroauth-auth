"""
app/routers/decide_v2.py
=========================
Router PARALELO para o motor decisorio v2.3.1.
Convive com /decide (v1.3) sem interferir.

Endpoints:
  POST /v2/decide    — avalia caso pelo motor v2.3.1 (JWT obrigatorio)
  POST /v2/outcome   — registra desfecho real para learning loop
  GET  /v2/health    — health check com info do motor v2.3.1

Integracoes mantidas do backend existente:
  - JWT auth via require_authorized (Gate A)
  - NeuroLog para rastreabilidade
  - Idempotencia por trace_id (janela 5min)

Autor: NEUROAUTH
Versao: 2.3.1-parallel
Data: 2026-04-19
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Optional
from app.core.security import require_authorized
from app.services.structured_logger import NeuroLog
import logging
import os
import uuid
import hashlib
import time
from datetime import datetime, timezone

logger = logging.getLogger("neuroauth.decide_v2")

router = APIRouter()


# ── Engine v2.3.1 (importacao isolada, nao toca o v1.3) ──────
from app.services.decision_engine_v2 import DecisionEngine, OutcomeRecorder

RULES_PATH = os.getenv(
    "NEUROAUTH_RULES_V2_PATH",
    os.path.join(os.path.dirname(__file__), "..", "services", "rules_v2_1.json"),
)

# ROL ANS 465 — lista minima para bootstrap (expandir via Sheets)
ROL_ANS_465 = [
    "30912033",  # Microdiscectomia lombar
    "30912017",  # Hernia disco lombar via aberta
    "30912025",  # Discectomia cervical
    "30912092",  # Artrodese cervical anterior
    "30201023",  # Embolizacao aneurisma cerebral
]

GLOBAL_CONTEXT = {
    "rol_ans_465": ROL_ANS_465,
}

# Inicializacao defensiva — se o rules nao existir, logar e seguir
# (o /v2/health vai reportar o problema)
_engine: Optional[DecisionEngine] = None
_engine_error: Optional[str] = None

try:
    _engine = DecisionEngine.from_file(RULES_PATH, context=GLOBAL_CONTEXT)
    logger.info(
        "[v2] Motor v2.3.1 carregado: %d regras, camadas=%s",
        len(_engine.rules),
        list(_engine.rules_by_layer.keys()),
    )
except Exception as exc:
    _engine_error = f"{type(exc).__name__}: {str(exc)[:200]}"
    logger.error("[v2] FALHA ao carregar motor v2.3.1: %s", _engine_error)

_recorder = OutcomeRecorder(sheets_client=None)  # injetar sheets_client real depois


# ── Idempotency cache (mesma estrategia do /decide v1) ────────
_V2_TRACE_CACHE: dict = {}  # trace_id -> (timestamp, response_dict)
V2_IDEMPOTENCY_WINDOW_SEC = 300


def _v2_gc_cache() -> None:
    now = time.time()
    expired = [k for k, (ts, _) in _V2_TRACE_CACHE.items()
               if now - ts > V2_IDEMPOTENCY_WINDOW_SEC]
    for k in expired:
        del _V2_TRACE_CACHE[k]


def _v2_check_cache(trace_id: str) -> Optional[dict]:
    _v2_gc_cache()
    entry = _V2_TRACE_CACHE.get(trace_id)
    if entry and (time.time() - entry[0]) < V2_IDEMPOTENCY_WINDOW_SEC:
        return entry[1]
    return None


def _v2_store_cache(trace_id: str, response: dict) -> None:
    _V2_TRACE_CACHE[trace_id] = (time.time(), response)


# ── Schemas ───────────────────────────────────────────────────
class V2DecideResponse(BaseModel):
    ok: bool = True
    trace_id: str
    engine_version: str = "2.3.1"
    final_gate: str
    final_score: int
    final_risk: str
    pending_items: list = Field(default_factory=list)
    recommended_action: str = ""
    summary: str = ""
    rules_fired: list = Field(default_factory=list)
    defense_text: str = ""
    idempotent_replay: bool = False
    timestamp: str = ""

    class Config:
        extra = "allow"


class V2OutcomePayload(BaseModel):
    trace_id: str
    motor_result: dict
    real_outcome: dict


# ── POST /v2/decide ──────────────────────────────────────────
@router.post("/decide", response_model=V2DecideResponse)
async def v2_decide(
    case: dict,
    user: dict = Depends(require_authorized),
):
    """
    Avalia caso clinico pelo motor v2.3.1.
    JWT obrigatorio. Fail-safe: erro → RESSALVA + CRITICO_ERRO_SISTEMA.
    """
    if _engine is None:
        raise HTTPException(
            status_code=503,
            detail=f"Motor v2.3.1 nao carregado: {_engine_error}",
        )

    # trace_id: usar do payload ou gerar
    trace_id = case.get("trace_id") or f"TR-{str(uuid.uuid4())[:12].upper()}"
    user_email = user.get("email", "unknown")
    t_start = datetime.now(timezone.utc)

    log = NeuroLog(
        trace_id=trace_id,
        episode_id=case.get("episodio_id", ""),
        service_name="neuroauth.decide_v2",
    )

    # Idempotencia por trace_id
    cached = _v2_check_cache(trace_id)
    if cached:
        logger.info("[v2] IDEMPOTENCY_HIT trace=%s", trace_id)
        replay = V2DecideResponse(**cached)
        replay.idempotent_replay = True
        return replay

    log.emit("request_received", status="ok", details={
        "engine": "v2.3.1",
        "user_email": user_email,
        "procedimento": case.get("procedimento", ""),
        "convenio": case.get("convenio", ""),
        "cid_principal": case.get("cid_principal", ""),
    })

    try:
        # Executar motor v2.3.1
        log.emit("decision_started", status="ok", details={
            "engine_version": "v2.3.1",
            "ruleset": "rules_v2_1.json",
            "rules_count": len(_engine.rules),
        })

        result = _engine.evaluate(case)

        # Montar resposta padronizada
        response = V2DecideResponse(
            ok=True,
            trace_id=result.get("trace_id", trace_id),
            engine_version="2.3.1",
            final_gate=result.get("final_gate", "UNKNOWN"),
            final_score=result.get("final_score", 0),
            final_risk=result.get("final_risk", "UNKNOWN"),
            pending_items=result.get("pending_items", []),
            recommended_action=result.get("recommended_action", ""),
            summary=result.get("summary", ""),
            rules_fired=result.get("rules_fired", []),
            defense_text=result.get("defense_text", ""),
            idempotent_replay=False,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # Log resultado
        latency = int((datetime.now(timezone.utc) - t_start).total_seconds() * 1000)
        log.emit("decision_result", status="ok", details={
            "final_gate": response.final_gate,
            "final_score": response.final_score,
            "final_risk": response.final_risk,
            "rules_fired_count": len(response.rules_fired),
            "pending_count": len(response.pending_items),
            "latency_ms": latency,
        })

        # Cache para idempotencia
        _v2_store_cache(trace_id, response.model_dump())

        log.emit("response_sent", status="ok", details={
            "http_status": 200,
            "total_latency_ms": latency,
        }, latency_ms=latency)

        return response

    except Exception as exc:
        log.error(
            failed_stage="v2_decide_endpoint",
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )
        logger.exception("[v2] Erro inesperado trace=%s", trace_id)

        # Fail-safe: retornar RESSALVA, jamais 500 silencioso
        return V2DecideResponse(
            ok=False,
            trace_id=trace_id,
            engine_version="2.3.1",
            final_gate="RESSALVA",
            final_score=0,
            final_risk="CRITICO_ERRO_SISTEMA",
            pending_items=[],
            recommended_action="REVISAR_MANUALMENTE",
            summary=f"Erro no motor v2.3.1: {type(exc).__name__}. Caso requer revisao manual.",
            rules_fired=[],
            defense_text="",
            idempotent_replay=False,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )


# ── POST /v2/outcome ────────────────────────────────────────
@router.post("/outcome")
async def v2_outcome(
    payload: V2OutcomePayload,
    user: dict = Depends(require_authorized),
):
    """
    Registra desfecho real da operadora para o learning loop.
    Idempotente por trace_id. JWT obrigatorio.
    """
    user_email = user.get("email", "unknown")
    logger.info(
        "[v2/outcome] trace=%s user=%s outcome=%s",
        payload.trace_id, user_email,
        payload.real_outcome.get("decisao", "?"),
    )
    recorded = _recorder.record(
        payload.trace_id,
        payload.motor_result,
        payload.real_outcome,
    )
    return {
        "recorded": recorded,
        "trace_id": payload.trace_id,
        "engine_version": "2.3.1",
    }


# ── GET /v2/health ───────────────────────────────────────────
@router.get("/health")
async def v2_health():
    """Health check do motor v2.3.1 — nao requer JWT."""
    if _engine is None:
        return {
            "status": "error",
            "engine_version": "2.3.1",
            "error": _engine_error,
            "rules_loaded": 0,
        }

    return {
        "status": "ok",
        "engine_version": "2.3.1",
        "rules_loaded": len(_engine.rules),
        "rules_by_layer": {
            camada: len(regras)
            for camada, regras in _engine.rules_by_layer.items()
        },
        "perfis_disponiveis": list(_engine.perfis.keys()),
        "context_keys": list(GLOBAL_CONTEXT.keys()),
        "coexistence": {
            "v1_endpoint": "/decide",
            "v2_endpoint": "/v2/decide",
            "note": "Ambos ativos. v1 = producao, v2 = shadow/homologacao.",
        },
    }
