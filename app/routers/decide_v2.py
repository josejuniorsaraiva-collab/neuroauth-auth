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

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Optional
from app.core.security import require_authorized
from app.services.structured_logger import NeuroLog
import logging
import os
import uuid
import hashlib
import time
import json
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
        # ── HARDENING V1 REUTILIZADO ──
        # Converte dict→DecideRequest, roda hardening, injeta campos derivados
        try:
            from app.services.input_hardening import run_hardening, _detectar_deficit_motor
            from app.models.decide import DecideRequest as DR, OpmeItem

            # Construir DecideRequest a partir do dict do frontend
            opme_raw = case.get("opme_items", [])
            opme_items = []
            if isinstance(opme_raw, list):
                for o in opme_raw:
                    if isinstance(o, dict):
                        opme_items.append(OpmeItem(
                            descricao=o.get("descricao", ""),
                            qtd=int(o.get("qtd", 1)) if o.get("qtd") else 1,
                            fabricante=o.get("fabricante", ""),
                        ))

            dr = DR(
                cid_principal=case.get("cid_principal", ""),
                procedimento=case.get("procedimento", ""),
                cod_cbhpm=case.get("cod_cbhpm", ""),
                convenio=case.get("convenio", ""),
                indicacao_clinica=case.get("indicacao_clinica", ""),
                achados_resumo=case.get("achados_resumo", ""),
                tto_conservador=case.get("tto_conservador", ""),
                necessita_opme=case.get("necessita_opme", "Não"),
                opme_items=opme_items,
                crm=case.get("crm", ""),
                cbo=case.get("cbo", ""),
                medico_solicitante=case.get("medico_solicitante", ""),
            )

            hr = run_hardening(dr)

            # Injetar campos derivados no case para o motor v2
            case["cid"] = case.get("cid_principal", "") or "CID nao informado"
            case["procedimento_descricao"] = case.get("procedimento", "")
            case["urgencia_caracterizada"] = hr.urgencia_hsa or (
                case.get("carater", "").lower() in ("urgência", "urgencia", "emergência", "emergencia")
            )
            case["deficit_motor_mencionado"] = _detectar_deficit_motor(
                (case.get("indicacao_clinica", "") or "") + " " + (case.get("achados_resumo", "") or "")
            )
            case["cid_format_invalido"] = not bool(
                (case.get("cid_principal", "") or "").strip()
            ) or not (case.get("cid_principal", "") or "")[0:1].isalpha()
            case["crm_presente"] = bool((case.get("crm", "") or "").strip())
            case["assinatura_presente"] = bool((case.get("medico_solicitante", "") or "").strip())
            case["carimbo_presente"] = case["crm_presente"]
            case["justificativa_clinica_length"] = len(
                (case.get("indicacao_clinica", "") or "").strip()
            )
            case["conservador_documentado"] = not hr.conservador_incompleto
            case["imagem_mencionada"] = any(
                t in ((case.get("indicacao_clinica", "") or "") + " " + (case.get("achados_resumo", "") or "")).lower()
                for t in ["rm", "ressonância", "ressonancia", "tc", "tomografia", "mri"]
            )
            case["opme_presente"] = case.get("necessita_opme", "") == "Sim"
            case["opme_cotacoes_count"] = len(opme_items)
            case["lateralidade_ausente"] = not bool((case.get("lateralidade", "") or "").strip())
            case["lateralidade_aplicavel"] = any(
                t in (case.get("procedimento", "") or "").lower()
                for t in ["lombar", "cervical", "craniotomia"]
            )
            case["convenio_perfil"] = case.get("convenio", "DEFAULT")

            # Injetar hardening pendências como campo auxiliar
            case["_hardening_pendencias_count"] = len(hr.pendencias)
            case["_hardening_bloqueios_count"] = len(hr.bloqueios)
            case["_hardening_pre_analise"] = hr.pre_analise_apenas

            logger.info(
                "[v2] hardening OK: pendencias=%d bloqueios=%d deficit=%s cid_ok=%s just_len=%d",
                len(hr.pendencias), len(hr.bloqueios),
                case["deficit_motor_mencionado"],
                not case["cid_format_invalido"],
                case["justificativa_clinica_length"],
            )
        except Exception as e:
            logger.warning("[v2] hardening falhou: %s", e)

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


# ── POST /v2/shadow-log ──────────────────────────────────────
class ShadowLogPayload(BaseModel):
    trace_id_v1: str
    trace_id_v2: str
    v1_gate: str = ""
    v1_classification: str = ""
    v1_score: Optional[int] = None
    v1_glosa_probability: Optional[float] = None
    v2_gate: str = ""
    v2_score: int = 0
    v2_risk: str = ""
    v2_pending_count: int = 0
    v2_pending_items: list = Field(default_factory=list)
    v2_summary: str = ""
    procedimento: str = ""
    convenio: str = ""
    cid: str = ""
    concordancia: bool = False  # v1 e v2 concordam?

    class Config:
        extra = "ignore"


TAB_SHADOW_COMPARE = "23_SHADOW_COMPARE"

# Critérios de virada automática
SHADOW_MIN_CASES = 5           # Mínimo de casos para avaliar
SHADOW_CONCORDANCE_THRESHOLD = 0.7  # 70% concordância mínima
SHADOW_FALSE_NOGO_MAX = 1     # Máximo de falsos NO_GO tolerados


def _persist_shadow_log(payload: ShadowLogPayload, user_email: str) -> None:
    """Background: persiste comparação shadow no Sheets."""
    try:
        from app.services.sheets_store import _get_client
        from app.core.config import settings as cfg

        gc = _get_client()
        ss = gc.open_by_key(cfg.SPREADSHEET_ID)

        # Criar aba se não existir
        try:
            ws = ss.worksheet(TAB_SHADOW_COMPARE)
        except Exception:
            ws = ss.add_worksheet(title=TAB_SHADOW_COMPARE, rows=500, cols=20)
            ws.append_row([
                "timestamp", "user_email",
                "trace_id_v1", "trace_id_v2",
                "procedimento", "convenio", "cid",
                "v1_gate", "v1_score", "v1_glosa_prob",
                "v2_gate", "v2_score", "v2_risk",
                "v2_pending_count", "v2_pending_items",
                "v2_summary", "concordancia",
            ])

        # Determinar concordância
        # GO/GO_COM_RESSALVAS são ambos "aprovação" vs NO_GO/RESSALVA
        v1_aprova = payload.v1_gate in ("GO", "GO_COM_RESSALVAS", "APROVADO")
        v2_aprova = payload.v2_gate in ("GO", "GO_COM_RESSALVAS")
        concordam = v1_aprova == v2_aprova

        ws.append_row([
            datetime.now(timezone.utc).isoformat(),
            user_email,
            payload.trace_id_v1,
            payload.trace_id_v2,
            payload.procedimento,
            payload.convenio,
            payload.cid,
            payload.v1_gate,
            payload.v1_score or "",
            payload.v1_glosa_probability or "",
            payload.v2_gate,
            payload.v2_score,
            payload.v2_risk,
            payload.v2_pending_count,
            json.dumps(payload.v2_pending_items, ensure_ascii=False)[:500],
            payload.v2_summary[:300],
            "SIM" if concordam else "NAO",
        ])
        logger.info("[v2/shadow-log] persisted trace_v1=%s concordancia=%s",
                     payload.trace_id_v1, concordam)
    except Exception as exc:
        logger.warning("[v2/shadow-log] persist failed: %s: %s",
                       type(exc).__name__, str(exc)[:200])


@router.post("/shadow-log")
async def v2_shadow_log(
    payload: ShadowLogPayload,
    background_tasks: BackgroundTasks,
    user: dict = Depends(require_authorized),
):
    """
    Persiste comparação shadow v1 vs v2 no Sheets (tab 23_SHADOW_COMPARE).
    Fire-and-forget: frontend envia após cada shadow e segue.
    """
    user_email = user.get("email", "unknown")
    background_tasks.add_task(_persist_shadow_log, payload, user_email)
    return {"logged": True, "trace_id_v1": payload.trace_id_v1}


# ── GET /v2/shadow-report ───────────────────────────────────
@router.get("/shadow-report")
async def v2_shadow_report(user: dict = Depends(require_authorized)):
    """
    Analisa dados shadow acumulados e recomenda se v2.3.1 está pronto
    para virar motor principal.
    """
    try:
        from app.services.sheets_store import _get_client
        from app.core.config import settings as cfg

        gc = _get_client()
        ss = gc.open_by_key(cfg.SPREADSHEET_ID)

        try:
            ws = ss.worksheet(TAB_SHADOW_COMPARE)
        except Exception:
            return {
                "status": "no_data",
                "total_cases": 0,
                "recommendation": "AGUARDAR",
                "message": "Nenhum dado shadow coletado ainda. Continue enviando casos.",
            }

        rows = ws.get_all_records()
        total = len(rows)

        if total == 0:
            return {
                "status": "no_data",
                "total_cases": 0,
                "recommendation": "AGUARDAR",
                "message": "Aba existe mas sem registros. Envie casos pelo formulário.",
            }

        concordantes = sum(1 for r in rows if r.get("concordancia") == "SIM")
        discordantes = total - concordantes
        taxa_concordancia = concordantes / total if total > 0 else 0

        # Falsos NO_GO: v2 deu NO_GO/RESSALVA mas v1 aprovou
        falsos_nogo = sum(
            1 for r in rows
            if r.get("v1_gate") in ("GO", "GO_COM_RESSALVAS", "APROVADO")
            and r.get("v2_gate") in ("NO_GO", "RESSALVA")
        )

        # v2 mais rigoroso: casos onde v2 pegou mais pendências
        v2_mais_rigoroso = sum(
            1 for r in rows
            if int(r.get("v2_pending_count", 0) or 0) > 0
            and r.get("v1_gate") in ("GO", "GO_COM_RESSALVAS", "APROVADO")
        )

        # Análise por caso
        casos = []
        for r in rows:
            casos.append({
                "timestamp": r.get("timestamp", ""),
                "procedimento": r.get("procedimento", ""),
                "v1_gate": r.get("v1_gate", ""),
                "v2_gate": r.get("v2_gate", ""),
                "v2_risk": r.get("v2_risk", ""),
                "concordancia": r.get("concordancia", ""),
            })

        # Recomendação
        if total < SHADOW_MIN_CASES:
            recommendation = "AGUARDAR"
            message = (
                f"Apenas {total} caso(s) coletado(s). "
                f"Mínimo {SHADOW_MIN_CASES} para avaliar. "
                f"Continue enviando casos."
            )
        elif falsos_nogo > SHADOW_FALSE_NOGO_MAX:
            recommendation = "CALIBRAR"
            message = (
                f"{falsos_nogo} falso(s) NO_GO detectado(s). "
                f"O v2 está bloqueando casos que o v1 aprova. "
                f"Revisar regras antes de promover."
            )
        elif taxa_concordancia >= SHADOW_CONCORDANCE_THRESHOLD:
            recommendation = "PRONTO_PARA_VIRADA"
            message = (
                f"Taxa de concordância {taxa_concordancia:.0%} ({concordantes}/{total}). "
                f"Falsos NO_GO: {falsos_nogo}. "
                f"O v2.3.1 está pronto para virar motor principal."
            )
        else:
            recommendation = "AVALIAR"
            message = (
                f"Taxa de concordância {taxa_concordancia:.0%} ({concordantes}/{total}). "
                f"Abaixo do limiar de {SHADOW_CONCORDANCE_THRESHOLD:.0%}. "
                f"Analisar os {discordantes} caso(s) discordante(s)."
            )

        return {
            "status": "analyzed",
            "total_cases": total,
            "concordantes": concordantes,
            "discordantes": discordantes,
            "taxa_concordancia": round(taxa_concordancia, 3),
            "falsos_nogo_v2": falsos_nogo,
            "v2_mais_rigoroso": v2_mais_rigoroso,
            "recommendation": recommendation,
            "message": message,
            "thresholds": {
                "min_cases": SHADOW_MIN_CASES,
                "concordance_min": SHADOW_CONCORDANCE_THRESHOLD,
                "false_nogo_max": SHADOW_FALSE_NOGO_MAX,
            },
            "casos": casos[-10:],  # últimos 10
        }

    except Exception as exc:
        logger.exception("[v2/shadow-report] erro")
        return {
            "status": "error",
            "recommendation": "ERRO",
            "message": f"Erro ao gerar relatório: {type(exc).__name__}: {str(exc)[:200]}",
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
