"""
app/services/decision_engine.py
Motor de decisão v1.3 — 3 camadas determinísticas + gate binário.

Arquitetura:
  CAMADA 1: Validação estrutural (PASS/FAIL)
  CAMADA 2: Validação clínica (score 0-1 + flags)
  CAMADA 3: Anti-glosa (risco + falhas + correções)
  GATE: estrutural_fail→NO_GO | risco_alto→NO_GO | score<0.6→RESSALVAS | else→GO

Backward-compatible: run_decision() still returns DecideResponse.
"""

from app.models.decide import DecideRequest, DecideResponse
from app.services.input_hardening import run_hardening
from app.services.engine_v3 import (
    run_engine,
    ENGINE_VERSION,
    normalize_input,
    run_structural_validation,
    run_clinical_validation,
    run_anti_glosa,
)
from datetime import datetime
import uuid
import logging

logger = logging.getLogger("neuroauth.decide")



def run_decision(req: DecideRequest) -> DecideResponse:
    """
    Entry point — delega ao engine_v3 (3 camadas) e retorna DecideResponse.
    Mantém retrocompatibilidade total com frontend v3 e sheets_store.
    """
    # INPUT HARDENING (mantém para TUSS normalization + PRE_ANALISE check)
    h = run_hardening(req)
    tuss_efetivo = h.tuss_normalizado or req.cod_cbhpm

    # PRÉ-ANÁLISE: convênio ausente bloqueia decisão final
    if h.pre_analise_apenas:
        return DecideResponse(
            decision_run_id=f"DR-{str(uuid.uuid4())[:8].upper()}",
            episodio_id=req.episodio_id,
            classification="PRE_ANALISE_APENAS",
            decision_status="PRE_ANALISE",
            score=None,
            justificativa=(
                "Análise clínica estruturada — decisão final bloqueada. "
                "Convênio ausente: não é possível aplicar regras regulatórias. "
                f"Procedimento: {req.procedimento} | CID: {req.cid_principal}."
            ),
            pendencias=h.pendencias,
            bloqueios=h.bloqueios,
            risco_glosa="indeterminado",
            pontos_frageis=[],
            proximos_passos=[
                "Informar o convênio do paciente.",
                "Resubmeter após inclusão do convênio para decisão final.",
            ],
            tuss_normalizado=tuss_efetivo,
            timestamp=datetime.utcnow().isoformat(),
        )

    # ── MOTOR v1.3 — 3 CAMADAS ──────────────────────────────────
    engine_out = run_engine(req)

    # Merge hardening pendencias + bloqueios
    all_pendencias = list(h.pendencias) + engine_out.pendencias
    all_bloqueios = list(h.bloqueios) + engine_out.bloqueios

    # Deduplicate
    seen_p = set()
    dedup_pendencias = []
    for p in all_pendencias:
        if p not in seen_p:
            seen_p.add(p)
            dedup_pendencias.append(p)

    seen_b = set()
    dedup_bloqueios = []
    for b in all_bloqueios:
        if b not in seen_b:
            seen_b.add(b)
            dedup_bloqueios.append(b)

    logger.info(
        f"[decide] v{ENGINE_VERSION} decision={engine_out.decision} "
        f"score={engine_out.score_100} risco={engine_out.risco_glosa} "
        f"gate={engine_out.gate_reason} tempo={engine_out.tempo_execucao_ms}ms"
    )

    return DecideResponse(
        decision_run_id=f"DR-{str(uuid.uuid4())[:8].upper()}",
        episodio_id=req.episodio_id,
        classification=engine_out.decision,
        decision_status=engine_out.decision_status,
        score=engine_out.score_100,
        justificativa=engine_out.justificativa_final,
        pendencias=dedup_pendencias,
        bloqueios=dedup_bloqueios,
        risco_glosa=engine_out.risco_glosa,
        pontos_frageis=engine_out.pontos_frageis,
        proximos_passos=engine_out.correcoes,  # correções como próximos passos
        tuss_normalizado=tuss_efetivo,
        timestamp=datetime.utcnow().isoformat(),
        # v1.3 extended fields
        falhas=engine_out.falhas,
        correcoes=engine_out.correcoes,
        score_clinico=engine_out.score_clinico,
        motor_version=ENGINE_VERSION,
        camada1=engine_out.camada1_resultado,
        camada2_score=engine_out.camada2_score,
        camada3_risco=engine_out.camada3_risco,
        gate_reason=engine_out.gate_reason,
        tempo_execucao_ms=engine_out.tempo_execucao_ms,
    )
