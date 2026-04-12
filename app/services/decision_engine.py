"""
app/services/decision_engine.py
NEUROAUTH — Decision Engine v2.0.0

Orquestrador final das 3 camadas de validação.
Não reimplementa regras — apenas consome saídas dos validadores e consolida.

Responsabilidades:
  1. Chamar os 3 validators em ordem (ANS → Evidência → Operadora)
  2. Consolidar score, gate, risco e pendências
  3. Resolver conflitos entre camadas
  4. Produzir decisão final rastreável e explicável

Gates finais: GO | GO_COM_RESSALVAS | NO_GO | JUNTA

Saída principal:
{
  "final_gate":            "GO" | "GO_COM_RESSALVAS" | "NO_GO" | "JUNTA",
  "final_score":           int (0–100),
  "final_risk":            "BAIXO" | "MODERADO" | "ALTO" | "CRITICO",
  "layer_status":          {"ANS": ..., "EVIDENCIA": ..., "OPERADORA": ...},
  "blocking_rules":        list[str],
  "warning_rules":         list[str],
  "pending_items":         list[str],
  "opme_flags":            list[str],
  "summary":               str,
  "recommended_next_action": str,
  "defense_ready":         bool,
  "decision_trace":        list[dict],
  "layer_results":         dict,
}
"""
from __future__ import annotations

import logging
import time
from typing import Any

from app.services.validator_ans       import run_ans_validation
from app.services.validator_evidencia import run_evidencia_validation
from app.services.validator_operadora import run_operadora_validation

logger = logging.getLogger("neuroauth.decision_engine")

ENGINE_VERSION = "2.0.0"

# ══════════════════════════════════════════════════════════════════════════════
# PESOS DE SCORE POR CAMADA
# ANS:      base 50  (pass = 50, cada falha reduz conforme score_impact)
# Evidência:base 30  (normalizado a partir de evidence_score 0–1)
# Operadora:base 20  (normalizado a partir de score_delta_total)
# ══════════════════════════════════════════════════════════════════════════════
_ANS_WEIGHT  = 50
_EVID_WEIGHT = 30
_OP_WEIGHT   = 20

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _ans_score(r_ans: dict) -> float:
    """ANS: 50 pts base, reduz conforme score_impact acumulado (negativo)."""
    base    = _ANS_WEIGHT
    impact  = r_ans.get("score_impact", 0)           # negativo
    penalty = abs(impact) / 100 * _ANS_WEIGHT         # proporcional
    return _clamp(base - penalty, 0, _ANS_WEIGHT)


def _evid_score(r_evid: dict) -> float:
    """Evidência: evidence_score 0–1 → 0–30 pts."""
    s = r_evid.get("evidence_score", 0.6)
    return _clamp(s * _EVID_WEIGHT, 0, _EVID_WEIGHT)


def _op_score(r_op: dict) -> float:
    """Operadora: parte de 20, reduz conforme delta negativo."""
    base   = _OP_WEIGHT
    delta  = r_op.get("score_delta_total", 0)          # negativo
    penalty = abs(min(delta, 0)) / 100 * _OP_WEIGHT
    return _clamp(base - penalty, 0, _OP_WEIGHT)


def _gate_priority(gates: list[str]) -> str:
    """Retorna o gate mais restritivo da lista."""
    order = ["NO_GO", "JUNTA", "GO_COM_RESSALVAS", "GO"]
    for g in order:
        if g in gates:
            return g
    return "GO"


def _risk_from_score(score: int, blocking: bool,
                     junta: bool, n_warnings: int) -> str:
    if blocking:                           return "CRITICO"
    if score < 40 or junta:                return "ALTO"
    if score < 60 or n_warnings >= 3:      return "MODERADO"
    return "BAIXO"


# ══════════════════════════════════════════════════════════════════════════════
# LÓGICA DE CONSOLIDAÇÃO DE GATE
# ══════════════════════════════════════════════════════════════════════════════

def _consolidate_gate(r_ans: dict, r_evid: dict, r_op: dict) -> tuple[str, list[str]]:
    """
    Consolida gate final a partir das 3 camadas.
    Retorna (gate, trace_msgs).

    Regras em ordem de prioridade:
      R1: ANS bloqueante       → NO_GO imediato
      R2: Evidência JUNTA      → JUNTA (exceto se ANS já definiu NO_GO)
      R3: Evidência CINZENTA   → GO_COM_RESSALVAS mínimo
      R4: Operadora restritiva → GO_COM_RESSALVAS
      R5: Tudo forte           → GO
      R6: Conflito evidência forte × operadora restritiva
          → preserva GO_COM_RESSALVAS, não derruba para NO_GO
    """
    trace: list[str] = []
    gate  = "GO"

    # R1 — ANS bloqueante
    if r_ans.get("blocking"):
        gate = "NO_GO"
        trace.append(
            f"R1·ANS_BLOCK: {r_ans.get('blocked_by','?')} bloqueou — "
            f"{_first_fail_msg(r_ans)}"
        )
        return gate, trace

    # R2 — Evidência manda para Junta
    evid_gate = r_evid.get("gate", "GO")
    if evid_gate == "JUNTA" or r_evid.get("junta_rules"):
        gate = "JUNTA"
        juntas = r_evid.get("junta_rules", [])
        trace.append(f"R2·EVID_JUNTA: {juntas} → revisão técnica necessária")

    # R3 — Evidência fraca (CINZENTA) → mínimo GO_COM_RESSALVAS
    strength = r_evid.get("clinical_strength", "MODERADA")
    if strength in ("FRACA", "CINZENTA") and gate == "GO":
        gate = "GO_COM_RESSALVAS"
        trace.append(f"R3·EVID_FRACA: clinical_strength={strength} → GO_COM_RESSALVAS")

    # R4 — Operadora com pendências
    op_gate = r_op.get("gate", "GO")
    op_risk = r_op.get("operator_risk_level", "BAIXO")
    if op_gate == "GO_COM_RESSALVAS" or op_risk in ("ALTO", "CRITICO"):
        if gate == "GO":
            gate = "GO_COM_RESSALVAS"
            trace.append(f"R4·OP_PENDENCIA: risk={op_risk} → GO_COM_RESSALVAS")

    # R6 — Conflito: evidência FORTE mas operadora ALTO/CRITICO
    # Não derruba para NO_GO. Mantém GO_COM_RESSALVAS com trilha explícita.
    if strength == "FORTE" and op_risk in ("ALTO", "CRITICO") and gate == "NO_GO":
        gate = "GO_COM_RESSALVAS"
        trace.append(
            "R6·CONFLITO: evidência FORTE conflita com operadora restritiva — "
            "mantém GO_COM_RESSALVAS (não NO_GO cego)"
        )

    # R5 — Tudo forte → GO
    if (not r_ans.get("failed_rules") and
            strength in ("FORTE", "MODERADA") and
            op_risk == "BAIXO" and
            gate == "GO"):
        trace.append("R5·TUDO_FORTE: ANS pass + evidência forte + operadora ok → GO")

    if not trace:
        trace.append(f"GATE_PADRAO: {gate}")

    return gate, trace


def _first_fail_msg(r: dict) -> str:
    for res in r.get("results", []):
        if not res.get("passed") and res.get("user_message"):
            return res["user_message"][:100]
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUÇÃO DO SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

_GATE_LABELS = {
    "GO":              "Autorizado",
    "GO_COM_RESSALVAS":"Autorizado com ressalvas",
    "NO_GO":           "Negado",
    "JUNTA":           "Encaminhado para junta médica",
}

def _build_summary(gate: str, r_ans: dict, r_evid: dict, r_op: dict,
                   score: int, risk: str) -> str:
    label    = _GATE_LABELS.get(gate, gate)
    op_name  = r_op.get("operator_name", "operadora")
    strength = r_evid.get("clinical_strength", "?")
    ans_stat = "aprovado" if not r_ans.get("failed_rules") else f"falhou em {r_ans.get('blocked_by') or r_ans.get('failed_rules',[{}])[0]}"
    parts    = [
        f"{label}.",
        f"ANS: {ans_stat}.",
        f"Evidência clínica: {strength.lower()}.",
        f"Risco operadora ({op_name}): {r_op.get('operator_risk_level','?').lower()}.",
        f"Score final: {score}/100.",
        f"Risco global: {risk.lower()}.",
    ]
    if r_ans.get("failed_rules"):
        parts.append(f"Falhas ANS: {', '.join(r_ans['failed_rules'])}.")
    if r_evid.get("junta_rules"):
        parts.append(f"Regras que pedem junta: {', '.join(r_evid['junta_rules'])}.")
    if r_op.get("operator_pending_items"):
        n = len(r_op["operator_pending_items"])
        parts.append(f"{n} pendência(s) documental(is) identificada(s).")
    return " ".join(parts)


def _recommended_action(gate: str, r_ans: dict, r_evid: dict, r_op: dict) -> str:
    if gate == "NO_GO":
        blocked = r_ans.get("blocked_by", "")
        return (f"Revisar e corrigir bloqueio regulatório ({blocked}) antes de resubmeter. "
                "Não enviar guia no estado atual.")
    if gate == "JUNTA":
        return ("Submeter caso para revisão por junta médica especializada. "
                "Incluir toda a documentação clínica disponível.")
    if gate == "GO_COM_RESSALVAS":
        pending = r_op.get("operator_pending_items", [])
        if pending:
            return f"Resolver pendências documentais antes de enviar: {pending[0][:80]}."
        evid_fails = r_evid.get("failed_rules", [])
        if evid_fails:
            return f"Fortalecer documentação clínica ({evid_fails[0]}) e enviar com ressalva explicada."
        return "Enviar guia com ressalva documentada. Preparar defesa técnica preventiva."
    return "Enviar guia. Documentação suficiente para autorização."


def _opme_flags(r_ans: dict, r_evid: dict, r_op: dict) -> list[str]:
    flags = []
    for res in r_op.get("results", []):
        if not res.get("passed") and res.get("failure_action") in ("FLAG_OPME",):
            if res.get("user_message"):
                flags.append(res["user_message"][:120])
    return flags


def _all_warnings(r_ans: dict, r_evid: dict, r_op: dict) -> list[str]:
    warns = []
    for layer, r in [("ANS", r_ans), ("EVIDENCIA", r_evid), ("OPERADORA", r_op)]:
        for res in r.get("results", []):
            if (not res.get("passed") and
                    res.get("failure_action") in ("WARN", "SCORE_DOWN", "FLAG_GLOSA")):
                if res.get("user_message"):
                    warns.append(f"[{layer}·{res['rule_id']}] {res['user_message'][:100]}")
    return warns


def _all_pending(r_ans: dict, r_evid: dict, r_op: dict) -> list[str]:
    pending = []
    for layer, r in [("ANS", r_ans), ("EVIDENCIA", r_evid), ("OPERADORA", r_op)]:
        for res in r.get("results", []):
            if (not res.get("passed") and
                    res.get("failure_action") in ("REQUEST_DOC", "FLAG_OPME")):
                if res.get("user_message"):
                    pending.append(f"[{layer}] {res['user_message'][:100]}")
    return pending


def _defense_ready(gate: str, r_ans: dict, r_evid: dict) -> bool:
    """Defesa técnica está pronta quando: não é NO_GO + ANS passou + evidência ≥ MODERADA."""
    if gate == "NO_GO":
        return False
    if r_ans.get("blocking"):
        return False
    strength = r_evid.get("clinical_strength", "FRACA")
    return strength in ("FORTE", "MODERADA")


# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE PÚBLICA PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def decide(ctx: dict) -> dict:
    """
    Ponto de entrada principal do motor v2.0.

    ctx: dict com todos os campos do caso clínico.
         Mesmo dict passado aos 3 validators.

    Retorna decisão completa e rastreável.
    """
    t0 = time.time()

    # ── 1. Executar os 3 validators ──────────────────────────────────────────
    r_ans   = run_ans_validation(ctx)
    r_evid  = run_evidencia_validation(ctx)
    r_op    = run_operadora_validation(ctx)

    # ── 2. Calcular score final ponderado ────────────────────────────────────
    s_ans  = _ans_score(r_ans)
    s_evid = _evid_score(r_evid)
    s_op   = _op_score(r_op)
    final_score = int(_clamp(s_ans + s_evid + s_op, 0, 100))

    # ── 3. Consolidar gate ───────────────────────────────────────────────────
    gate, gate_trace = _consolidate_gate(r_ans, r_evid, r_op)

    # ── 4. Coletar blocking, warnings, pending ───────────────────────────────
    blocking_rules = (
        ([r_ans.get("blocked_by")] if r_ans.get("blocked_by") else []) +
        [r for r in r_evid.get("failed_rules", [])
         if any(res.get("rule_id") == r and res.get("blocking")
                for res in r_evid.get("results", []))]
    )
    warning_rules = _all_warnings(r_ans, r_evid, r_op)
    pending_items = _all_pending(r_ans, r_evid, r_op)
    opme_flags    = _opme_flags(r_ans, r_evid, r_op)

    # ── 5. Risco final ───────────────────────────────────────────────────────
    n_warns   = len(warning_rules)
    is_junta  = gate == "JUNTA"
    final_risk = _risk_from_score(
        final_score,
        blocking=bool(r_ans.get("blocking")),
        junta=is_junta,
        n_warnings=n_warns,
    )

    # ── 6. Textos explicativos ───────────────────────────────────────────────
    summary  = _build_summary(gate, r_ans, r_evid, r_op, final_score, final_risk)
    action   = _recommended_action(gate, r_ans, r_evid, r_op)
    defense  = _defense_ready(gate, r_ans, r_evid)

    # ── 7. Decision trace completo ───────────────────────────────────────────
    trace = [
        {"step": "ANS",       "overall": r_ans.get("overall"),  "gate": r_ans.get("gate"),
         "blocking": r_ans.get("blocking"), "failed": r_ans.get("failed_rules"),
         "score_contribution": round(s_ans, 1)},
        {"step": "EVIDENCIA",  "overall": r_evid.get("overall"), "gate": r_evid.get("gate"),
         "clinical_strength": r_evid.get("clinical_strength"),
         "evidence_score": r_evid.get("evidence_score"),
         "junta_rules": r_evid.get("junta_rules"),
         "score_contribution": round(s_evid, 1)},
        {"step": "OPERADORA",  "overall": r_op.get("overall"),   "gate": r_op.get("gate"),
         "operator": r_op.get("operator_name"),
         "risk_level": r_op.get("operator_risk_level"),
         "score_contribution": round(s_op, 1)},
        *[{"step": "GATE_LOGIC", "rule": msg} for msg in gate_trace],
        {"step": "FINAL",
         "gate": gate, "score": final_score,
         "risk": final_risk, "elapsed_ms": round((time.time() - t0) * 1000, 1)},
    ]

    layer_status = {
        "ANS":       r_ans.get("overall"),
        "EVIDENCIA": f"{r_evid.get('overall')} ({r_evid.get('clinical_strength')})",
        "OPERADORA": f"{r_op.get('overall')} (risk={r_op.get('operator_risk_level')})",
    }

    logger.info(
        "decision_engine v%s: gate=%s score=%d risk=%s "
        "ans=%s evid=%s op=%s elapsed=%.1fms",
        ENGINE_VERSION, gate, final_score, final_risk,
        r_ans.get("overall"), r_evid.get("clinical_strength"),
        r_op.get("operator_risk_level"),
        (time.time() - t0) * 1000,
    )

    return {
        "engine_version":        ENGINE_VERSION,
        "final_gate":            gate,
        "final_score":           final_score,
        "final_risk":            final_risk,
        "layer_status":          layer_status,
        "blocking_rules":        blocking_rules,
        "warning_rules":         warning_rules,
        "pending_items":         pending_items,
        "opme_flags":            opme_flags,
        "summary":               summary,
        "recommended_next_action": action,
        "defense_ready":         defense,
        "decision_trace":        trace,
        "layer_results": {
            "ans":       r_ans,
            "evidencia": r_evid,
            "operadora": r_op,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# WRAPPER DE COMPATIBILIDADE — mantém interface do motor v1.x
# O router decide.py e runner.py chamam run_decision(req: DecideRequest)
# ══════════════════════════════════════════════════════════════════════════════

def _req_to_ctx(req: Any) -> dict:
    """Converte DecideRequest para o dict de contexto dos validators."""
    from app.services.engine_v3 import normalize_input  # type: ignore

    n = normalize_input(req)
    return {
        # Identidade
        "episodio_id":        n.episodio_id,
        "convenio":           n.convenio,
        "procedimento":       n.procedimento,
        "procedimento_tuss":  n.procedimento_tuss,
        "cid_principal":      n.cid_principal,
        "cid_secundarios":    n.cid_secundarios,
        # Clínica
        "indicacao_clinica":  n.indicacao_clinica,
        "achados_resumo":     n.achados_resumo,
        "laudo_imagem":       n.achados_resumo,
        "tto_conservador":    n.tto_conservador,
        "semanas_conservador":n.semanas_conservador,
        "tem_deficit_motor":  n.tem_deficit_motor,
        "urgencia":           n.carater in ("urgencia", "urgência"),
        # Documentação
        "crm":                n.crm,
        "cbo":                n.cbo,
        "relatorio_clinico":  n.indicacao_clinica,
        # OPME
        "necessita_opme":     n.necessita_opme,
        "opme_itens_count":   n.opme_items_count,
        "justificativa_opme": (
            getattr(req, "justificativas_opme", None) or
            getattr(req, "justificativa_opme", None) or ""
        ),
        "cotacoes_opme":      getattr(req, "cotacoes_opme", []) or [],
    }


def _decision_to_response(d: dict, req: Any) -> Any:
    """Converte saída do decide() para DecideResponse (retrocompat)."""
    from app.models.decide import DecideResponse  # type: ignore
    import uuid
    from datetime import datetime, timezone

    gate   = d["final_gate"]
    score  = d["final_score"]
    risk   = d["final_risk"]

    # Mapear gate → classification e decision_status
    gate_map = {
        "GO":              ("GO",              "APROVADO"),
        "GO_COM_RESSALVAS":("GO_COM_RESSALVAS","AUTORIZADO_COM_RESSALVAS"),
        "NO_GO":           ("NO_GO",           "NEGADO"),
        "JUNTA":           ("GO_COM_RESSALVAS","AUTORIZADO_COM_RESSALVAS"),
    }
    classification, decision_status = gate_map.get(gate, ("GO_COM_RESSALVAS","AUTORIZADO_COM_RESSALVAS"))

    r_ans  = d["layer_results"]["ans"]
    r_evid = d["layer_results"]["evidencia"]
    r_op   = d["layer_results"]["operadora"]

    # Consolida pontos frágeis e pendências
    pontos_frageis = []
    for layer_r in [r_ans, r_evid, r_op]:
        for res in layer_r.get("results", []):
            if not res.get("passed") and res.get("user_message"):
                pontos_frageis.append(res["user_message"][:120])

    return DecideResponse(
        decision_run_id   = f"RUN_{uuid.uuid4().hex[:12].upper()}",
        episodio_id       = req.episodio_id or "",
        classification    = classification,
        decision_status   = decision_status,
        score             = score,
        justificativa     = d["summary"],
        pendencias        = [p[:120] for p in d["pending_items"][:5]],
        bloqueios         = [b[:120] for b in d["blocking_rules"][:5]],
        risco_glosa       = risk.lower(),
        pontos_frageis    = pontos_frageis[:8],
        proximos_passos   = [d["recommended_next_action"][:150]],
        timestamp         = datetime.now(timezone.utc).isoformat(),
        motor_version     = ENGINE_VERSION,
        score_clinico     = r_evid.get("evidence_score"),
        camada1           = r_ans.get("overall", ""),
        camada2_score     = r_evid.get("evidence_score"),
        camada3_risco     = r_op.get("operator_risk_level", "").lower(),
        gate_reason       = "; ".join(
            t.get("rule","") for t in d["decision_trace"]
            if t.get("step") == "GATE_LOGIC"
        )[:200],
        falhas            = [f[:120] for f in d["opme_flags"][:5]],
        # v2.0 trace estruturado completo
        v2_trace          = {
            "final_gate":              d["final_gate"],
            "final_score":             d["final_score"],
            "final_risk":              d["final_risk"],
            "layer_status":            d["layer_status"],
            "blocking_rules":          d["blocking_rules"],
            "warning_rules":           d["warning_rules"][:5],
            "pending_items":           d["pending_items"][:5],
            "recommended_next_action": d["recommended_next_action"][:200],
            "defense_ready":           d["defense_ready"],
            "summary":                 d["summary"][:300],
            "trace":                   d["decision_trace"][:8],
            "layer_results_summary": {
                "ans": {
                    "overall":       r_ans.get("overall"),
                    "failed_rules":  r_ans.get("failed_rules",[]),
                    "blocked_by":    r_ans.get("blocked_by"),
                },
                "evidencia": {
                    "overall":           r_evid.get("overall"),
                    "clinical_strength": r_evid.get("clinical_strength"),
                    "evidence_score":    r_evid.get("evidence_score"),
                    "junta_rules":       r_evid.get("junta_rules",[]),
                    "failed_rules":      r_evid.get("failed_rules",[])[:5],
                },
                "operadora": {
                    "overall":           r_op.get("overall"),
                    "operator_name":     r_op.get("operator_name"),
                    "risk_level":        r_op.get("operator_risk_level"),
                    "pending_items":     r_op.get("operator_pending_items",[])[:4],
                    "failed_rules":      r_op.get("failed_rules",[])[:5],
                },
            },
        },
    )


def run_decision(req: Any) -> Any:
    """
    Interface de compatibilidade v1.x → v2.0.
    Recebe DecideRequest, devolve DecideResponse.
    """
    ctx = _req_to_ctx(req)
    d   = decide(ctx)
    return _decision_to_response(d, req)
