"""
app/services/decision_engine_v1.py
NEUROAUTH — Decision Engine Clinical Layer v1.0

Camada de ENRIQUECIMENTO sobre o motor decisorio v2.0.
NAO altera nenhuma logica do motor existente.
Adiciona campos clinicos estruturados ao output, mantendo 100% de retrocompatibilidade.

Funcao principal:
    build_clinical_decision_output(decision: dict, ctx: dict) -> dict

Entrada:
    - decision: output completo do decide() (v2.0)
    - ctx: contexto original do caso clinico

Saida:
    - decision enriquecida com schema_version, glosa_probability,
      structured_justification (raciocinio_clinico, fundamento_regulatorio, texto_convenio)

Seguranca:
    - Se qualquer erro ocorrer, retorna decision original intacta
    - Nunca quebra a resposta do /decide
    - Camada 100% opcional e resiliente
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("neuroauth.decision_engine_v1")

CLINICAL_SCHEMA_VERSION = "decision-clinical-1.0"


def _safe_text(s: str, limit: int = 1200) -> str:
    """Trunca texto para evitar payload excessivo em integrações."""
    return s[:limit] if s else s

# ══════════════════════════════════════════════════════════════════════════════
# GLOSA PROBABILITY — heuristica inicial baseada em final_risk
# ══════════════════════════════════════════════════════════════════════════════

_RISK_TO_GLOSA_PROB = {
    "BAIXO":    0.10,
    "MODERADO": 0.35,
    "ALTO":     0.65,
    "CRITICO":  0.85,
}


def _compute_glosa_probability(decision: dict) -> float:
    """
    Calcula probabilidade de glosa baseada no risco final.
    Ajusta com fatores agravantes/atenuantes do decision_trace.
    """
    risk = decision.get("final_risk", "MODERADO")
    base_prob = _RISK_TO_GLOSA_PROB.get(risk, 0.35)

    # Ajuste fino baseado em score (quanto menor o score, maior a probabilidade)
    score = decision.get("final_score", 50)
    if score < 30:
        base_prob = min(base_prob + 0.10, 0.95)
    elif score > 80:
        base_prob = max(base_prob - 0.05, 0.05)

    # Agravante: pendencias documentais aumentam risco de glosa
    n_pending = len(decision.get("pending_items", []))
    if n_pending >= 3:
        base_prob = min(base_prob + 0.10, 0.95)
    elif n_pending >= 1:
        base_prob = min(base_prob + 0.05, 0.95)

    # Atenuante: defense_ready diminui risco
    if decision.get("defense_ready"):
        base_prob = max(base_prob - 0.05, 0.05)

    return round(base_prob, 2)


# ══════════════════════════════════════════════════════════════════════════════
# RACIOCINIO CLINICO — texto medico direto
# ══════════════════════════════════════════════════════════════════════════════

def _build_raciocinio_clinico(decision: dict, ctx: dict) -> str:
    """
    Constroi texto de raciocinio clinico usando dados do contexto.
    Formato: texto medico direto, sem floreio.
    """
    parts = []

    # Hipotese diagnostica
    cid = ctx.get("cid_principal", "")
    indicacao = ctx.get("indicacao_clinica", "")
    procedimento = ctx.get("procedimento", "")

    if cid and indicacao:
        parts.append(
            f"Paciente com hipotese diagnostica {cid}, "
            f"apresentando quadro clinico: {indicacao}."
        )
    elif cid:
        parts.append(f"Hipotese diagnostica: {cid}.")

    # Achados / gravidade
    achados = ctx.get("achados_resumo", "") or ctx.get("laudo_imagem", "")
    if achados:
        parts.append(f"Achados: {achados}.")

    # Impacto funcional
    if ctx.get("tem_deficit_motor"):
        parts.append("Paciente apresenta deficit motor, configurando urgencia clinica.")

    # Falha terapeutica previa
    tto = ctx.get("tto_conservador", "")
    semanas = ctx.get("semanas_conservador", 0)
    if tto:
        if semanas > 0:
            parts.append(
                f"Tratamento conservador previo realizado por {semanas} semanas "
                f"({tto}), sem resposta satisfatoria."
            )
        else:
            parts.append(
                f"Tratamento conservador previo: {tto}, sem resposta satisfatoria."
            )

    # Necessidade do procedimento
    if procedimento:
        parts.append(f"Indicacao de {procedimento} para resolucao do quadro.")

    # OPME se aplicavel
    if ctx.get("necessita_opme"):
        n_itens = ctx.get("opme_itens_count", 0)
        if n_itens > 0:
            parts.append(
                f"Procedimento requer {n_itens} item(ns) OPME com justificativa clinica documentada."
            )

    # Evidencia clinica (do motor v2.0)
    layer_results = decision.get("layer_results", {})
    evid = layer_results.get("evidencia", {})
    strength = evid.get("clinical_strength", "")
    if strength:
        parts.append(f"Forca da evidencia clinica avaliada como: {strength}.")

    if not parts:
        parts.append("Raciocinio clinico baseado na analise automatica do motor decisorio.")

    return " ".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# FUNDAMENTO REGULATORIO
# ══════════════════════════════════════════════════════════════════════════════

_GATE_REGULATORIO = {
    "GO":               "Documentacao atende aos requisitos regulatorios. Sem bloqueios identificados.",
    "GO_COM_RESSALVAS": "Documentacao parcialmente completa. Ressalvas identificadas que devem ser enderecadas.",
    "NO_GO":            "Documentacao nao atende aos requisitos minimos. Bloqueios regulatorios identificados.",
    "JUNTA":            "Caso requer avaliacao por junta medica especializada conforme normativa ANS.",
}


def _build_fundamento_regulatorio(decision: dict, ctx: dict) -> str:
    """
    Constroi justificativa regulatoria baseada no gate, score e regras acionadas.
    """
    parts = []

    gate = decision.get("final_gate", "GO_COM_RESSALVAS")
    score = decision.get("final_score", 0)

    # Base regulatoria
    parts.append(_GATE_REGULATORIO.get(gate, _GATE_REGULATORIO["GO_COM_RESSALVAS"]))

    # Score
    parts.append(f"Score de conformidade: {score}/100.")

    # Regras acionadas no trace
    trace = decision.get("decision_trace", [])
    gate_rules = [
        t.get("rule", "")
        for t in trace
        if t.get("step") == "GATE_LOGIC" and t.get("rule")
    ]
    if gate_rules:
        parts.append(f"Regras determinantes: {'; '.join(gate_rules[:3])}.")

    # Status das camadas
    layer_status = decision.get("layer_status", {})
    ans_status = layer_status.get("ANS", "")
    if ans_status:
        parts.append(f"Validacao ANS: {ans_status}.")

    # Completude documental
    blocking = decision.get("blocking_rules", [])
    pending = decision.get("pending_items", [])
    if blocking:
        parts.append(
            f"Bloqueios regulatorios: {', '.join(b[:60] for b in blocking[:3])}."
        )
    elif pending:
        parts.append(
            f"Pendencias documentais: {len(pending)} item(ns) a resolver."
        )
    else:
        parts.append("Documentacao completa, sem pendencias identificadas.")

    return " ".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# TEXTO PARA CONVENIO — pronto para colar no formulario SADT/internacao
# ══════════════════════════════════════════════════════════════════════════════

def _build_texto_convenio(decision: dict, ctx: dict) -> str:
    """
    Gera texto padrao pronto para colar no formulario de autorizacao.
    Formato adaptado dinamicamente com base no contexto.
    """
    cid = ctx.get("cid_principal", "quadro clinico documentado")
    procedimento = ctx.get("procedimento", "procedimento solicitado")
    indicacao = ctx.get("indicacao_clinica", "")
    tto = ctx.get("tto_conservador", "")
    achados = ctx.get("achados_resumo", "") or ctx.get("laudo_imagem", "")
    urgencia = ctx.get("urgencia", False)
    deficit = ctx.get("tem_deficit_motor", False)

    # Construir condicao
    condicao_parts = [f"diagnostico {cid}"]
    if indicacao:
        condicao_parts.append(indicacao)
    condicao = ", ".join(condicao_parts)

    # Construir justificativa clinica
    justificativa_parts = []
    if achados:
        justificativa_parts.append(f"achados clinicos/imagem: {achados}")
    if tto:
        justificativa_parts.append(f"falha de tratamento conservador ({tto})")
    if deficit:
        justificativa_parts.append("deficit motor documentado")
    if urgencia:
        justificativa_parts.append("carater de urgencia")

    if justificativa_parts:
        justificativa = ", ".join(justificativa_parts)
    else:
        justificativa = "indicacao clinica documentada"

    # Montar texto
    texto = (
        f"Paciente apresenta {condicao}, "
        f"com indicacao de {procedimento}. "
        f"Ha evidencia de {justificativa}. "
        f"Solicitacao compativel com criterios de cobertura, "
        f"sem contraindicacoes formais. "
        f"Recomenda-se autorizacao do procedimento."
    )

    # OPME addendum
    if ctx.get("necessita_opme"):
        n_itens = ctx.get("opme_itens_count", 0)
        if n_itens > 0:
            texto += (
                f" Material OPME necessario ({n_itens} item/itens) "
                f"com justificativa clinica individualizada em anexo."
            )

    return texto


# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE PUBLICA PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def build_clinical_decision_output(decision: dict, ctx: dict) -> dict:
    """
    Enriquece o output do motor v2.0 com campos clinicos estruturados.

    SEGURANCA: Se qualquer erro ocorrer, retorna decision original intacta.
    RETROCOMPATIBILIDADE: Nenhum campo existente e removido ou alterado.

    Campos adicionados:
        - schema_version: "decision-clinical-1.0"
        - glosa_probability: float (0.0-1.0)
        - structured_justification: {
            raciocinio_clinico: str,
            fundamento_regulatorio: str,
            texto_convenio: str
          }
    """
    try:
        glosa_prob = _compute_glosa_probability(decision)
        raciocinio = _safe_text(_build_raciocinio_clinico(decision, ctx))
        regulatorio = _safe_text(_build_fundamento_regulatorio(decision, ctx))
        texto_conv = _safe_text(_build_texto_convenio(decision, ctx))

        # Enriquecer SEM sobrescrever campos existentes
        decision["schema_version"] = CLINICAL_SCHEMA_VERSION
        decision["glosa_probability"] = glosa_prob
        decision["trace_id"] = ctx.get("trace_id")
        decision["structured_justification"] = {
            "raciocinio_clinico": raciocinio,
            "fundamento_regulatorio": regulatorio,
            "texto_convenio": texto_conv,
        }

        logger.info(
            "decision_engine_v1: schema=%s glosa_prob=%.2f gate=%s score=%d",
            CLINICAL_SCHEMA_VERSION,
            glosa_prob,
            decision.get("final_gate", "?"),
            decision.get("final_score", 0),
        )

        return decision

    except Exception as exc:
        logger.warning(
            "decision_engine_v1: enriquecimento falhou (%s: %s) — "
            "retornando decision original intacta",
            type(exc).__name__, str(exc)[:200],
        )
        return decision
