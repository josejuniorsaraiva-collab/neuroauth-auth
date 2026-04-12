"""
app/services/validator_evidencia.py
NEUROAUTH — Validator Evidência v1.0.0

Camada 2 do motor de regras: pertinência clínica e robustez da indicação.
Não avalia regulatório (ANS) nem regras de operadora.
Pergunta: "a indicação se sustenta clinicamente?"

Contrato de saída por regra (igual ao validator_ans.py):
{
    "rule_id":         str,
    "rule_name":       str,
    "layer":           "EVIDENCIA",
    "passed":          bool,
    "severity":        "BAIXA" | "MODERADA" | "ALTA" | "CRITICA",
    "blocking":        bool,
    "failure_action":  "PASS" | "WARN" | "SCORE_UP" | "SCORE_DOWN"
                       | "SEND_TO_JUNTA" | "REQUEST_DOC" | "NO_GO",
    "gate_suggestion": "NO_GO" | "GO_COM_RESSALVAS" | "JUNTA" | "GO" | None,
    "score_delta":     int,       # positivo = reforça, negativo = enfraquece
    "user_message":    str,
    "technical_rationale": str,
    "rule_source":     str,
}

Saída consolidada:
{
    "layer":            "EVIDENCIA",
    "overall":          "PASS" | "FAIL" | "WARN" | "JUNTA",
    "blocking":         bool,
    "gate":             "NO_GO" | "GO_COM_RESSALVAS" | "JUNTA" | "GO",
    "evidence_score":   float,  # 0.0–1.0
    "clinical_strength":"FORTE" | "MODERADA" | "FRACA" | "CINZENTA",
    "recommended_path": "GO" | "GO_COM_RESSALVAS" | "JUNTA" | "NO_GO",
    "score_delta_total":int,
    "results":          list[dict],
    "failed_rules":     list[str],
    "junta_rules":      list[str],
    "boost_rules":      list[str],
}
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger("neuroauth.validator_evidencia")

# ══════════════════════════════════════════════════════════════════════════════
# RULE LIBRARY — EVIDENCIA
# ══════════════════════════════════════════════════════════════════════════════

EVID_RULES: list[dict[str, Any]] = [

    {
        "rule_id":   "EVID_STRUCT_001",
        "rule_name": "Coerência estrutural clínica mínima",
        "priority":  1,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "source_reference": "Critérios mínimos de documentação clínica — NEUROAUTH CORE",
    },
    {
        "rule_id":   "EVID_CORR_001",
        "rule_name": "Correlação clínico-radiológica",
        "priority":  2,
        "severity":  "CRITICA",
        "blocking":  False,
        "rule_type": "BINARIA",
        "failure_action": "SEND_TO_JUNTA",
        "gate_if_fail":   "JUNTA",
        "score_delta":    -25,
        "source_reference": "Consenso SBN/SBOT — pertinência clínica para cirurgia de coluna",
    },
    {
        "rule_id":   "EVID_DEFICIT_001",
        "rule_name": "Déficit neurológico objetivo — reforço de indicação",
        "priority":  3,
        "severity":  "MODERADA",
        "blocking":  False,
        "rule_type": "SCORE",
        "failure_action": "SCORE_DOWN",
        "success_action": "SCORE_UP",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -10,   # sem déficit reduz; com déficit aumenta
        "score_delta_up": +15,
        "source_reference": "Diretrizes de neurocirurgia — indicação baseada em déficit objetivo",
    },
    {
        "rule_id":   "EVID_MIELOPATIA_001",
        "rule_name": "Mielopatia ou compressão medular significativa",
        "priority":  4,
        "severity":  "CRITICA",
        "blocking":  False,
        "rule_type": "SCORE",
        "failure_action": "PASS",
        "success_action": "SCORE_UP",
        "gate_if_fail":   None,
        "score_delta_up": +20,
        "source_reference": "Protocolo de mielopatia cervical — SBN 2023",
    },
    {
        "rule_id":   "EVID_CONSERV_001",
        "rule_name": "Falha de tratamento conservador documentada",
        "priority":  5,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "SCORE",
        "failure_action": "SCORE_DOWN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "score_delta_up": +15,
        "source_reference": "DUT coluna + consensos internacionais — falha conservadora como pré-requisito",
    },
    {
        "rule_id":   "EVID_IMG_001",
        "rule_name": "Imagem compatível com nível e lado dos sintomas",
        "priority":  6,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "BINARIA",
        "failure_action": "SEND_TO_JUNTA",
        "gate_if_fail":   "JUNTA",
        "score_delta":    -20,
        "source_reference": "Consenso coluna — exigência de correlação topográfica imagem×clínica",
    },
    {
        "rule_id":   "EVID_RED_FLAG_001",
        "rule_name": "Red flags ou progressão neurológica",
        "priority":  7,
        "severity":  "CRITICA",
        "blocking":  False,
        "rule_type": "SCORE",
        "failure_action": "PASS",
        "success_action": "SCORE_UP",
        "gate_if_fail":   None,
        "score_delta_up": +20,
        "source_reference": "Red flags neurocirúrgicos — guias NICE + SBN",
    },
    {
        "rule_id":   "EVID_GEN_001",
        "rule_name": "Indicação genérica sem ancoragem clínica objetiva",
        "priority":  8,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "SCORE",
        "failure_action": "SCORE_DOWN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "source_reference": "Critérios de pertinência — auditoria médica",
    },
    {
        "rule_id":   "EVID_EXPER_001",
        "rule_name": "Evidência fraca ou zona cinzenta — revisão técnica",
        "priority":  9,
        "severity":  "MODERADA",
        "blocking":  False,
        "rule_type": "RESSALVA",
        "failure_action": "SEND_TO_JUNTA",
        "gate_if_fail":   "JUNTA",
        "score_delta":    -15,
        "source_reference": "Classificação de evidência — Oxford CEBM / GRADE",
    },
    {
        "rule_id":   "EVID_ACHADOS_001",
        "rule_name": "Achados de imagem objetivos documentados",
        "priority":  10,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "source_reference": "Requisitos documentais — operadoras + ANS",
    },
]

# ══════════════════════════════════════════════════════════════════════════════
# VOCABULÁRIOS DE DETECÇÃO
# ══════════════════════════════════════════════════════════════════════════════

# Déficit motor objetivo
DEFICIT_MOTOR_POS = {
    "déficit motor", "deficit motor", "paresia", "plegia", "paraparesia",
    "paraplegia", "tetraparesia", "tetraplegia", "hemiparesia",
    "força grau", "força muscular grau", "hiperreflexia", "babinski",
    "clônus", "clonus", "sinal de lhermitte", "lhermitte",
    "déficit neurológico", "deficit neurologico", "fraqueza muscular objetiva",
}
DEFICIT_MOTOR_NEG = {
    "sem déficit", "sem deficit", "força normal", "força preservada",
    "reflexos normais", "sem alteração neurológica", "exame normal",
    "sem sinais focais",
}

# Mielopatia
MIELOPATIA_KEYWORDS = {
    "mielopatia", "myelopathy", "compressão medular", "compressao medular",
    "sinal de lhermitte", "disfunção medular", "disfuncao medular",
    "hiperreflexia", "babinski", "clônus", "clonus",
    "marcha atáxica", "marcha ataxica", "incontinência", "incontinencia",
    "sinais piramidais", "sinal piramidal",
}

# Red flags neurocirúrgicos
RED_FLAGS = {
    "síndrome da cauda equina", "sindrome da cauda equina",
    "cauda equina", "incontinência urinária aguda", "incontinencia urinaria",
    "retenção urinária", "retencao urinaria",
    "déficit motor progressivo", "deficit motor progressivo",
    "mielopatia progressiva", "progressão rápida", "progressao rapida",
    "compressão aguda", "compressao aguda",
    "trauma", "fratura", "luxação", "luxacao",
    "tumor", "neoplasia", "metástase", "metastase",
    "infecção", "infeccao", "abscesso", "empiema",
    "emergência neurológica", "emergencia neurologica",
}

# Indicação genérica (sem ancoragem objetiva)
INDICACAO_GENERICA_TOKENS = {
    "dor", "lombalgia", "cervicalgia", "cefaleia",
    "dormência", "dormencia", "formigamento",
    "sem melhora", "piora", "tratamento clínico", "tratamento conservador",
    "cirurgia indicada", "indicado cirurgia",
}

# Evidência fraca / zona cinzenta para cirurgia de coluna
ZONA_CINZENTA = {
    "fibromialgia", "dor crônica", "dor cronica",
    "síndrome miofascial", "sindrome miofascial",
    "instabilidade leve", "degeneração leve", "degeneracao leve",
    "espondilose sem compressão", "artrose facetária sem compressão",
    "sem compressão radicular", "sem compressao radicular",
    "sem correlação clínica", "sem correlacao clinica",
}

# Correlação imagem×sintoma — keywords de compatibilidade
CORR_IMG_POS = {
    "correlação clínico-radiológica", "correlacao clinico-radiologica",
    "compatível com", "compativel com", "nível correspondente",
    "comprometimento de", "correlaciona com", "confirma",
    "concordante", "correspondendo ao nível", "nível da queixa",
}
CORR_IMG_NEG = {
    "sem correlação", "sem correlacao", "achado incidental",
    "imagem isolada", "imagem sem correlação clínica",
    "não correlaciona", "nao correlaciona",
    "alteração inespecífica", "alteracao inespecifica",
}

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _texto(ctx: dict, *campos) -> str:
    """Concatena campos de texto do contexto em minúsculas."""
    return " ".join(
        str(ctx.get(c, "") or "").lower()
        for c in campos
    )


def _contem(texto: str, keywords: set[str]) -> bool:
    return any(kw in texto for kw in keywords)


def _score_frac(positivos: int, total: int) -> float:
    return positivos / total if total else 0.0


def _resultado_evid(rule: dict, passed: bool,
                    msg: str = "", rationale: str = "",
                    delta_override: int | None = None) -> dict:
    delta = 0
    action = "PASS"
    gate = rule.get("gate_if_pass", "GO")

    if not passed:
        delta = delta_override if delta_override is not None else rule.get("score_delta", 0)
        action = rule["failure_action"]
        gate   = rule.get("gate_if_fail", None)
    else:
        if rule.get("success_action") == "SCORE_UP":
            delta = rule.get("score_delta_up", 0)

    return {
        "rule_id":          rule["rule_id"],
        "rule_name":        rule["rule_name"],
        "layer":            "EVIDENCIA",
        "passed":           passed,
        "severity":         rule["severity"],
        "blocking":         False,  # Evidência nunca bloqueia sozinha
        "failure_action":   action,
        "gate_suggestion":  gate,
        "score_delta":      delta,
        "user_message":     msg if not passed else "",
        "technical_rationale": rationale,
        "rule_source":      rule.get("source_reference", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
# AVALIADORES POR REGRA
# ══════════════════════════════════════════════════════════════════════════════

def _eval_struct_001(ctx: dict) -> dict:
    """EVID_STRUCT_001 — Coerência estrutural clínica mínima."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_STRUCT_001")
    missing = []
    if not ctx.get("indicacao_clinica") or len(ctx.get("indicacao_clinica","")) < 30:
        missing.append("indicação clínica objetiva (mínimo 30 chars)")
    if not ctx.get("achados_resumo") and not ctx.get("laudo_imagem"):
        missing.append("achados de imagem ou laudo")
    if not ctx.get("diagnostico") and not ctx.get("cid_principal"):
        missing.append("diagnóstico ou CID")
    if missing:
        return _resultado_evid(rule, False,
            f"Documentação clínica insuficiente: {'; '.join(missing)}.",
            "Caso sem dados clínicos mínimos não pode ser avaliado para pertinência.")
    return _resultado_evid(rule, True)


def _eval_corr_001(ctx: dict) -> dict:
    """EVID_CORR_001 — Correlação clínico-radiológica."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_CORR_001")
    texto = _texto(ctx, "indicacao_clinica", "achados_resumo", "laudo_imagem", "queixa_principal")

    tem_imagem  = bool(ctx.get("achados_resumo") or ctx.get("laudo_imagem"))
    tem_clinica = bool(ctx.get("indicacao_clinica") and len(ctx.get("indicacao_clinica","")) > 20)

    if not tem_imagem:
        return _resultado_evid(rule, False,
            "Correlação clínico-radiológica impossível — sem achados de imagem documentados.",
            "Achado de imagem é pré-requisito para validar indicação cirúrgica.")

    neg = _contem(texto, CORR_IMG_NEG)
    pos = _contem(texto, CORR_IMG_POS)

    if neg and not pos:
        return _resultado_evid(rule, False,
            "Achado de imagem sem correlação clínica — fragilidade crítica da indicação. "
            "Encaminhado para junta médica.",
            "Imagem isolada sem correlação topográfica com sintomas fragiliza indicação cirúrgica.")

    if tem_imagem and tem_clinica:
        return _resultado_evid(rule, True)

    # Zona cinzenta — tem imagem mas correlação não explícita
    return _resultado_evid(rule, False,
        "Correlação clínico-radiológica não explicitada no texto. "
        "Incluir nível, lado e achado correlacionado com sintoma.",
        "Auditoria exige correlação topográfica explícita entre imagem e queixa.")


def _eval_deficit_001(ctx: dict) -> dict:
    """EVID_DEFICIT_001 — Déficit neurológico objetivo."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_DEFICIT_001")
    texto = _texto(ctx, "indicacao_clinica", "queixa_principal", "achados_resumo", "exame_fisico")

    tem_deficit  = _contem(texto, DEFICIT_MOTOR_POS) or ctx.get("tem_deficit_motor", False)
    nega_deficit = _contem(texto, DEFICIT_MOTOR_NEG)

    if tem_deficit and not nega_deficit:
        return _resultado_evid(rule, True,
            delta_override=rule.get("score_delta_up", 15))
    # Sem déficit objetivo — score desce, mas não bloqueia
    return _resultado_evid(rule, False,
        "Déficit neurológico objetivo não documentado. "
        "Indicação se apoia apenas em sintomas subjetivos.",
        "Ausência de déficit reduz robustez da indicação — "
        "exige conservador completo e correlação radiológica forte.")


def _eval_mielopatia_001(ctx: dict) -> dict:
    """EVID_MIELOPATIA_001 — Mielopatia ou compressão medular."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_MIELOPATIA_001")
    texto = _texto(ctx, "indicacao_clinica", "achados_resumo", "laudo_imagem", "exame_fisico")
    if _contem(texto, MIELOPATIA_KEYWORDS):
        return _resultado_evid(rule, True,
            delta_override=rule.get("score_delta_up", 20))
    # Sem mielopatia — neutro, não penaliza
    return {
        **_resultado_evid(rule, True),
        "score_delta": 0,
        "technical_rationale": "Mielopatia não identificada — sem bônus de score.",
    }


def _eval_conserv_001(ctx: dict) -> dict:
    """EVID_CONSERV_001 — Falha de tratamento conservador documentada."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_CONSERV_001")
    semanas   = ctx.get("semanas_conservador", 0)
    deficit   = ctx.get("tem_deficit_motor", False)
    urgencia  = ctx.get("urgencia", False)
    tto_texto = _texto(ctx, "tto_conservador", "indicacao_clinica")

    # Urgência com déficit → conservador não é pré-requisito
    if urgencia and deficit:
        return _resultado_evid(rule, True, delta_override=0)

    # Documentado e suficiente (≥4 semanas de texto ou campo)
    if semanas >= 4:
        return _resultado_evid(rule, True,
            delta_override=rule.get("score_delta_up", 15))

    # Menção a conservador mas sem quantificação
    mencao = any(kw in tto_texto for kw in
                 ["fisioterapia", "analgésico", "analgesia", "repouso",
                  "anti-inflamatório", "anti-inflamatorio", "bloqueio",
                  "infiltração", "infiltracao", "conservador"])
    if mencao and semanas == 0:
        return _resultado_evid(rule, False,
            "Tratamento conservador mencionado mas não quantificado (semanas). "
            "Documentar duração e resposta para fortalecer indicação.",
            "Falha conservadora documentada é pré-requisito clínico — tempo deve estar explícito.")

    # Sem conservador e sem urgência/déficit
    return _resultado_evid(rule, False,
        "Ausência de tratamento conservador documentado — "
        "negativa provável sem déficit motor ou urgência.",
        "Consensos exigem falha conservadora documentada antes de indicação cirúrgica eletiva.")


def _eval_img_001(ctx: dict) -> dict:
    """EVID_IMG_001 — Imagem compatível com nível e lado dos sintomas."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_IMG_001")
    texto = _texto(ctx, "achados_resumo", "laudo_imagem", "indicacao_clinica")

    if not (ctx.get("achados_resumo") or ctx.get("laudo_imagem")):
        return _resultado_evid(rule, False,
            "Sem achados objetivos — fragilidade crítica. "
            "RM ou TC compatível com a indicação é obrigatório.",
            "Sem imagem, impossível confirmar compatibilidade topográfica.")

    # Verificar incompatibilidade explícita
    if _contem(texto, CORR_IMG_NEG):
        return _resultado_evid(rule, False,
            "Imagem sem correlação topográfica com sintomas. "
            "Nível e/ou lado do achado não corresponde à queixa — encaminhado para junta.",
            "Discordância imagem×clínica é critério de envio a junta médica.")

    return _resultado_evid(rule, True)


def _eval_red_flag_001(ctx: dict) -> dict:
    """EVID_RED_FLAG_001 — Red flags ou progressão neurológica."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_RED_FLAG_001")
    texto = _texto(ctx, "indicacao_clinica", "queixa_principal", "achados_resumo", "red_flags")
    if _contem(texto, RED_FLAGS) or ctx.get("red_flags"):
        return _resultado_evid(rule, True,
            delta_override=rule.get("score_delta_up", 20))
    return {**_resultado_evid(rule, True), "score_delta": 0}


def _eval_gen_001(ctx: dict) -> dict:
    """EVID_GEN_001 — Indicação genérica sem ancoragem clínica."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_GEN_001")
    indicacao = (ctx.get("indicacao_clinica") or "").lower()

    if len(indicacao) < 30:
        return _resultado_evid(rule, False,
            "Indicação clínica insuficiente — auditoria rejeita sem especificidade objetiva.",
            "Indicação deve conter diagnóstico, tempo de evolução, achados objetivos e falha terapêutica.")

    tokens = set(indicacao.split())
    genericos = tokens & INDICACAO_GENERICA_TOKENS
    # Mais de 3 tokens genéricos sem ancoragem específica
    tem_ancoragem = any(kw in indicacao for kw in
                        ["hérnia", "hernia", "estenose", "compressão", "compressao",
                         "radiculopatia", "mielopatia", "fratura", "tumor",
                         "aneurisma", "malformação", "malformacao"])
    if len(genericos) >= 3 and not tem_ancoragem:
        return _resultado_evid(rule, False,
            "Indicação genérica sem ancoragem clínica objetiva. "
            "Incluir: diagnóstico principal, tempo de sintomas, achados de imagem, tratamentos tentados.",
            "Indicação vaga fragiliza pertinência e facilita negativa por auditoria.")

    return _resultado_evid(rule, True)


def _eval_exper_001(ctx: dict) -> dict:
    """EVID_EXPER_001 — Evidência fraca ou zona cinzenta."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_EXPER_001")
    texto = _texto(ctx, "indicacao_clinica", "achados_resumo", "diagnostico")
    if _contem(texto, ZONA_CINZENTA):
        return _resultado_evid(rule, False,
            "Indicação em zona cinzenta de evidência — revisão técnica recomendada. "
            "Diagnóstico/achado identificado com evidência limitada para indicação cirúrgica.",
            "Casos sem evidência robusta devem ser avaliados por junta médica especializada.")
    return _resultado_evid(rule, True)


def _eval_achados_001(ctx: dict) -> dict:
    """EVID_ACHADOS_001 — Achados de imagem objetivos documentados."""
    rule = next(r for r in EVID_RULES if r["rule_id"] == "EVID_ACHADOS_001")
    achados = ctx.get("achados_resumo", "") or ctx.get("laudo_imagem", "")
    if not achados or len(str(achados)) < 20:
        return _resultado_evid(rule, False,
            "Sem achados objetivos de imagem documentados — fragilidade crítica. "
            "Incluir resultado de RM/TC com achado, nível e data.",
            "Operadoras rejeitam solicitação sem correlação com exame de imagem objetivo.")
    return _resultado_evid(rule, True)


# Mapa rule_id → avaliador
_EVALUATORS_EVID = {
    "EVID_STRUCT_001":    _eval_struct_001,
    "EVID_CORR_001":      _eval_corr_001,
    "EVID_DEFICIT_001":   _eval_deficit_001,
    "EVID_MIELOPATIA_001":_eval_mielopatia_001,
    "EVID_CONSERV_001":   _eval_conserv_001,
    "EVID_IMG_001":       _eval_img_001,
    "EVID_RED_FLAG_001":  _eval_red_flag_001,
    "EVID_GEN_001":       _eval_gen_001,
    "EVID_EXPER_001":     _eval_exper_001,
    "EVID_ACHADOS_001":   _eval_achados_001,
}

# ══════════════════════════════════════════════════════════════════════════════
# CONSOLIDAÇÃO DE SCORE E CLINICAL STRENGTH
# ══════════════════════════════════════════════════════════════════════════════

# Score base 100 pts — distribuição por domínio
_BASE_SCORE   = 100
_SCORE_MAX    = 100
_SCORE_BOOST  = 30  # máximo de pontos extras por boosts

def _clinical_strength(score: float) -> str:
    if score >= 0.80: return "FORTE"
    if score >= 0.60: return "MODERADA"
    if score >= 0.40: return "FRACA"
    return "CINZENTA"

def _recommended_path(strength: str, junta_rules: list, blocking: bool) -> str:
    if blocking:                      return "NO_GO"
    if junta_rules:                   return "JUNTA"
    if strength == "FORTE":           return "GO"
    if strength == "MODERADA":        return "GO_COM_RESSALVAS"
    if strength == "FRACA":           return "GO_COM_RESSALVAS"
    return "JUNTA"  # CINZENTA

def _gate_from_results(results: list[dict]) -> str:
    gate = "GO"
    for r in results:
        sug = r.get("gate_suggestion", "")
        if sug == "NO_GO":                      return "NO_GO"
        if sug == "JUNTA" and gate != "NO_GO":  gate = "JUNTA"
        if sug == "GO_COM_RESSALVAS" and gate == "GO": gate = "GO_COM_RESSALVAS"
    return gate

# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════

def run_evidencia_validation(ctx: dict) -> dict:
    """
    Executa todas as regras de Evidência em ordem de prioridade.

    ctx: dict com campos do caso clínico:
        cid_principal, diagnostico, procedimento,
        indicacao_clinica, queixa_principal, achados_resumo, laudo_imagem,
        exame_fisico, tto_conservador, semanas_conservador,
        tem_deficit_motor, urgencia, red_flags,
        mielopatia (bool), correlacao_clinica_radiologica (bool)

    Retorna dict consolidado com evidence_score, clinical_strength,
    recommended_path e results por regra.
    """
    results: list[dict] = []
    total_delta   = 0
    junta_rules:  list[str] = []
    boost_rules:  list[str] = []
    failed_rules: list[str] = []
    gate = "GO"

    sorted_rules = sorted(EVID_RULES, key=lambda r: r["priority"])

    for rule in sorted_rules:
        rule_id   = rule["rule_id"]
        evaluator = _EVALUATORS_EVID.get(rule_id)
        if not evaluator:
            logger.warning("validator_evidencia: sem evaluator para %s", rule_id)
            continue

        result = evaluator(ctx)
        results.append(result)

        total_delta += result.get("score_delta", 0)

        if not result["passed"]:
            failed_rules.append(rule_id)
            if result["failure_action"] == "SEND_TO_JUNTA":
                junta_rules.append(rule_id)
        elif result.get("score_delta", 0) > 0:
            boost_rules.append(rule_id)

    # Calcula score: parte de 0.6 base + deltas normalizados
    raw = _BASE_SCORE + total_delta                    # pode ser negativo
    raw = max(0, min(_BASE_SCORE + _SCORE_BOOST, raw)) # clamp 0–130
    evidence_score = round(raw / (_BASE_SCORE + _SCORE_BOOST), 3)

    gate          = _gate_from_results(results)
    strength      = _clinical_strength(evidence_score)
    path          = _recommended_path(strength, junta_rules, blocking=False)
    overall       = ("JUNTA" if junta_rules else
                     "FAIL"  if len(failed_rules) > len(results) // 2 else
                     "WARN"  if failed_rules else
                     "PASS")

    logger.info(
        "validator_evidencia: score=%.3f strength=%s path=%s "
        "gate=%s failed=%s junta=%s boost=%s",
        evidence_score, strength, path, gate,
        failed_rules, junta_rules, boost_rules,
    )

    return {
        "layer":            "EVIDENCIA",
        "overall":          overall,
        "blocking":         False,
        "gate":             gate,
        "evidence_score":   evidence_score,
        "clinical_strength":strength,
        "recommended_path": path,
        "score_delta_total":total_delta,
        "results":          results,
        "failed_rules":     failed_rules,
        "junta_rules":      junta_rules,
        "boost_rules":      boost_rules,
    }
