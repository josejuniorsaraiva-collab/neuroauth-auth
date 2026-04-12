"""
app/services/validator_operadora.py
NEUROAUTH — Validator Operadora v1.0.0

Camada 3 do motor de regras: regras operacionais e documentais por convênio.
Não avalia regulatório ANS nem pertinência clínica.
Pergunta: "com esta operadora, esta documentação passa liso, com ressalva ou trava?"

Contrato de saída por regra (padrão dos 3 validators):
{
    "rule_id":          str,
    "rule_name":        str,
    "layer":            "OPERADORA",
    "passed":           bool,
    "severity":         "BAIXA" | "MODERADA" | "ALTA" | "CRITICA",
    "blocking":         bool,
    "failure_action":   "PASS" | "WARN" | "REQUEST_DOC" | "SCORE_DOWN"
                        | "SCORE_UP" | "FLAG_OPME" | "FLAG_GLOSA",
    "gate_suggestion":  "NO_GO" | "GO_COM_RESSALVAS" | "GO" | None,
    "score_delta":      int,
    "user_message":     str,
    "technical_rationale": str,
    "rule_source":      str,
}

Saída consolidada:
{
    "layer":                   "OPERADORA",
    "overall":                 "PASS" | "FAIL" | "WARN",
    "blocking":                bool,
    "gate":                    "NO_GO" | "GO_COM_RESSALVAS" | "GO",
    "operator_risk_level":     "BAIXO" | "MODERADO" | "ALTO" | "CRITICO",
    "operator_pending_items":  list[str],
    "operator_recommended_path": "GO" | "GO_COM_RESSALVAS" | "NO_GO",
    "score_delta_total":       int,
    "results":                 list[dict],
    "failed_rules":            list[str],
}
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("neuroauth.validator_operadora")

# ══════════════════════════════════════════════════════════════════════════════
# OPERATOR PROFILES — parametrizáveis
# Substituível por: leitura de Google Sheets (tab OPERATOR_PROFILES)
# ou injeção via JSON externo
# ══════════════════════════════════════════════════════════════════════════════

OPERATOR_PROFILES: dict[str, dict[str, Any]] = {
    "unimed": {
        "name":                  "UNIMED",
        "conservador_min_semanas": 6,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 200,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": True,
        "aceita_rx_como_base":   False,
        "rigidez_coluna_eletiva":"ALTA",
        "prazo_resposta_dias":   10,
        "regras_extras": {
            "exige_laudo_fisio":        True,
            "exige_evolucao_clinica":   True,
            "cap_opme_itens":           15,
        },
    },
    "bradesco": {
        "name":                  "BRADESCO SAÚDE",
        "conservador_min_semanas": 8,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 250,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": True,
        "aceita_rx_como_base":   False,
        "rigidez_coluna_eletiva":"ALTA",
        "prazo_resposta_dias":   10,
        "regras_extras": {
            "exige_lateralidade_explicita": True,
            "exige_escala_funcional":       False,
            "cap_opme_itens":               12,
        },
    },
    "sulamerica": {
        "name":                  "SULAMÉRICA",
        "conservador_min_semanas": 8,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 300,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": True,
        "aceita_rx_como_base":   False,
        "rigidez_coluna_eletiva":"CRITICA",
        "prazo_resposta_dias":   10,
        "regras_extras": {
            "exige_oswestry_ou_vas":    True,
            "bloqueia_sem_deficit_motor":True,
            "cap_opme_itens":           10,
        },
    },
    "cassi": {
        "name":                  "CASSI",
        "conservador_min_semanas": 4,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 150,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": False,
        "aceita_rx_como_base":   True,
        "rigidez_coluna_eletiva":"MODERADA",
        "prazo_resposta_dias":   7,
        "regras_extras": {
            "exige_laudo_fisio":     False,
            "cap_opme_itens":        20,
        },
    },
    "amil": {
        "name":                  "AMIL",
        "conservador_min_semanas": 6,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 200,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": True,
        "aceita_rx_como_base":   False,
        "rigidez_coluna_eletiva":"ALTA",
        "prazo_resposta_dias":   10,
        "regras_extras":         {"cap_opme_itens": 15},
    },
    "hapvida": {
        "name":                  "HAPVIDA",
        "conservador_min_semanas": 6,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 150,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": True,
        "aceita_rx_como_base":   True,
        "rigidez_coluna_eletiva":"MODERADA",
        "prazo_resposta_dias":   10,
        "regras_extras":         {"cap_opme_itens": 20},
    },
    # fallback genérico — aplicado quando operadora não mapeada
    "_default": {
        "name":                  "OPERADORA GENÉRICA",
        "conservador_min_semanas": 6,
        "opme_cotacoes_min":     3,
        "exige_rm_pre":          True,
        "exige_relatorio_min_chars": 150,
        "exige_crm_cbo":         True,
        "exige_similar_nacional": False,
        "aceita_rx_como_base":   True,
        "rigidez_coluna_eletiva":"MODERADA",
        "prazo_resposta_dias":   10,
        "regras_extras":         {},
    },
}


def get_operator_profile(convenio: str) -> dict[str, Any]:
    """Retorna perfil da operadora por nome (fuzzy match em minúsculas)."""
    cl = (convenio or "").lower()
    for key, profile in OPERATOR_PROFILES.items():
        if key == "_default":
            continue
        if key in cl or cl in key:
            return profile
    return {**OPERATOR_PROFILES["_default"], "name": convenio.upper() or "DESCONHECIDA"}


# ══════════════════════════════════════════════════════════════════════════════
# RULE LIBRARY — OPERADORA
# ══════════════════════════════════════════════════════════════════════════════

OP_RULES: list[dict[str, Any]] = [
    {
        "rule_id":   "OP_STRUCT_001",
        "rule_name": "Operadora identificada e perfil carregado",
        "priority":  1,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "BINARIA",
        "failure_action": "WARN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -10,
        "source_reference": "Pré-condição para aplicar regras de auditoria específicas",
    },
    {
        "rule_id":   "OP_CONSERV_001",
        "rule_name": "Conservador mínimo por operadora",
        "priority":  2,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "BINARIA",
        "failure_action": "SCORE_DOWN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -25,
        "source_reference": "Manual de auditoria por operadora — tempo conservador",
    },
    {
        "rule_id":   "OP_RELATORIO_001",
        "rule_name": "Relatório clínico com densidade suficiente",
        "priority":  3,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "SCORE",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "source_reference": "Critérios de auditoria — densidade documental mínima",
    },
    {
        "rule_id":   "OP_EXAMES_001",
        "rule_name": "Exames obrigatórios anexados",
        "priority":  4,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "source_reference": "Requisitos documentais por operadora",
    },
    {
        "rule_id":   "OP_GUIA_001",
        "rule_name": "Campos críticos da guia completos",
        "priority":  5,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -15,
        "source_reference": "TISS ANS — campos obrigatórios guia SADT/internação",
    },
    {
        "rule_id":   "OP_OPME_001",
        "rule_name": "OPME com justificativa clínica individualizada",
        "priority":  6,
        "severity":  "ALTA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "FLAG_OPME",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -20,
        "source_reference": "Manual OPME — operadoras + RN ANS 440",
    },
    {
        "rule_id":   "OP_OPME_002",
        "rule_name": "3 cotações obrigatórias para OPME",
        "priority":  7,
        "severity":  "MODERADA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -15,
        "source_reference": "Manual OPME por operadora — tripé documental",
    },
    {
        "rule_id":   "OP_OPME_003",
        "rule_name": "Similar nacional — alerta de substituição",
        "priority":  8,
        "severity":  "BAIXA",
        "blocking":  False,
        "rule_type": "ALERTA",
        "failure_action": "FLAG_OPME",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -5,
        "source_reference": "Política OPME — preferência por similar nacional",
    },
    {
        "rule_id":   "OP_AUDITORIA_001",
        "rule_name": "Regras específicas de auditoria da operadora",
        "priority":  9,
        "severity":  "MODERADA",
        "blocking":  False,
        "rule_type": "CHECKLIST",
        "failure_action": "WARN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -10,
        "source_reference": "Manual de auditoria específico por operadora",
    },
    {
        "rule_id":   "OP_PRAZO_001",
        "rule_name": "Caráter urgência/eletivo e prazo de resposta",
        "priority":  10,
        "severity":  "MODERADA",
        "blocking":  False,
        "rule_type": "BINARIA",
        "failure_action": "WARN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_delta":    -10,
        "source_reference": "RN ANS 465 — prazos de autorização",
    },
]

# Procedimentos que tipicamente requerem OPME
PROC_REQUER_OPME = {
    "artrodese", "fixação", "fixacao", "prótese", "protese",
    "implante", "endovascular", "stent", "clip", "derivação",
    "derivacao", "shunt", "estimulação", "estimulacao",
}

# OPME importados / sem similar nacional comum
OPME_IMPORTADO_KEYWORDS = {
    "imported", "import", "importado",
    "synthes", "medtronic", "depuy", "stryker", "nuvasive",
    "globus", "zimmer", "biomet",
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _r(rule: dict, passed: bool, msg: str = "",
       rationale: str = "", delta_override: int | None = None) -> dict:
    delta  = 0 if passed else (delta_override if delta_override is not None else rule.get("score_delta", 0))
    action = "PASS" if passed else rule["failure_action"]
    gate   = (rule.get("gate_if_pass", "GO") if passed else rule.get("gate_if_fail"))
    return {
        "rule_id":             rule["rule_id"],
        "rule_name":           rule["rule_name"],
        "layer":               "OPERADORA",
        "passed":              passed,
        "severity":            rule["severity"],
        "blocking":            False,
        "failure_action":      action,
        "gate_suggestion":     gate,
        "score_delta":         delta,
        "user_message":        "" if passed else msg,
        "technical_rationale": rationale,
        "rule_source":         rule.get("source_reference", ""),
    }


def _get_rule(rule_id: str) -> dict:
    return next(r for r in OP_RULES if r["rule_id"] == rule_id)


# ══════════════════════════════════════════════════════════════════════════════
# AVALIADORES POR REGRA
# ══════════════════════════════════════════════════════════════════════════════

def _eval_struct_001(ctx: dict, profile: dict) -> dict:
    """OP_STRUCT_001 — Operadora identificada."""
    rule = _get_rule("OP_STRUCT_001")
    convenio = ctx.get("convenio", "")
    if not convenio:
        return _r(rule, False,
            "Convênio não informado — impossível aplicar regras de auditoria específicas.",
            "Perfil de operadora é pré-condição para validação operacional.")
    if profile.get("name") in ("OPERADORA GENÉRICA", "DESCONHECIDA"):
        return _r(rule, False,
            f"Operadora '{convenio}' não mapeada — aplicando regras genéricas. "
            "Verificar nome correto do convênio.",
            "Sem perfil específico, regras conservadoras genéricas são aplicadas.")
    return _r(rule, True)


def _eval_conserv_001(ctx: dict, profile: dict) -> dict:
    """OP_CONSERV_001 — Conservador mínimo por operadora."""
    rule     = _get_rule("OP_CONSERV_001")
    semanas  = ctx.get("semanas_conservador", 0)
    deficit  = ctx.get("tem_deficit_motor", False)
    urgencia = ctx.get("urgencia", False)
    minimo   = profile.get("conservador_min_semanas", 6)
    nome_op  = profile.get("name", "operadora")

    # Urgência com déficit → conservador não é exigência operacional
    if urgencia and deficit:
        return _r(rule, True)

    # SulAmérica: bloqueia sem déficit motor independente de conservador
    if profile.get("regras_extras", {}).get("bloqueia_sem_deficit_motor") and not deficit and not urgencia:
        return _r(rule, False,
            f"{nome_op} não autoriza procedimento eletivo sem déficit motor documentado. "
            "Regra de auditoria específica desta operadora.",
            f"{nome_op} tem critério restritivo: exige déficit motor objetivo para autorização eletiva.")

    if semanas < minimo:
        return _r(rule, False,
            f"Conservador insuficiente ({semanas} sem.) — {nome_op} exige {minimo} sem. mínimas. "
            "Documentar exceção ou complementar tempo de tratamento.",
            f"Manual de auditoria {nome_op} define conservador mínimo de {minimo} semanas.")
    return _r(rule, True)


def _eval_relatorio_001(ctx: dict, profile: dict) -> dict:
    """OP_RELATORIO_001 — Relatório clínico com densidade suficiente."""
    rule    = _get_rule("OP_RELATORIO_001")
    rel     = ctx.get("relatorio_clinico") or ctx.get("indicacao_clinica") or ""
    minimo  = profile.get("exige_relatorio_min_chars", 150)
    nome_op = profile.get("name", "operadora")
    tam     = len(str(rel))

    extras = profile.get("regras_extras", {})
    pendencias = []

    if tam < minimo:
        pendencias.append(f"relatório com menos de {minimo} caracteres ({tam} atual)")

    if extras.get("exige_laudo_fisio") and not ctx.get("laudo_fisioterapia"):
        pendencias.append("laudo de fisioterapia")

    if extras.get("exige_evolucao_clinica") and not ctx.get("evolucao_clinica"):
        pendencias.append("evolução clínica")

    if extras.get("exige_lateralidade_explicita"):
        texto = str(rel).lower()
        if not any(k in texto for k in ["direito", "esquerdo", "bilateral", "direita", "esquerda"]):
            pendencias.append("lateralidade explícita no relatório")

    if extras.get("exige_oswestry_ou_vas"):
        texto = str(rel).lower()
        if not any(k in texto for k in ["oswestry", "vas", "eva", "escala de dor", "visual analogue"]):
            pendencias.append("escala funcional (Oswestry ou VAS/EVA)")

    if pendencias:
        return _r(rule, False,
            f"Relatório clínico insuficiente para {nome_op}: {'; '.join(pendencias)}.",
            f"{nome_op} exige relatório clínico completo com densidade documental mínima.")
    return _r(rule, True)


def _eval_exames_001(ctx: dict, profile: dict) -> dict:
    """OP_EXAMES_001 — Exames obrigatórios anexados."""
    rule    = _get_rule("OP_EXAMES_001")
    nome_op = profile.get("name", "operadora")
    faltam  = []

    tem_rm = bool(ctx.get("laudo_imagem") or ctx.get("achados_resumo"))
    if profile.get("exige_rm_pre") and not tem_rm:
        faltam.append("RM ou TC com laudo")

    tem_rx = bool(ctx.get("rx_simples"))
    if not profile.get("aceita_rx_como_base") and not tem_rm and tem_rx:
        faltam.append("RM (RX simples não é aceito como exame base por esta operadora)")

    if faltam:
        return _r(rule, False,
            f"Exames obrigatórios ausentes para {nome_op}: {'; '.join(faltam)}. "
            "Operadora rejeita solicitação sem correlação com exame de imagem objetivo.",
            f"{nome_op} exige exame de imagem com laudo como pré-requisito de autorização.")
    return _r(rule, True)


def _eval_guia_001(ctx: dict, profile: dict) -> dict:
    """OP_GUIA_001 — Campos críticos da guia completos."""
    rule    = _get_rule("OP_GUIA_001")
    nome_op = profile.get("name", "operadora")
    faltam  = []

    if profile.get("exige_crm_cbo"):
        if not ctx.get("crm"): faltam.append("CRM")
        if not ctx.get("cbo"): faltam.append("CBO")

    if not ctx.get("cid_principal"):
        faltam.append("CID principal")

    if not ctx.get("procedimento"):
        faltam.append("procedimento")

    if faltam:
        return _r(rule, False,
            f"Guia incompleta para {nome_op}: campos ausentes: {', '.join(faltam)}. "
            "Guia rejeitada automaticamente pelo sistema de auditoria.",
            "Campos obrigatórios da guia são validados eletronicamente antes de chegarem ao auditor.")
    return _r(rule, True)


def _eval_opme_001(ctx: dict, profile: dict) -> dict:
    """OP_OPME_001 — OPME com justificativa clínica."""
    rule = _get_rule("OP_OPME_001")
    if not ctx.get("necessita_opme"):
        return _r(rule, True)  # sem OPME → não se aplica

    nome_op = profile.get("name", "operadora")
    justif  = ctx.get("justificativa_opme") or ctx.get("justificativas_opme")
    if not justif:
        return _r(rule, False,
            f"OPME sem justificativa clínica individualizada — {nome_op} glosa sistematicamente. "
            "Incluir justificativa por item com embasamento técnico.",
            "Justificativa clínica individualizada por item OPME é exigência de todas as operadoras.")
    return _r(rule, True)


def _eval_opme_002(ctx: dict, profile: dict) -> dict:
    """OP_OPME_002 — 3 cotações obrigatórias."""
    rule = _get_rule("OP_OPME_002")
    if not ctx.get("necessita_opme"):
        return _r(rule, True)

    nome_op  = profile.get("name", "operadora")
    min_cot  = profile.get("opme_cotacoes_min", 3)
    cotacoes = ctx.get("cotacoes_opme", [])
    n_cot    = len(cotacoes) if isinstance(cotacoes, list) else int(bool(cotacoes))

    if n_cot < min_cot:
        return _r(rule, False,
            f"OPME incompleta: {n_cot} cotação(ões) de {min_cot} exigidas por {nome_op}. "
            "Sem o tripé documental (indicação + justificativa + cotações), a guia é retida.",
            f"{nome_op} exige {min_cot} orçamentos de fornecedores distintos para análise de OPME.")
    return _r(rule, True)


def _eval_opme_003(ctx: dict, profile: dict) -> dict:
    """OP_OPME_003 — Similar nacional."""
    rule = _get_rule("OP_OPME_003")
    if not ctx.get("necessita_opme"):
        return _r(rule, True)
    if not profile.get("exige_similar_nacional"):
        return _r(rule, True)

    nome_op = profile.get("name", "operadora")
    opme_desc = str(ctx.get("opme_descricao") or ctx.get("justificativa_opme") or "").lower()
    tem_importado = any(kw in opme_desc for kw in OPME_IMPORTADO_KEYWORDS)

    if tem_importado:
        return _r(rule, False,
            f"OPME importado identificado — {nome_op} pode exigir justificativa de ausência de similar nacional. "
            "Verificar se há equivalente nacional disponível.",
            "Política de OPME das operadoras prevê preferência por similar nacional quando disponível.")
    return _r(rule, True)


def _eval_auditoria_001(ctx: dict, profile: dict) -> dict:
    """OP_AUDITORIA_001 — Regras específicas de auditoria da operadora."""
    rule    = _get_rule("OP_AUDITORIA_001")
    nome_op = profile.get("name", "operadora")
    extras  = profile.get("regras_extras", {})
    alertas = []

    # Cap de itens OPME
    cap = extras.get("cap_opme_itens", 20)
    opme_itens = ctx.get("opme_itens_count", 0)
    if opme_itens > cap:
        alertas.append(f"quantidade de itens OPME ({opme_itens}) acima do padrão {nome_op} ({cap})")

    # Bradesco: lateralidade
    if extras.get("exige_lateralidade_explicita"):
        texto = str(ctx.get("indicacao_clinica","") or "").lower()
        if not any(k in texto for k in ["direito","esquerdo","bilateral","direita","esquerda"]):
            alertas.append("lateralidade não identificada no texto — Bradesco exige explicitação")

    # SulAmérica: escala funcional
    if extras.get("exige_oswestry_ou_vas"):
        texto = str(ctx.get("indicacao_clinica","") or "").lower()
        if not any(k in texto for k in ["oswestry","vas","eva","escala de dor"]):
            alertas.append("escala funcional (Oswestry/VAS) ausente — exigida por SulAmérica")

    if alertas:
        return _r(rule, False,
            f"Pendências específicas de auditoria {nome_op}: {'; '.join(alertas)}.",
            f"Manual de auditoria de {nome_op} tem requisitos adicionais além do padrão ANS.")
    return _r(rule, True)


def _eval_prazo_001(ctx: dict, profile: dict) -> dict:
    """OP_PRAZO_001 — Urgência/eletivo e prazo de resposta."""
    rule     = _get_rule("OP_PRAZO_001")
    urgencia = ctx.get("urgencia", False)
    nome_op  = profile.get("name", "operadora")
    prazo    = profile.get("prazo_resposta_dias", 10)

    if urgencia:
        # Urgência: prazo é 4h (RN ANS 465) — só alerta
        return _r(rule, True)

    # Eletivo: lembrete do prazo de resposta
    return {
        **_r(rule, True),
        "user_message": f"Prazo de resposta {nome_op} para caso eletivo: até {prazo} dias úteis.",
        "failure_action": "PASS",
    }


# Mapa rule_id → avaliador
_EVALUATORS_OP = {
    "OP_STRUCT_001":    _eval_struct_001,
    "OP_CONSERV_001":   _eval_conserv_001,
    "OP_RELATORIO_001": _eval_relatorio_001,
    "OP_EXAMES_001":    _eval_exames_001,
    "OP_GUIA_001":      _eval_guia_001,
    "OP_OPME_001":      _eval_opme_001,
    "OP_OPME_002":      _eval_opme_002,
    "OP_OPME_003":      _eval_opme_003,
    "OP_AUDITORIA_001": _eval_auditoria_001,
    "OP_PRAZO_001":     _eval_prazo_001,
}

# ══════════════════════════════════════════════════════════════════════════════
# CONSOLIDAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

def _risk_level(delta_total: int, failed: list) -> str:
    criticos = sum(1 for _ in failed)
    if delta_total <= -50 or criticos >= 4: return "CRITICO"
    if delta_total <= -30 or criticos >= 2: return "ALTO"
    if delta_total <= -15 or criticos >= 1: return "MODERADO"
    return "BAIXO"


def _op_path(risk: str, gate: str) -> str:
    if gate == "NO_GO":            return "NO_GO"
    if risk in ("CRITICO","ALTO"): return "GO_COM_RESSALVAS"
    if risk == "MODERADO":         return "GO_COM_RESSALVAS"
    return "GO"


def _gate_from(results: list[dict]) -> str:
    gate = "GO"
    for r in results:
        sug = r.get("gate_suggestion") or ""
        if sug == "NO_GO":                              return "NO_GO"
        if sug == "GO_COM_RESSALVAS" and gate == "GO":  gate = "GO_COM_RESSALVAS"
    return gate


# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════

def run_operadora_validation(ctx: dict) -> dict:
    """
    Executa todas as regras de Operadora em ordem de prioridade.

    ctx: dict com campos do caso:
        convenio, procedimento, cid_principal, crm, cbo,
        indicacao_clinica, relatorio_clinico, laudo_imagem,
        achados_resumo, laudo_fisioterapia, evolucao_clinica,
        semanas_conservador, tem_deficit_motor, urgencia,
        necessita_opme, opme_itens_count, opme_descricao,
        justificativa_opme, cotacoes_opme (list),
        rx_simples (bool)

    operator_profile pode ser injetado via ctx['_operator_profile']
    para override (ex: carregado de Sheets).
    """
    profile  = ctx.get("_operator_profile") or get_operator_profile(ctx.get("convenio", ""))
    results  : list[dict] = []
    failed   : list[str]  = []
    pending  : list[str]  = []
    delta    = 0
    gate     = "GO"

    sorted_rules = sorted(OP_RULES, key=lambda r: r["priority"])

    for rule in sorted_rules:
        rule_id   = rule["rule_id"]
        evaluator = _EVALUATORS_OP.get(rule_id)
        if not evaluator:
            logger.warning("validator_operadora: sem evaluator para %s", rule_id)
            continue

        result = evaluator(ctx, profile)
        results.append(result)
        delta += result.get("score_delta", 0)

        if not result["passed"]:
            failed.append(rule_id)
            if result["user_message"]:
                pending.append(result["user_message"])

    gate    = _gate_from(results)
    risk    = _risk_level(delta, failed)
    path    = _op_path(risk, gate)
    overall = "FAIL" if len(failed) > len(results) // 2 else ("WARN" if failed else "PASS")

    logger.info(
        "validator_operadora: op=%s risk=%s gate=%s path=%s delta=%d failed=%s",
        profile.get("name"), risk, gate, path, delta, failed,
    )

    return {
        "layer":                     "OPERADORA",
        "overall":                   overall,
        "blocking":                  False,
        "gate":                      gate,
        "operator_name":             profile.get("name", ""),
        "operator_risk_level":       risk,
        "operator_pending_items":    pending,
        "operator_recommended_path": path,
        "score_delta_total":         delta,
        "results":                   results,
        "failed_rules":              failed,
    }
