"""
app/services/validator_ans.py
NEUROAUTH — Validator ANS v1.0.0

Camada 1 do motor de regras: filtro regulatório duro.
Avalia Rol, DUT, compatibilidade TUSS×CID, campos obrigatórios e exclusões.

Contrato de saída por regra:
{
    "rule_id":        str,
    "rule_name":      str,
    "layer":          "ANS",
    "passed":         bool,
    "severity":       "BAIXA" | "MODERADA" | "ALTA" | "CRITICA",
    "blocking":       bool,
    "failure_action": "BLOCK" | "WARN" | "REQUEST_DOC" | "SCORE_DOWN",
    "gate_suggestion": "NO_GO" | "GO_COM_RESSALVAS" | "GO" | None,
    "score_impact":   int,   # negativo
    "user_message":   str,
    "rule_source":    str,
}

Saída consolidada do validator:
{
    "layer":        "ANS",
    "overall":      "PASS" | "FAIL" | "WARN",
    "blocking":     bool,          # True se qualquer regra CRITICA falhou
    "gate":         "NO_GO" | "GO_COM_RESSALVAS" | "GO",
    "score_impact": int,           # soma dos impactos
    "results":      list[dict],    # resultado por regra
    "failed_rules": list[str],     # rule_ids que falharam
    "blocked_by":   str | None,    # primeira regra bloqueante
}
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("neuroauth.validator_ans")

# ══════════════════════════════════════════════════════════════════════════════
# RULE LIBRARY — ANS
# Fonte: regras embutidas v1.0.0
# Substituível por: leitura de Google Sheets (tab RULE_LIBRARY, layer=ANS)
# ══════════════════════════════════════════════════════════════════════════════

# Mapa TUSS → família de CIDs aceitos
# Fonte: Rol ANS + DUT coluna + compatibilidade TUSS×CID
TUSS_CID_MAP: dict[str, list[str]] = {
    # Artrodese cervical (ACDF 1 e 2 níveis)
    "30713162": ["M50", "M47", "M48", "G54", "M96"],
    "30713170": ["M50", "M47", "M48", "G54", "M96"],
    # Artrodese lombar PLIF/TLIF
    "30714011": ["M51", "M47", "M48", "M43", "M96", "G55"],
    "30714038": ["M51", "M47", "M48", "M43", "M96", "G55"],
    # Discectomia cervical
    "30713081": ["M50", "G54", "M47"],
    # Discectomia lombar
    "30714003": ["M51", "G55", "M47"],
    # Laminectomia
    "30714070": ["M48", "M47", "M51", "G99"],
    # Craniectomia descompressiva
    "30601014": ["S06", "I63", "G93", "G91"],
    # Clipagem de aneurisma
    "30607034": ["I60", "Q28"],
    # DVE / derivação ventricular
    "30608014": ["G91", "G93", "I60", "S06"],
    # Embolização endovascular
    "30607077": ["I60", "I61", "I67", "Q28"],
}

# CIDs que habilitam urgência/emergência (deficit motor, compressão aguda)
CIDS_URGENCIA = {"G82", "G83", "G54", "G55", "S14", "S24", "S34", "I60", "I61", "I63"}

# Conservador mínimo por convênio (semanas) — espelha engine_v3
CONSERVADOR_MIN: dict[str, int] = {
    "sulamerica": 8, "sulamérica": 8,
    "bradesco": 8,
    "unimed": 6, "amil": 6, "hapvida": 6, "cassi": 4,
}
CONSERVADOR_DEFAULT = 6

# ══════════════════════════════════════════════════════════════════════════════
# ANS RULES v1.0.0
# Cada regra é um dict com o schema da Rule Library
# ══════════════════════════════════════════════════════════════════════════════

ANS_RULES: list[dict[str, Any]] = [

    # ── R001: Campos obrigatórios mínimos ────────────────────────────────────
    {
        "rule_id":        "ANS_STRUCT_001",
        "rule_name":      "Campos obrigatórios mínimos",
        "priority":       1,
        "severity":       "CRITICA",
        "blocking":       True,
        "rule_type":      "CHECKLIST",
        "failure_action": "BLOCK",
        "gate_if_fail":   "NO_GO",
        "score_impact":   -40,
        "source_reference": "Resolução Normativa ANS 501/2022",
    },

    # ── R002: CRM e CBO do solicitante ───────────────────────────────────────
    {
        "rule_id":        "ANS_SADT_001",
        "rule_name":      "CRM e CBO do solicitante obrigatórios",
        "priority":       2,
        "severity":       "ALTA",
        "blocking":       False,
        "rule_type":      "CHECKLIST",
        "failure_action": "REQUEST_DOC",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_impact":   -15,
        "source_reference": "RN ANS 501 — guia SADT campos obrigatórios",
    },

    # ── R003: CID presente e completo (mínimo 4 chars: letra+3 dígitos) ──────
    {
        "rule_id":        "ANS_CID_001",
        "rule_name":      "CID principal completo",
        "priority":       3,
        "severity":       "CRITICA",
        "blocking":       True,
        "rule_type":      "BINARIA",
        "failure_action": "BLOCK",
        "gate_if_fail":   "NO_GO",
        "score_impact":   -30,
        "source_reference": "Tabela CID-10 / TISS ANS — campo obrigatório SADT",
    },

    # ── R004: Compatibilidade TUSS × CID ────────────────────────────────────
    {
        "rule_id":        "ANS_TUSS_001",
        "rule_name":      "Compatibilidade TUSS × CID",
        "priority":       4,
        "severity":       "ALTA",
        "blocking":       False,
        "rule_type":      "BINARIA",
        "failure_action": "WARN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_impact":   -20,
        "source_reference": "Mapa TUSS×CID — Rol ANS + DUT",
    },

    # ── R005: DUT — Tratamento conservador mínimo (eletivo, sem déficit) ─────
    {
        "rule_id":        "ANS_DUT_COLUNA_001",
        "rule_name":      "DUT conservador mínimo — coluna eletivo",
        "priority":       5,
        "severity":       "CRITICA",
        "blocking":       True,
        "rule_type":      "BINARIA",
        "failure_action": "BLOCK",
        "gate_if_fail":   "NO_GO",
        "score_impact":   -35,
        "source_reference": "DUT coluna — item 3 — ANS",
        "applies_if": {"eletivo": True, "deficit_motor": False,
                       "familias_proc": ["ACDF","ARTRODESE","DISCECTOMIA","LAMINECTOMIA"]},
        "excludes_if":    {"urgencia": True},
    },

    # ── R006: DUT — Urgência/emergência exige CID compatível ─────────────────
    {
        "rule_id":        "ANS_DUT_URG_001",
        "rule_name":      "Urgência exige CID de emergência neurológica",
        "priority":       6,
        "severity":       "ALTA",
        "blocking":       False,
        "rule_type":      "BINARIA",
        "failure_action": "WARN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_impact":   -20,
        "source_reference": "RN ANS 465 — urgência e emergência",
        "applies_if":     {"urgencia": True},
    },

    # ── R007: Indicação clínica mínima (não pode ser genérica) ───────────────
    {
        "rule_id":        "ANS_INDICACAO_001",
        "rule_name":      "Indicação clínica específica obrigatória",
        "priority":       7,
        "severity":       "ALTA",
        "blocking":       False,
        "rule_type":      "SCORE",
        "failure_action": "WARN",
        "gate_if_fail":   "GO_COM_RESSALVAS",
        "score_impact":   -15,
        "source_reference": "Critérios de pertinência ANS / auditoria técnica",
    },

    # ── R008: Procedimento no Rol ANS ───────────────────────────────────────
    {
        "rule_id":        "ANS_ROL_001",
        "rule_name":      "Procedimento no Rol ANS",
        "priority":       8,
        "severity":       "CRITICA",
        "blocking":       True,
        "rule_type":      "BINARIA",
        "failure_action": "BLOCK",
        "gate_if_fail":   "NO_GO",
        "score_impact":   -50,
        "source_reference": "Rol de Procedimentos ANS — RN 465/2021",
    },
]

# Procedimentos que estão no Rol ANS (neurocirurgia/coluna — lista base)
ROL_ANS_KEYWORDS = {
    "artrodese", "acdf", "discectomia", "laminectomia", "laminoplastia",
    "descompressão", "descompressao", "microdiscectomia",
    "clipagem", "aneurisma", "embolização", "embolizacao",
    "craniectomia", "craniotomia", "derivação", "derivacao",
    "dve", "dvp", "drenagem", "biópsia", "biopsia",
    "ressecção", "resseccao", "tumor", "hematoma",
}

INDICACAO_GENERICA = {
    "dor", "lombalgia", "cervicalgia", "cefaleia", "dormência",
    "formigamento", "fraqueza", "sem melhora", "piora",
}

FAMILIA_PROC: dict[str, list[str]] = {
    "ACDF":        ["artrodese cervi", "acdf", "cervical anterior"],
    "ARTRODESE":   ["artrodese", "tlif", "plif", "alif", "xlif"],
    "DISCECTOMIA": ["discectomia", "microdiscectomia"],
    "LAMINECTOMIA":["laminectomia", "laminoplastia", "descompressão", "descompressao"],
    "CRANIO":      ["craniotomia", "craniectomia", "clipagem", "hematoma"],
    "ENDOVASCULAR":["embolização", "embolizacao", "coiling", "stent", "trombectomia"],
    "DVE":         ["derivação ventricular", "derivacao ventricular", "dve", "dvp"],
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_conservador_min(convenio: str) -> int:
    cl = convenio.lower()
    for k, v in CONSERVADOR_MIN.items():
        if k in cl:
            return v
    return CONSERVADOR_DEFAULT


def _detectar_familia_proc(procedimento: str) -> list[str]:
    p = procedimento.lower()
    return [fam for fam, kws in FAMILIA_PROC.items() if any(kw in p for kw in kws)]


def _cid_prefixo(cid: str) -> str:
    """Extrai prefixo de 3 chars do CID: M50.1 → M50."""
    return cid[:3] if cid else ""


def _tuss_compativel_com_cid(tuss: str, cid: str) -> bool | None:
    """
    Verifica se TUSS é compatível com CID.
    Retorna None se TUSS não estiver no mapa (não aplicável).
    """
    if not tuss or tuss not in TUSS_CID_MAP:
        return None
    prefixo = _cid_prefixo(cid)
    return prefixo in TUSS_CID_MAP[tuss]


def _indicacao_especifica(indicacao: str) -> bool:
    """True se indicação tem especificidade mínima (não é genérica curta)."""
    if not indicacao or len(indicacao) < 30:
        return False
    tokens = set(indicacao.lower().split())
    genericos = tokens & INDICACAO_GENERICA
    return len(genericos) < 3  # aceita alguma sobreposição


def _resultado(rule: dict, passed: bool, msg_fail: str = "") -> dict:
    return {
        "rule_id":         rule["rule_id"],
        "rule_name":       rule["rule_name"],
        "layer":           "ANS",
        "passed":          passed,
        "severity":        rule["severity"],
        "blocking":        rule["blocking"] and not passed,
        "failure_action":  rule["failure_action"] if not passed else "PASS",
        "gate_suggestion": rule["gate_if_fail"] if not passed else rule.get("gate_if_pass", "GO"),
        "score_impact":    rule["score_impact"] if not passed else 0,
        "user_message":    msg_fail if not passed else "",
        "rule_source":     rule.get("source_reference", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
# AVALIADORES POR REGRA
# ══════════════════════════════════════════════════════════════════════════════

def _eval_struct_001(ctx: dict) -> dict:
    """ANS_STRUCT_001 — Campos obrigatórios mínimos."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_STRUCT_001")
    missing = []
    if not ctx.get("cid_principal"):  missing.append("CID principal")
    if not ctx.get("procedimento"):   missing.append("procedimento")
    if not ctx.get("convenio"):       missing.append("convênio")
    if not ctx.get("indicacao_clinica"): missing.append("indicação clínica")
    if missing:
        return _resultado(rule, False,
            f"Campos obrigatórios ausentes: {', '.join(missing)}. "
            "Guia rejeitada automaticamente pelo sistema TISS.")
    return _resultado(rule, True)


def _eval_sadt_001(ctx: dict) -> dict:
    """ANS_SADT_001 — CRM e CBO obrigatórios."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_SADT_001")
    missing = []
    if not ctx.get("crm"): missing.append("CRM")
    if not ctx.get("cbo"): missing.append("CBO")
    if missing:
        return _resultado(rule, False,
            f"Guia sem {'/'.join(missing)} rejeitada por auditoria eletrônica. "
            "CRM e CBO do médico solicitante são obrigatórios no TISS.")
    return _resultado(rule, True)


def _eval_cid_001(ctx: dict) -> dict:
    """ANS_CID_001 — CID completo."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_CID_001")
    cid = ctx.get("cid_principal", "")
    ok = bool(cid and len(cid) >= 4 and re.match(r"^[A-Z]\d{2}", cid))
    if not ok:
        return _resultado(rule, False,
            f"CID incompleto ou ausente ('{cid}'). "
            "Glosa automática no TISS — CID deve ter letra + 3 dígitos mínimo.")
    return _resultado(rule, True)


def _eval_tuss_001(ctx: dict) -> dict:
    """ANS_TUSS_001 — Compatibilidade TUSS×CID."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_TUSS_001")
    tuss = ctx.get("procedimento_tuss", "")
    cid  = ctx.get("cid_principal", "")
    compat = _tuss_compativel_com_cid(tuss, cid)
    if compat is None:
        return _resultado(rule, True)  # TUSS não mapeado → não bloqueia
    if not compat:
        esperados = TUSS_CID_MAP.get(tuss, [])
        return _resultado(rule, False,
            f"TUSS {tuss} incompatível com CID {cid}. "
            f"CIDs aceitos para este procedimento: {', '.join(esperados)}. "
            "Inconsistência gera glosa técnica automática.")
    return _resultado(rule, True)


def _eval_dut_coluna_001(ctx: dict) -> dict:
    """ANS_DUT_COLUNA_001 — Conservador mínimo eletivo sem déficit."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_DUT_COLUNA_001")

    # Verifica se a regra se aplica ao procedimento
    familias = _detectar_familia_proc(ctx.get("procedimento", ""))
    familias_alvo = {"ACDF", "ARTRODESE", "DISCECTOMIA", "LAMINECTOMIA"}
    if not (familias_alvo & set(familias)):
        return _resultado(rule, True)  # procedimento fora do escopo

    urgencia   = ctx.get("urgencia", False)
    deficit    = ctx.get("tem_deficit_motor", False)
    eletivo    = not urgencia
    semanas    = ctx.get("semanas_conservador", 0)
    convenio   = ctx.get("convenio", "")
    min_semanas = _get_conservador_min(convenio)

    # Urgência ou déficit motor → DUT não bloqueia
    if urgencia or deficit:
        return _resultado(rule, True)

    # Eletivo sem déficit → exige conservador mínimo
    if semanas < min_semanas:
        return _resultado(rule, False,
            f"Tratamento conservador insuficiente ({semanas} sem.) — "
            f"DUT exige {min_semanas} sem. para caso eletivo sem déficit motor. "
            f"Documentar falha do conservador ou justificativa de urgência.")
    return _resultado(rule, True)


def _eval_dut_urg_001(ctx: dict) -> dict:
    """ANS_DUT_URG_001 — Urgência deve ter CID de emergência."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_DUT_URG_001")
    urgencia = ctx.get("urgencia", False)
    if not urgencia:
        return _resultado(rule, True)  # não se aplica
    cid = _cid_prefixo(ctx.get("cid_principal", ""))
    if cid not in CIDS_URGENCIA:
        return _resultado(rule, False,
            f"Caso marcado como urgência mas CID {ctx.get('cid_principal','')} "
            "não é reconhecido como emergência neurológica. "
            "Revisar caráter ou justificar clinicamente.")
    return _resultado(rule, True)


def _eval_indicacao_001(ctx: dict) -> dict:
    """ANS_INDICACAO_001 — Indicação específica."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_INDICACAO_001")
    if not _indicacao_especifica(ctx.get("indicacao_clinica", "")):
        return _resultado(rule, False,
            "Indicação genérica — auditoria rejeita sem especificidade clínica objetiva. "
            "Incluir: diagnóstico principal, tempo de sintomas, achados de imagem, "
            "tratamentos tentados e resposta.")
    return _resultado(rule, True)


def _eval_rol_001(ctx: dict) -> dict:
    """ANS_ROL_001 — Procedimento no Rol ANS."""
    rule = next(r for r in ANS_RULES if r["rule_id"] == "ANS_ROL_001")
    proc = ctx.get("procedimento", "").lower()
    tuss = ctx.get("procedimento_tuss", "")

    # TUSS mapeado → está no Rol
    if tuss and tuss in TUSS_CID_MAP:
        return _resultado(rule, True)

    # Verifica por keyword
    if any(kw in proc for kw in ROL_ANS_KEYWORDS):
        return _resultado(rule, True)

    if not proc:
        return _resultado(rule, False, "Procedimento ausente — impossível verificar cobertura no Rol ANS.")

    return _resultado(rule, False,
        f"Procedimento '{ctx.get('procedimento','')}' não identificado no Rol ANS. "
        "Verificar cobertura contratual antes de submeter autorização.")


# Mapa rule_id → função avaliadora
_EVALUATORS = {
    "ANS_STRUCT_001":      _eval_struct_001,
    "ANS_SADT_001":        _eval_sadt_001,
    "ANS_CID_001":         _eval_cid_001,
    "ANS_TUSS_001":        _eval_tuss_001,
    "ANS_DUT_COLUNA_001":  _eval_dut_coluna_001,
    "ANS_DUT_URG_001":     _eval_dut_urg_001,
    "ANS_INDICACAO_001":   _eval_indicacao_001,
    "ANS_ROL_001":         _eval_rol_001,
}


# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════

def _load_external_ans_rules() -> list[dict]:
    """
    Tenta carregar regras ANS da Rule Library externa.
    Retorna lista vazia se Sheets não disponível (validators usam fallback embutido).
    """
    try:
        from app.services.rule_library_adapter import get_rules_by_layer
        rules = get_rules_by_layer("ANS")
        if rules:
            logger.info("validator_ans: %d regras externas carregadas", len(rules))
        return rules
    except Exception as exc:
        logger.debug("validator_ans: Rule Library externa indisponível (%s) — usando fallback", exc)
        return []


def _apply_external_rule(rule: dict, ctx: dict) -> dict | None:
    """
    Avalia uma regra externa usando o parser do adapter.
    Retorna resultado padronizado ou None se a regra não se aplica.
    """
    try:
        from app.services.rule_library_adapter import evaluate_condition
        applies = evaluate_condition(rule.get("applies_if_json", ""), ctx)
        excludes = evaluate_condition(rule.get("excludes_if_json", ""), ctx) if rule.get("excludes_if_json") else False
        if excludes or not applies:
            return None
        passed = evaluate_condition(rule.get("validation_logic_json", ""), ctx)
        return _resultado(rule, passed,
            rule.get("user_message", "") if not passed else "")
    except Exception as exc:
        logger.warning("validator_ans: erro em regra externa %s: %s", rule.get("rule_id"), exc)
        return None


def run_ans_validation(ctx: dict) -> dict:
    """
    Executa todas as regras ANS em ordem de prioridade.
    Tenta usar regras externas (Sheets); fallback para embutidas se indisponível.

    ctx: dict com campos do caso clínico:
        cid_principal, procedimento, procedimento_tuss, convenio,
        indicacao_clinica, crm, cbo, semanas_conservador,
        tem_deficit_motor, urgencia

    Retorna dict consolidado com gate, blocking, score_impact e results por regra.
    """
    results: list[dict] = []
    total_score_impact = 0
    blocked = False
    blocked_by: str | None = None
    gate = "GO"
    external_applied = 0

    # Tentar regras externas como suplemento (não substituição das embutidas críticas)
    ext_rules = _load_external_ans_rules()
    for ext_rule in sorted(ext_rules, key=lambda r: r.get("priority", 99)):
        result = _apply_external_rule(ext_rule, ctx)
        if result is None:
            continue
        results.append(result)
        external_applied += 1
        if not result["passed"]:
            total_score_impact += result["score_impact"]
            sug = result.get("gate_suggestion", "")
            if sug == "NO_GO": gate = "NO_GO"
            elif sug == "GO_COM_RESSALVAS" and gate != "NO_GO": gate = "GO_COM_RESSALVAS"
            if result["blocking"] and not blocked:
                blocked = True
                blocked_by = ext_rule["rule_id"]
                break

    if external_applied:
        logger.info("validator_ans: %d regras externas aplicadas", external_applied)

    sorted_rules = sorted(ANS_RULES, key=lambda r: r["priority"])
    # Se ANS já foi bloqueado por regra externa crítica, pula as embutidas
    if blocked:
        failed = [r["rule_id"] for r in results if not r["passed"]]
        overall = "FAIL"
        return {
            "layer": "ANS", "overall": overall, "blocking": blocked,
            "gate": gate, "score_impact": total_score_impact,
            "results": results, "failed_rules": failed, "blocked_by": blocked_by,
        }

    for rule in sorted_rules:
        rule_id = rule["rule_id"]
        evaluator = _EVALUATORS.get(rule_id)
        if not evaluator:
            logger.warning("validator_ans: sem evaluator para %s", rule_id)
            continue

        result = evaluator(ctx)
        results.append(result)

        if not result["passed"]:
            total_score_impact += result["score_impact"]

            # Atualiza gate para o mais restritivo
            sug = result.get("gate_suggestion", "")
            if sug == "NO_GO":
                gate = "NO_GO"
            elif sug == "GO_COM_RESSALVAS" and gate != "NO_GO":
                gate = "GO_COM_RESSALVAS"

            # Primeiro bloqueio crítico encerra o fluxo
            if result["blocking"] and not blocked:
                blocked = True
                blocked_by = rule_id
                logger.info("validator_ans: BLOCK em %s — %s", rule_id, result["user_message"][:80])
                break  # hard stop: não avalia regras seguintes

    failed = [r["rule_id"] for r in results if not r["passed"]]
    overall = "PASS" if not failed else ("FAIL" if blocked else "WARN")

    return {
        "layer":        "ANS",
        "overall":      overall,
        "blocking":     blocked,
        "gate":         gate,
        "score_impact": total_score_impact,
        "results":      results,
        "failed_rules": failed,
        "blocked_by":   blocked_by,
    }
