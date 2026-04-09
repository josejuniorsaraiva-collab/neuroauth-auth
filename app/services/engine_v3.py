"""
app/services/engine_v3.py
NEUROAUTH — Motor Decisório v1.3 — 3 Camadas Determinísticas

Arquitetura:
  CAMADA 1: Validação estrutural (campos obrigatórios, consistência) → PASS/FAIL
  CAMADA 2: Validação clínica (diagnóstico sustenta procedimento? tto conservador? gravidade?) → SCORE 0-1 + FLAGS
  CAMADA 3: Anti-glosa (coerência OPME × procedimento, excesso/ausência de material, risco auditoria) → risco + falhas

GATE BINÁRIO após camadas:
  estrutural_fail → NO_GO
  risco_alto → NO_GO
  score_clinico < 0.6 → GO_COM_RESSALVAS
  else → GO

Cada camada é testável e auditável independentemente.
"""

import re
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

from app.models.decide import DecideRequest

logger = logging.getLogger("neuroauth.engine_v3")

ENGINE_VERSION = "1.3"


# ═══════════════════════════════════════════════════════════════════════════════
# INPUT NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class NormalizedInput:
    """Payload normalizado com campos obrigatórios padronizados."""
    episodio_id: str = ""
    cid_principal: str = ""
    cid_secundarios: list = field(default_factory=list)
    procedimento: str = ""
    procedimento_tuss: str = ""
    convenio: str = ""
    convenio_lower: str = ""
    indicacao_clinica: str = ""
    achados_resumo: str = ""
    tto_conservador: str = ""
    semanas_conservador: int = 0
    necessita_opme: bool = False
    opme_items_count: int = 0
    opme_items_completos: bool = False
    crm: str = ""
    cbo: str = ""
    carater: str = "eletivo"
    tem_deficit_motor: bool = False
    input_hash: str = ""


def normalize_input(req: DecideRequest) -> NormalizedInput:
    """Padroniza entrada, extrai features derivadas."""
    n = NormalizedInput()
    n.episodio_id = req.episodio_id or ""
    n.cid_principal = (req.cid_principal or "").strip().upper()
    n.procedimento = (req.procedimento or "").strip()
    n.procedimento_tuss = (req.cod_cbhpm or "").strip()
    n.convenio = (req.convenio or "").strip()
    n.convenio_lower = n.convenio.lower()
    n.indicacao_clinica = (req.indicacao_clinica or "").strip()
    n.achados_resumo = (req.achados_resumo or "").strip()
    n.tto_conservador = (req.tto_conservador or "").strip()
    n.semanas_conservador = _extrair_semanas(n.tto_conservador)
    n.necessita_opme = (req.necessita_opme or "").strip().lower() == "sim"
    n.opme_items_count = len(req.opme_items) if req.opme_items else 0
    n.opme_items_completos = (
        n.opme_items_count > 0
        and all(item.descricao and item.qtd > 0 for item in (req.opme_items or []))
    )
    n.crm = (req.crm or "").strip()
    n.cbo = (req.cbo or "").strip()
    n.carater = getattr(req, "carater", "eletivo") or "eletivo"
    n.tem_deficit_motor = _detectar_deficit_motor(n.indicacao_clinica)

    # CID secundários
    if hasattr(req, "cid_secundarios") and req.cid_secundarios:
        n.cid_secundarios = [c.strip().upper() for c in req.cid_secundarios if c]

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 1: VALIDAÇÃO ESTRUTURAL
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StructuralResult:
    """Resultado da validação estrutural."""
    passed: bool = True
    falhas: list = field(default_factory=list)
    avisos: list = field(default_factory=list)
    campos_presentes: int = 0
    campos_obrigatorios: int = 7  # total de campos obrigatórios


STRUCTURAL_RULES = [
    # (campo_check, codigo, mensagem_falha, is_bloqueio)
    ("cid_principal",      "E001", "CID principal ausente", True),
    ("procedimento",       "E002", "Procedimento ausente", True),
    ("convenio",           "E003", "Convênio ausente — impossível aplicar regras regulatórias", True),
    ("indicacao_clinica",  "E004", "Indicação clínica ausente", True),
]

STRUCTURAL_WARNINGS = [
    ("achados_resumo",     "E005", "Achados de imagem ausentes — fragilidade documental"),
    ("crm",                "E006", "CRM do solicitante ausente"),
    ("cbo",                "E007", "CBO do solicitante ausente"),
]


def run_structural_validation(n: NormalizedInput) -> StructuralResult:
    """
    CAMADA 1 — Validação estrutural.
    Verifica campos obrigatórios e consistência básica.
    Qualquer falha → FAIL (motor retorna NO_GO).
    """
    r = StructuralResult()

    # Regras bloqueantes
    for campo, codigo, msg, is_bloqueio in STRUCTURAL_RULES:
        valor = getattr(n, campo, "")
        if valor and len(valor) >= 2:
            r.campos_presentes += 1
        else:
            r.falhas.append(f"[{codigo}] {msg}")
            r.passed = False

    # Regras de aviso (não bloqueiam)
    for campo, codigo, msg in STRUCTURAL_WARNINGS:
        valor = getattr(n, campo, "")
        if valor and len(valor) >= 1:
            r.campos_presentes += 1
        else:
            r.avisos.append(f"[{codigo}] {msg}")

    # Validação de consistência: CID deve ter formato válido (mínimo 3 chars, alfanumérico)
    if n.cid_principal and (len(n.cid_principal) < 3 or not n.cid_principal[0].isalpha()):
        r.falhas.append("[E008] CID principal com formato inválido")
        r.passed = False

    # Indicação clínica mínima (30 chars)
    if n.indicacao_clinica and len(n.indicacao_clinica) < 30:
        r.avisos.append("[E009] Indicação clínica muito curta (<30 chars) — risco de rejeição")

    logger.info(
        f"[engine_v3] CAMADA1 passed={r.passed} "
        f"campos={r.campos_presentes}/{r.campos_obrigatorios} "
        f"falhas={len(r.falhas)} avisos={len(r.avisos)}"
    )
    return r


# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 2: VALIDAÇÃO CLÍNICA
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ClinicalResult:
    """Resultado da validação clínica."""
    score: float = 0.0  # 0.0 a 1.0
    flags: list = field(default_factory=list)
    detalhes: dict = field(default_factory=dict)
    pendencias: list = field(default_factory=list)
    pontos_frageis: list = field(default_factory=list)


# Semanas mínimas por convênio
CONVENIO_SEMANAS = {
    "sulamérica": 8, "sulamerica": 8, "sul america": 8,
    "bradesco": 8,
    "unimed": 6, "amil": 6, "hapvida": 6,
}
SEMANAS_DEFAULT = 6


def run_clinical_validation(n: NormalizedInput) -> ClinicalResult:
    """
    CAMADA 2 — Validação clínica.
    Avalia se diagnóstico sustenta procedimento, tratamento conservador,
    gravidade, e completude clínica.
    Retorna score 0-1 + flags.
    """
    r = ClinicalResult()
    pontos = 0.0  # acumula de 0 a 100, depois normaliza
    max_pontos = 100.0

    # ── BLOCO A: Completude clínica (40 pts) ─────────────────────
    bloco_a = 0.0

    # CID (10 pts)
    if n.cid_principal and len(n.cid_principal) >= 4:
        bloco_a += 10
        r.detalhes["cid_ok"] = True
    else:
        r.pendencias.append("CID principal ausente ou incompleto (mínimo 4 chars)")
        r.pontos_frageis.append("[CL001] CID incompleto — glosa automática no TISS")
        r.detalhes["cid_ok"] = False

    # Indicação clínica (10 pts)
    if n.indicacao_clinica and len(n.indicacao_clinica) > 30:
        bloco_a += 10
        r.detalhes["indicacao_ok"] = True
    else:
        r.pendencias.append("Indicação clínica insuficiente (mínimo 30 caracteres)")
        r.pontos_frageis.append("[CL002] Indicação genérica — auditoria rejeita sem especificidade")
        r.detalhes["indicacao_ok"] = False

    # Achados de imagem (10 pts)
    if n.achados_resumo and len(n.achados_resumo) > 20:
        bloco_a += 10
        r.detalhes["achados_ok"] = True
    else:
        r.pendencias.append("Achados de imagem ausentes ou insuficientes")
        r.pontos_frageis.append("[CL003] Sem achados objetivos — fragilidade crítica")
        r.detalhes["achados_ok"] = False

    # CRM + CBO (10 pts)
    if n.crm and n.cbo:
        bloco_a += 10
        r.detalhes["crm_cbo_ok"] = True
    else:
        r.pendencias.append("CRM e/ou CBO do solicitante ausentes")
        r.pontos_frageis.append("[CL004] Guia sem CRM/CBO rejeitada por auditoria eletrônica")
        r.detalhes["crm_cbo_ok"] = False

    r.detalhes["bloco_a_completude"] = bloco_a

    # ── BLOCO B: Tratamento conservador (30 pts) ──────────────────
    bloco_b = 0.0
    semanas_minimas = _get_semanas_minimas(n.convenio_lower)
    r.detalhes["semanas_conservador"] = n.semanas_conservador
    r.detalhes["semanas_minimas"] = semanas_minimas
    r.detalhes["deficit_motor"] = n.tem_deficit_motor

    if n.semanas_conservador >= semanas_minimas:
        bloco_b = 30
        r.flags.append("CONSERVADOR_COMPLETO")
    elif n.semanas_conservador > 0:
        if n.tem_deficit_motor:
            bloco_b = 22  # urgência relativa mitiga
            r.flags.append("CONSERVADOR_PARCIAL_COM_DEFICIT")
            r.pendencias.append(
                f"Conservador ({n.semanas_conservador} sem.) abaixo do mínimo "
                f"{n.convenio} ({semanas_minimas} sem.) — déficit motor justifica "
                "urgência relativa, mas documentar exceção"
            )
        else:
            bloco_b = 15
            r.flags.append("CONSERVADOR_PARCIAL_SEM_DEFICIT")
            r.pendencias.append(
                f"Conservador ({n.semanas_conservador} sem.) abaixo do mínimo "
                f"{n.convenio} ({semanas_minimas} sem.). Documentar exceção."
            )
            r.pontos_frageis.append(
                "[CL005] Conservador insuficiente — principal causa de glosa em coluna"
            )
    else:
        if n.tem_deficit_motor:
            bloco_b = 10
            r.flags.append("SEM_CONSERVADOR_COM_DEFICIT")
            r.pendencias.append("Tratamento conservador não documentado — déficit motor presente")
        else:
            bloco_b = 0
            r.flags.append("SEM_CONSERVADOR_SEM_DEFICIT")
            r.pendencias.append(
                f"Tratamento conservador não documentado. "
                f"{n.convenio} exige {semanas_minimas} sem. ou justificativa de urgência."
            )
            r.pontos_frageis.append("[CL006] Ausência de tto conservador — negativa provável")

    r.detalhes["bloco_b_conservador"] = bloco_b

    # ── BLOCO C: Gravidade e urgência (15 pts) ────────────────────
    bloco_c = 0.0

    if n.carater == "urgencia":
        bloco_c += 10
        r.flags.append("CARATER_URGENCIA")
    elif n.carater == "eletivo":
        bloco_c += 5
    else:
        bloco_c += 5

    # CIDs secundários documentados (contribuição marginal)
    if n.cid_secundarios and len(n.cid_secundarios) >= 1:
        bloco_c += 5
        r.flags.append("CID_SECUNDARIOS_PRESENTES")
    else:
        r.detalhes["cid_sec_ausentes"] = True

    r.detalhes["bloco_c_gravidade"] = bloco_c

    # ── BLOCO D: Convênio aderência (15 pts) ──────────────────────
    bloco_d = 0.0
    if "unimed" in n.convenio_lower:
        bloco_d = 15
    elif any(k in n.convenio_lower for k in ["bradesco", "sulamérica", "sulamerica", "amil", "hapvida"]):
        bloco_d = 12
        r.pendencias.append(f"Convênio '{n.convenio}' — regras específicas. Verificar SADT.")
    else:
        bloco_d = 8
        r.pendencias.append(f"Convênio '{n.convenio}' — verificar regras e cobertura específicas.")

    r.detalhes["bloco_d_convenio"] = bloco_d

    # ── SCORE FINAL CLÍNICO ───────────────────────────────────────
    pontos = bloco_a + bloco_b + bloco_c + bloco_d
    r.score = round(pontos / max_pontos, 3)

    # Cap: conservador insuficiente sem déficit → max 0.74
    if n.semanas_conservador < semanas_minimas and not n.tem_deficit_motor and n.semanas_conservador > 0:
        r.score = min(r.score, 0.74)
        r.flags.append("CAP_CONSERVADOR_INSUFICIENTE")

    r.detalhes["pontos_brutos"] = pontos
    r.detalhes["score_final"] = r.score

    logger.info(
        f"[engine_v3] CAMADA2 score={r.score:.3f} "
        f"blocos=[A={bloco_a},B={bloco_b},C={bloco_c},D={bloco_d}] "
        f"flags={r.flags}"
    )
    return r


# ═══════════════════════════════════════════════════════════════════════════════
# CAMADA 3: ANTI-GLOSA
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class AntiGlosaResult:
    """Resultado da análise anti-glosa."""
    risco: str = "baixo"  # baixo | moderado | alto | critico
    falhas: list = field(default_factory=list)
    correcoes: list = field(default_factory=list)
    detalhes: dict = field(default_factory=dict)


# Procedimentos que tipicamente requerem OPME
PROC_REQUER_OPME = [
    "artrodese", "fixação", "prótese", "implante",
    "estimulação cerebral", "endovascular", "stent",
    "clip", "derivação", "shunt",
]


def run_anti_glosa(n: NormalizedInput, req: DecideRequest) -> AntiGlosaResult:
    """
    CAMADA 3 — Anti-glosa.
    Analisa coerência OPME × procedimento, excesso/ausência de material,
    risco de auditoria.
    """
    r = AntiGlosaResult()
    risk_score = 0  # 0-100, depois mapeia para categorias

    # ── CHECK 1: OPME × Procedimento (C001) ──────────────────────
    proc_lower = n.procedimento.lower()
    proc_tipicamente_opme = any(p in proc_lower for p in PROC_REQUER_OPME)

    if proc_tipicamente_opme and not n.necessita_opme:
        r.falhas.append(
            f"[C001] Procedimento '{n.procedimento}' tipicamente requer OPME, "
            "mas flag necessita_opme está como 'Não'. Verificar ou declarar OPME."
        )
        r.correcoes.append("Marcar necessita_opme='Sim' e declarar itens OPME")
        risk_score += 20

    # ── CHECK 2: OPME declarado mas vazio (C002) ─────────────────
    if n.necessita_opme and n.opme_items_count == 0:
        r.falhas.append(
            "[C002] OPME marcado como necessário mas nenhum item declarado. "
            "Inconsistência documental."
        )
        r.correcoes.append("Declarar itens OPME com descrição, quantidade e fabricante")
        risk_score += 25

    # ── CHECK 3: OPME incompleto (C003) ──────────────────────────
    if n.necessita_opme and n.opme_items_count > 0 and not n.opme_items_completos:
        r.falhas.append(
            "[C003] Itens OPME com descrição ou quantidade incompletas."
        )
        r.correcoes.append("Completar descrição e quantidade de todos os itens OPME")
        risk_score += 15

    # ── CHECK 4: Quantidade OPME vs procedimento (C004) ──────────
    if n.opme_items_count > 10:
        r.falhas.append(
            f"[C004] Quantidade de itens OPME ({n.opme_items_count}) acima do padrão. "
            "Risco de auditoria por excesso."
        )
        r.correcoes.append("Revisar necessidade de cada item OPME declarado")
        risk_score += 15

    # ── CHECK 5: OPME genérico (C005) ────────────────────────────
    opme_generico = False
    if req.opme_items:
        for item in req.opme_items:
            desc = (item.descricao or "").lower()
            if any(g in desc for g in ["kit", "genérico", "generico", "material cirúrgico", "material cirurgico"]):
                opme_generico = True
                r.falhas.append(
                    f"[C005] Item OPME genérico: '{item.descricao}'. "
                    "Auditoria rejeita descrições vagas."
                )
                r.correcoes.append(f"Substituir '{item.descricao}' por descrição técnica específica")
                risk_score += 20
                break

    r.detalhes["opme_generico"] = opme_generico

    # ── CHECK 6: Justificativa OPME ausente (C006) ───────────────
    has_justificativas = bool(req.justificativas_opme)
    if n.necessita_opme and n.opme_items_count > 0 and not has_justificativas:
        r.falhas.append(
            "[C006] Justificativas clínicas para OPME ausentes. "
            "Convênios auditam sistematicamente OPME sem justificativa."
        )
        r.correcoes.append("Adicionar justificativa clínica para cada item OPME")
        risk_score += 10

    # ── CHECK 7: CRM ausente com OPME (C007) ─────────────────────
    if n.necessita_opme and not n.crm:
        r.falhas.append(
            "[C007] OPME declarado sem CRM do solicitante. "
            "Guia rejeitada automaticamente."
        )
        r.correcoes.append("Informar CRM do médico solicitante")
        risk_score += 15

    # ── CHECK 8: Achados ausentes em procedimento maior (C008) ───
    procedimento_maior = any(p in proc_lower for p in [
        "artrodese", "craniotomia", "laminectomia", "clipagem",
        "ressecção", "exerese", "implante",
    ])
    if procedimento_maior and not n.achados_resumo:
        r.falhas.append(
            "[C008] Procedimento de grande porte sem achados de imagem documentados. "
            "Principal causa de negativa em cirurgia eletiva."
        )
        r.correcoes.append("Anexar laudos de RM/TC com achados relevantes")
        risk_score += 20

    # ── RISCO FINAL ──────────────────────────────────────────────
    r.detalhes["risk_score_raw"] = risk_score
    if risk_score >= 60:
        r.risco = "critico"
    elif risk_score >= 40:
        r.risco = "alto"
    elif risk_score >= 20:
        r.risco = "moderado"
    else:
        r.risco = "baixo"

    logger.info(
        f"[engine_v3] CAMADA3 risco={r.risco} "
        f"risk_score={risk_score} falhas={len(r.falhas)} "
        f"correcoes={len(r.correcoes)}"
    )
    return r


# ═══════════════════════════════════════════════════════════════════════════════
# GATE BINÁRIO + OUTPUT FINAL
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class EngineOutput:
    """Output estruturado obrigatório do motor v1.3."""
    decision: str = "NO_GO"         # GO | GO_COM_RESSALVAS | NO_GO
    decision_status: str = "NEGADO"  # APROVADO | AUTORIZADO_COM_RESSALVAS | NEGADO
    risco_glosa: str = "alto"
    falhas: list = field(default_factory=list)
    correcoes: list = field(default_factory=list)
    justificativa_final: str = ""
    score_clinico: float = 0.0
    score_100: int = 0              # score 0-100 para retrocompat
    pendencias: list = field(default_factory=list)
    bloqueios: list = field(default_factory=list)
    pontos_frageis: list = field(default_factory=list)
    # Metadados
    versao_motor: str = ENGINE_VERSION
    tempo_execucao_ms: int = 0
    camada1_resultado: str = ""
    camada2_score: float = 0.0
    camada3_risco: str = ""
    gate_reason: str = ""


JUSTIFICATIVA_BASE = (
    "Paciente portador de {cid}, com quadro de {indicacao}. "
    "Tratamento conservador realizado por {semanas}. "
    "Achados de imagem: {achados}. "
    "Indicação de {procedimento} em conformidade com protocolo CBHPM e "
    "critérios ANS de cobertura obrigatória. "
    "Material OPME tecnicamente necessário para estabilização e fusão "
    "intersomática conforme diretrizes SBN."
)


def run_engine(req: DecideRequest) -> EngineOutput:
    """
    Pipeline principal v1.3 — 3 camadas + gate binário.
    Determinístico, auditável, defensável.
    """
    t0 = time.monotonic()
    output = EngineOutput()

    # ── NORMALIZE ────────────────────────────────────────────────
    n = normalize_input(req)

    # ── CAMADA 1: ESTRUTURAL ─────────────────────────────────────
    c1 = run_structural_validation(n)
    output.camada1_resultado = "PASS" if c1.passed else "FAIL"
    output.bloqueios = list(c1.falhas)

    # ── CAMADA 2: CLÍNICA ────────────────────────────────────────
    c2 = run_clinical_validation(n)
    output.camada2_score = c2.score
    output.score_clinico = c2.score
    output.score_100 = int(round(c2.score * 100))
    output.pendencias = list(c2.pendencias)
    output.pontos_frageis = list(c2.pontos_frageis)

    # ── CAMADA 3: ANTI-GLOSA ─────────────────────────────────────
    c3 = run_anti_glosa(n, req)
    output.camada3_risco = c3.risco
    output.falhas = list(c3.falhas)
    output.correcoes = list(c3.correcoes)

    # Merge pontos_frageis da anti-glosa
    for f in c3.falhas:
        if f not in output.pontos_frageis:
            output.pontos_frageis.append(f)

    # ── GATE BINÁRIO ─────────────────────────────────────────────
    if not c1.passed:
        output.decision = "NO_GO"
        output.decision_status = "NEGADO"
        output.risco_glosa = "critico"
        output.gate_reason = "ESTRUTURAL_FAIL"
    elif c3.risco in ("alto", "critico"):
        output.decision = "NO_GO"
        output.decision_status = "NEGADO"
        output.risco_glosa = c3.risco
        output.gate_reason = f"ANTI_GLOSA_{c3.risco.upper()}"
    elif c2.score < 0.6:
        output.decision = "GO_COM_RESSALVAS"
        output.decision_status = "AUTORIZADO_COM_RESSALVAS"
        output.risco_glosa = "moderado" if c3.risco == "baixo" else c3.risco
        output.gate_reason = f"SCORE_CLINICO_BAIXO ({c2.score:.3f} < 0.6)"
    else:
        output.decision = "GO"
        output.decision_status = "APROVADO"
        output.risco_glosa = c3.risco if c3.risco != "baixo" else "baixo"
        output.gate_reason = f"APROVADO (score={c2.score:.3f}, risco={c3.risco})"

    # Add structural warnings to pendencias
    for aviso in c1.avisos:
        output.pendencias.append(aviso)

    # ── JUSTIFICATIVA ────────────────────────────────────────────
    output.justificativa_final = JUSTIFICATIVA_BASE.format(
        cid=n.cid_principal or "não informado",
        indicacao=n.indicacao_clinica[:120] if n.indicacao_clinica else "não informada",
        semanas=(
            f"{n.semanas_conservador} semanas"
            if n.semanas_conservador > 0
            else "período documentado"
        ),
        achados=n.achados_resumo[:120] if n.achados_resumo else "conforme laudo em anexo",
        procedimento=n.procedimento or "não informado",
    )

    # ── TEMPO ────────────────────────────────────────────────────
    output.tempo_execucao_ms = int((time.monotonic() - t0) * 1000)

    logger.info(
        f"[engine_v3] GATE decision={output.decision} "
        f"reason={output.gate_reason} score={output.score_clinico:.3f} "
        f"risco={output.risco_glosa} tempo={output.tempo_execucao_ms}ms"
    )
    return output


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _extrair_semanas(tto_str: str) -> int:
    if not tto_str:
        return 0
    # Try "X semanas" or "X sem" first
    m = re.search(r"(\d+)\s*(?:semana|sem)", tto_str.lower())
    if m:
        return int(m.group(1))
    # Try "X meses" → convert
    m = re.search(r"(\d+)\s*(?:mes|mês|meses)", tto_str.lower())
    if m:
        return int(m.group(1)) * 4
    # Fallback: first number found (could be days → convert to weeks)
    m = re.search(r"(\d+)", tto_str)
    if m:
        val = int(m.group(1))
        # If large number, likely days
        if val > 52:
            return val // 7
        return val
    return 0


def _detectar_deficit_motor(texto: str) -> bool:
    """Detecta déficit motor com proteção contra negações."""
    if not texto:
        return False
    t = texto.lower()

    SINAIS_POSITIVOS = [
        "déficit motor", "deficit motor",
        "força grau", "paresia", "plegia",
        "queda de força", "fraqueza muscular",
        "deficit neurológico motor", "déficit neurológico motor",
        "progressivo em", "déficit progressivo", "deficit progressivo",
    ]
    NEGACOES = [
        "sem déficit motor", "sem deficit motor",
        "ausência de déficit", "ausencia de deficit",
        "nega déficit", "nega deficit",
        "sem paresia", "força preservada",
        "força normal", "sem déficit neurológico",
        "sem deficit neurologico", "não apresenta déficit",
        "nao apresenta deficit", "sem déficit", "sem deficit",
    ]

    tem_positivo = any(s in t for s in SINAIS_POSITIVOS)
    tem_negacao = any(n in t for n in NEGACOES)

    if tem_positivo and not tem_negacao:
        return True
    return False


def _get_semanas_minimas(convenio_lower: str) -> int:
    for k, v in CONVENIO_SEMANAS.items():
        if k in convenio_lower:
            return v
    return SEMANAS_DEFAULT
