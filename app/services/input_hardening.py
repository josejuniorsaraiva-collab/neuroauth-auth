"""
app/services/input_hardening.py
NEUROAUTH — INPUT HARDENING PATCH v1.0

Camada de validação e normalização que roda ANTES do motor decisório.
Não altera a lógica de score existente — apenas gate/bloqueia/normaliza.

Gates implementados:
  1. Convênio ausente → PRE_ANALISE_APENAS
  2. OPME genérico ("kit") → pendência de detalhamento
  3. TUSS sem hífen → normalização automática
  4. Conservador incompleto → pendência documental
  5. Checklist defensivo clínico → pendências por perfil
  6. OPME incompatível com perfil → inconsistência flagada
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from app.models.decide import DecideRequest, OpmeItem
from app.services.opme_validator import validate_opme_items, apply_opme_caps, OpmePendencia

logger = logging.getLogger("neuroauth.input_hardening")

# ── CONSTANTES ─────────────────────────────────────────────────────────────

TUSS_ALIAS = {
    "3.07.15.180": "3.07.15.18-0",
    "30715180":    "3.07.15.18-0",
    "3071518":     "3.07.15.18-0",
    "3.07.14.025": "3.07.14.02-5",
    "30714025":    "3.07.14.02-5",
    "3.07.13.021": "3.07.13.02-1",
    "30713021":    "3.07.13.02-1",
    "3.07.15.020": "3.07.15.02-0",
}

# OPME_INCOMPATIVEIS — movida para opme_validator.py (PROCEDURE_RULES)

PERFIS_COLUNA_ELETIVA = [
    "microdiscectomia", "artrodese", "laminectomia",
    "discectomia", "foraminotomia", "hemilaminectomia"
]

# ── RESULTADO DO HARDENING ──────────────────────────────────────────────────

@dataclass
class HardeningResult:
    bloqueio_convenio: bool = False
    pre_analise_apenas: bool = False
    pendencias: list = field(default_factory=list)
    bloqueios: list = field(default_factory=list)
    tuss_original: Optional[str] = None
    tuss_normalizado: Optional[str] = None
    tuss_pendencia: bool = False
    opme_generico_bloqueado: bool = False
    opme_incompativel: bool = False
    conservador_incompleto: bool = False
    checklist_defensivo: dict = field(default_factory=dict)
    logs: list = field(default_factory=list)
    _opme_validation: object = None  # OpmeValidationResult — exposto para apply_opme_caps

    def logar(self, msg: str):
        self.logs.append(msg)
        logger.info(msg)


# ── FUNÇÃO PRINCIPAL ────────────────────────────────────────────────────────


def _detectar_deficit_motor(texto: str) -> bool:
    """
    Detecta déficit motor com proteção contra negações.
    Retorna True apenas se houver sinal positivo sem negação dominante.
    Princípio: em caso de ambiguidade, adotar interpretação conservadora.
    """
    t = texto.lower()

    SINAIS_POSITIVOS = [
        "déficit motor", "deficit motor",
        "força grau", "paresia", "plegia",
        "queda de força", "fraqueza muscular",
        "deficit neurológico motor", "déficit neurológico motor",
    ]
    NEGACOES = [
        "sem déficit motor", "sem deficit motor",
        "ausência de déficit", "ausencia de deficit",
        "nega déficit", "nega deficit",
        "sem paresia", "força preservada",
        "força normal", "sem déficit neurológico",
        "sem deficit neurologico", "não apresenta déficit",
        "nao apresenta deficit", "sem déficit",
        "sem deficit",
    ]

    tem_positivo = any(s in t for s in SINAIS_POSITIVOS)
    tem_negacao  = any(n in t for n in NEGACOES)

    if tem_positivo and not tem_negacao:
        return True
    if tem_positivo and tem_negacao:
        # Ambiguidade — interpretação conservadora: não usar como fator favorável
        return False
    return False

def run_hardening(req: DecideRequest) -> HardeningResult:
    r = HardeningResult()
    ep = getattr(req, "episodio_id", "SEM_EP")
    proc = req.procedimento.lower()
    conv = req.convenio.strip() if req.convenio else ""

    r.logar(f"INPUT_HARDENING_START ep={ep} proc={req.procedimento} convenio='{conv}'")

    # GATE 1 — Convênio
    _gate_convenio(req, r, ep)

    # GATE 2 — OPME
    _gate_opme(req, r, ep, proc)

    # GATE 3 — TUSS
    _gate_tuss(req, r, ep)

    # GATE 4 — Conservador
    _gate_conservador(req, r, ep, proc)

    # GATE 5 — Checklist defensivo
    _gate_checklist_defensivo(req, r, ep, proc)

    # Decisão final do gate
    if r.bloqueio_convenio:
        r.logar(f"PRE_ANALYSIS_ONLY ep={ep} motivo=convenio_ausente")
    else:
        r.logar(f"DECISION_GATE_RELEASED ep={ep} pendencias={len(r.pendencias)}")

    return r


# ── GATES INTERNOS ──────────────────────────────────────────────────────────

def _gate_convenio(req: DecideRequest, r: HardeningResult, ep: str):
    conv = req.convenio.strip() if req.convenio else ""
    placeholders = {"", "string", "convenio", "convênio", "n/a", "na", "nenhum", "outro"}

    if not conv or conv.lower() in placeholders:
        r.bloqueio_convenio = True
        r.pre_analise_apenas = True
        r.bloqueios.append(
            "CONVÊNIO AUSENTE — Convênio obrigatório para análise regulatória e anti-glosa. "
            "Sistema não pode emitir decisão final sem convênio identificado."
        )
        r.logar(f"CONVENIO_MISSING_BLOCK ep={ep}")
    else:
        r.logar(f"CONVENIO_OK ep={ep} convenio='{conv}'")


def _gate_opme(req: DecideRequest, r: HardeningResult, ep: str, proc: str):
    """
    Delega validação de OPME ao opme_validator.py (v2.0).
    Pendências de incompatibilidade têm prioridade sobre genérico (ordenadas por severidade).
    Dreno sem fabricante ainda verificado aqui como pendência menor.
    """
    if req.necessita_opme != "Sim" or not req.opme_items:
        return

    # ── Delegação ao validador externo ────────────────────────────────────
    justificativas = getattr(req, 'justificativas_opme', None) or {}
    val = validate_opme_items(req.procedimento, req.opme_items, justificativas)

    # Transferir flags + guardar resultado completo
    r.opme_generico_bloqueado = val.opme_generico_detectado
    r.opme_incompativel       = val.opme_incompativel_detectado
    r._opme_validation        = val

    # Transferir pendências (já ordenadas por severidade: critica > alta)
    for p in val.pendencias:
        r.pendencias.append(p.mensagem)

    # Transferir logs
    for log in val.logs:
        r.logar(f"{log} ep={ep}")

    # Dreno: verificação movida para opme_validator (opcionais_com_justificativa)
    # Evitar duplicidade de pendências


def _gate_tuss(req: DecideRequest, r: HardeningResult, ep: str):
    tuss = req.cod_cbhpm.strip() if req.cod_cbhpm else ""
    if not tuss:
        return

    r.tuss_original = tuss

    # Já está no formato correto com hífen
    if re.match(r"^\d\.\d{2}\.\d{2}\.\d{2}-\d$", tuss):
        r.tuss_normalizado = tuss
        r.logar(f"TUSS_OK ep={ep} tuss='{tuss}'")
        return

    # Tentar alias direto
    if tuss in TUSS_ALIAS:
        r.tuss_normalizado = TUSS_ALIAS[tuss]
        r.logar(f"TUSS_NORMALIZED ep={ep} original='{tuss}' normalizado='{r.tuss_normalizado}'")
        return

    # Tentar normalização automática: inserir hífen antes do último dígito
    # Ex: 3.07.15.18 0 → 3.07.15.18-0 | 3.07.15.180 → 3.07.15.18-0
    cleaned = tuss.replace(" ", "")
    m = re.match(r"^(\d\.\d{2}\.\d{2}\.\d{2})(\d)$", cleaned)
    if m:
        r.tuss_normalizado = f"{m.group(1)}-{m.group(2)}"
        r.logar(f"TUSS_NORMALIZED ep={ep} original='{tuss}' normalizado='{r.tuss_normalizado}'")
        return

    # Não conseguiu normalizar
    r.tuss_pendencia = True
    r.pendencias.append(
        f"Código TUSS '{tuss}' em formato não reconhecido. "
        "Verificar e corrigir antes da submissão (formato esperado: X.XX.XX.XX-X)."
    )
    r.logar(f"TUSS_PENDENCIA ep={ep} tuss='{tuss}'")


def _gate_conservador(req: DecideRequest, r: HardeningResult, ep: str, proc: str):
    eh_coluna_eletiva = any(p in proc for p in PERFIS_COLUNA_ELETIVA)
    if not eh_coluna_eletiva:
        return

    tto = req.tto_conservador or ""
    tem_deficit_motor = _detectar_deficit_motor(req.indicacao_clinica)

    if not tto or len(tto.strip()) < 10:
        if tem_deficit_motor:
            r.pendencias.append(
                "Tratamento conservador não documentado. "
                "Déficit motor presente permite urgência relativa, mas exige registro "
                "da indicação de urgência no laudo para evitar negativa."
            )
        else:
            r.pendencias.append(
                "Tratamento conservador ausente ou insuficiente. "
                "Para coluna eletiva, documentar: fisioterapia (sessões), "
                "medicação (classe + duração), infiltração se realizada."
            )
        r.conservador_incompleto = True
        r.logar(f"CONSERVADOR_INCOMPLETE ep={ep} deficit_motor={tem_deficit_motor}")
        return

    # Verificar se modalidades estão nominadas
    modalidades = ["fisioterap", "analgesi", "infiltraç", "infiltracao",
                   "anti-inflamat", "opioide", "bloqueio"]
    tem_modalidade = any(m in tto.lower() for m in modalidades)

    if not tem_modalidade:
        r.conservador_incompleto = True
        r.pendencias.append(
            "Tratamento conservador informado mas sem modalidades nominadas. "
            "Especificar: fisioterapia (número de sessões), medicação (classe + duração), "
            "infiltração (se realizada). Documentação incompleta gera pendência em auditoria."
        )
        r.logar(f"CONSERVADOR_INCOMPLETE ep={ep} motivo=sem_modalidades")


# ── PERFIS DE CHECKLIST POR REGIÃO ANATÔMICA ───────────────────────────────
# Nunca aplicar checklist de região errada — erro crítico de sistema

_PERFIS_LOMBAR = [
    "microdiscectomia", "discectomia lombar", "hérnia lombar",
    "hernia lombar", "laminectomia lombar", "artrodese lombar",
]
_PERFIS_CERVICAL = [
    "acdf", "artrodese cervical", "discectomia cervical",
    "hérnia cervical", "hernia cervical", "mielopatia cervical",
    "cirurgia cervical",
]


def _detectar_regiao_checklist(proc: str) -> str:
    """
    Retorna 'lombar', 'cervical' ou 'nenhum'.
    Cervical tem precedência — ACDF com discectomia não deve acionar lombar.
    """
    for p in _PERFIS_CERVICAL:
        if p in proc:
            return "cervical"
    # Lombar: apenas se não for cervical
    for p in _PERFIS_LOMBAR:
        if p in proc:
            return "lombar"
    return "nenhum"


def _gate_checklist_defensivo(req: DecideRequest, r: HardeningResult, ep: str, proc: str):
    """
    Roteia para o checklist correto com base na região anatômica.
    Nunca aplica checklist lombar em caso cervical.
    """
    regiao = _detectar_regiao_checklist(proc)

    if regiao == "lombar":
        _checklist_lombar(req, r, ep)
    elif regiao == "cervical":
        _checklist_cervical(req, r, ep)
    else:
        return  # perfil sem checklist específico — sem pendências lombares/cervicais


def _checklist_lombar(req: DecideRequest, r: HardeningResult, ep: str):
    """Checklist defensivo para coluna lombar (hérnia, microdiscectomia)."""
    indicacao = req.indicacao_clinica.lower()
    achados   = (req.achados_resumo or "").lower()
    texto     = indicacao + " " + achados

    checklist = {
        "lasegue_documentado":         any(t in texto for t in ["lasègue", "lasegue", "laségue"]),
        "deficit_motor_graduado":      _detectar_deficit_motor(req.indicacao_clinica + " " + (req.achados_resumo or "")),
        "dermatomero_correlacionado":  any(t in texto for t in ["l4", "l5", "s1", "dermatômero", "dermatomero", "radicular"]),
        "imagem_correlata":            any(t in texto for t in ["rm", "ressonância", "ressonancia", "tc", "tomografia", "mri"]),
        "compressao_radicular_descrita": any(t in texto for t in ["compressão", "compressao", "comprime", "compressivo"]),
    }
    r.checklist_defensivo = checklist

    ausentes = [k for k, v in checklist.items() if not v]
    criticos  = ["lasegue_documentado", "deficit_motor_graduado", "compressao_radicular_descrita"]

    LABELS = {
        "lasegue_documentado":          "Sinal de Lasègue",
        "deficit_motor_graduado":       "Déficit motor com graduação de força",
        "dermatomero_correlacionado":   "Correlação com dermátomo (L4/L5/S1)",
        "imagem_correlata":             "Referência a exame de imagem (RM/TC)",
        "compressao_radicular_descrita":"Compressão radicular descrita",
    }
    for item in ausentes:
        pfx = "⚠️ PENDÊNCIA CRÍTICA" if item in criticos else "Pendência documental"
        r.pendencias.append(
            f"{pfx}: {LABELS[item]} não identificado no texto. "
            "Incluir no laudo médico para fortalecer defesa anti-glosa."
        )

    r.logar(f"CHECKLIST_LOMBAR ep={ep} ausentes={ausentes if ausentes else 'nenhum'}")


def _checklist_cervical(req: DecideRequest, r: HardeningResult, ep: str):
    """
    Checklist defensivo para coluna cervical (ACDF, mielopatia).
    Itens específicos de cervical — nunca Lasègue, nunca dermátomo L4/L5/S1.
    """
    indicacao = req.indicacao_clinica.lower()
    achados   = (req.achados_resumo or "").lower()
    texto     = indicacao + " " + achados

    checklist = {
        "mielopatia_documentada":       any(t in texto for t in ["mielopatia", "myelopathy", "compressão medular", "compressao medular"]),
        "deficit_mmss_documentado":     any(t in texto for t in ["membro superior", "mmss", "ms ", "braço", "braco", "mão", "mao", "dedos"]) or
                                         _detectar_deficit_motor(req.indicacao_clinica + " " + (req.achados_resumo or "")),
        "hiperreflexia_ou_piramidal":   any(t in texto for t in ["hiperreflexia", "sinal piramidal", "babinski", "clônus", "clonus", "spasticidade", "espasticidade"]),
        "lhermitte_ou_sinal_cervical":  any(t in texto for t in ["lhermitte", "lhermite", "sinal cervical", "lermitte"]),
        "compressao_medular_descrita":  any(t in texto for t in ["compressão medular", "compressao medular", "sinal intramedular",
                                                                   "alteração de sinal", "alteracao de sinal", "myelopathy",
                                                                   "compressão da medula", "compressao da medula"]),
        "imagem_cervical_correlata":    any(t in texto for t in ["rm cervical", "ressonância cervical", "ressonancia cervical",
                                                                   "tc cervical", "tomografia cervical", "rm de cervical",
                                                                   "mri cervical", "rm", "ressonância", "ressonancia"]),
        "nivel_cervical_correlacionado": any(t in texto for t in ["c3", "c4", "c5", "c6", "c7", "c8",
                                                                    "c3-c4", "c4-c5", "c5-c6", "c6-c7", "c7-t1"]),
    }
    r.checklist_defensivo = checklist

    ausentes = [k for k, v in checklist.items() if not v]
    # Críticos cervicais: mielopatia + compressão medular são os pilares da indicação
    criticos = ["mielopatia_documentada", "compressao_medular_descrita"]

    LABELS = {
        "mielopatia_documentada":        "Mielopatia documentada (diagnóstico principal)",
        "deficit_mmss_documentado":      "Déficit em membros superiores ou graduação de força",
        "hiperreflexia_ou_piramidal":    "Hiperreflexia / sinais piramidais (Babinski, clônus)",
        "lhermitte_ou_sinal_cervical":   "Sinal de Lhermitte ou sinal cervical específico",
        "compressao_medular_descrita":   "Compressão medular descrita (RM com sinal intramedular)",
        "imagem_cervical_correlata":     "Referência a exame de imagem cervical (RM/TC)",
        "nivel_cervical_correlacionado": "Nível cervical correlacionado (C3-C7)",
    }
    for item in ausentes:
        pfx = "⚠️ PENDÊNCIA CRÍTICA" if item in criticos else "Pendência documental"
        r.pendencias.append(
            f"{pfx}: {LABELS[item]} não identificado no texto. "
            "Incluir no laudo para fortalecer indicação em convênio."
        )

    r.logar(f"CHECKLIST_CERVICAL ep={ep} ausentes={ausentes if ausentes else 'nenhum'}")
