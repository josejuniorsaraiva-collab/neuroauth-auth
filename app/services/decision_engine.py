"""
app/services/decision_engine.py
Motor de decisão v2 — com INPUT HARDENING integrado.
Score 0–100, 4 blocos, classificação GO/GO_COM_RESSALVAS/NO_GO/PRE_ANALISE_APENAS.
"""

from app.models.decide import DecideRequest, DecideResponse
from app.services.input_hardening import run_hardening
from datetime import datetime
import uuid
import re
import logging

logger = logging.getLogger("neuroauth.decide")

SCORE_THRESHOLDS = {"GO": 75, "GO_COM_RESSALVAS": 50}
SEMANAS_CONSERVADOR_DEFAULT = 6
SEMANAS_CONSERVADOR_SULAMERICA = 8

CONVENIO_SEMANAS = {
    "sulamérica": 8, "sulamerica": 8, "sul america": 8,
    "bradesco": 8,
    "unimed": 6, "amil": 6, "hapvida": 6,
}

JUSTIFICATIVA_BASE = (
    "Paciente portador de {cid}, com quadro de {indicacao}. "
    "Tratamento conservador realizado por {semanas}. "
    "Achados de imagem: {achados}. "
    "Indicação de {procedimento} em conformidade com protocolo CBHPM e "
    "critérios ANS de cobertura obrigatória. "
    "Material OPME tecnicamente necessário para estabilização e fusão "
    "intersomática conforme diretrizes SBN."
)



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

def run_decision(req: DecideRequest) -> DecideResponse:

    # ── FASE 1: INPUT HARDENING ─────────────────────────────────────────────
    h = run_hardening(req)

    # Aplicar normalização TUSS ao request (não muta o original, apenas usa)
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
                "Convênio ausente: não é possível aplicar regras regulatórias, "
                "verificar cobertura ou emitir parecer anti-glosa definitivo. "
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

    # ── FASE 2: MOTOR DECISÓRIO (convênio presente) ─────────────────────────
    score = 0
    pendencias: list[str] = list(h.pendencias)  # herda pendências do hardening
    pontos_frageis: list[str] = []

    # BLOCO 1 — Completude (40 pts)
    if req.cid_principal and len(req.cid_principal) >= 4:
        score += 10
    else:
        pendencias.append("CID principal ausente ou incompleto.")
        pontos_frageis.append("CID incompleto — glosa automática no TISS.")

    if req.indicacao_clinica and len(req.indicacao_clinica) > 30:
        score += 10
    else:
        pendencias.append("Indicação clínica insuficiente (mínimo 30 caracteres).")
        pontos_frageis.append("Indicação genérica — auditoria rejeita sem especificidade.")

    if req.achados_resumo and len(req.achados_resumo) > 20:
        score += 10
    else:
        pendencias.append("Achados de imagem ausentes ou insuficientes.")
        pontos_frageis.append("Sem achados objetivos — fragilidade crítica.")

    if req.crm and req.cbo:
        score += 10
    else:
        pendencias.append("CRM e/ou CBO do solicitante ausentes.")
        pontos_frageis.append("Guia sem CRM/CBO rejeitada por auditoria eletrônica.")

    # BLOCO 2 — Tratamento conservador (20 pts)
    conv_lower = req.convenio.lower()
    semanas_minimas = next(
        (v for k, v in CONVENIO_SEMANAS.items() if k in conv_lower),
        SEMANAS_CONSERVADOR_DEFAULT
    )
    semanas = _extrair_semanas(req.tto_conservador)
    tem_deficit_motor = _detectar_deficit_motor(req.indicacao_clinica)

    if semanas >= semanas_minimas:
        score += 20
    elif semanas > 0:
        if tem_deficit_motor:
            score += 15  # urgência relativa mitiga penalidade
            pendencias.append(
                f"Conservador ({semanas} sem.) abaixo do mínimo {req.convenio} "
                f"({semanas_minimas} sem.) — déficit motor justifica urgência relativa, "
                "mas documentar exceção explicitamente."
            )
        else:
            score += 10
            pendencias.append(
                f"Conservador ({semanas} sem.) abaixo do mínimo {req.convenio} "
                f"({semanas_minimas} sem.). Documentar exceção ou complementar."
            )
            pontos_frageis.append("Conservador insuficiente — principal causa de glosa em coluna.")
    else:
        if not tem_deficit_motor:
            pendencias.append(
                f"Tratamento conservador não documentado. "
                f"{req.convenio} exige {semanas_minimas} sem. ou justificativa de urgência."
            )
            pontos_frageis.append("Ausência de tto conservador — negativa provável.")

    # BLOCO 3 — OPME (20 pts)
    if req.necessita_opme == "Sim":
        if req.opme_items and not h.opme_generico_bloqueado and not h.opme_incompativel:
            completos = all(item.descricao and item.qtd > 0 for item in req.opme_items)
            score += 20 if completos else 8
            if not completos:
                pendencias.append("Itens OPME com descrição ou quantidade incompletas.")
                pontos_frageis.append("OPME incompleto — glosa na fatura hospitalar.")
        elif h.opme_generico_bloqueado or h.opme_incompativel:
            score += 0
            score = min(score, 74)  # forçar GO_COM_RESSALVAS — OPME problemático nunca libera GO
            pontos_frageis.append("OPME genérico ou incompatível — alto risco de glosa.")
        else:
            pendencias.append("OPME marcado como necessário mas nenhum item informado.")
            pontos_frageis.append("OPME vazio com flag ativa — inconsistência documental.")
    else:
        score += 20

    # BLOCO 4 — Convênio específico (20 pts)
    if "unimed" in conv_lower:
        score += 20
    elif any(k in conv_lower for k in ["bradesco", "sulamérica", "sulamerica", "amil", "hapvida"]):
        score += 15
        pendencias.append(f"Convênio '{req.convenio}' — regras específicas aplicadas. Verificar SADT.")
    else:
        score += 10
        pendencias.append(f"Convênio '{req.convenio}' — verificar regras e cobertura específicas.")

    # CAP 1: OPME problemático nunca libera GO
    if h.opme_generico_bloqueado or h.opme_incompativel:
        score = min(score, 74)

    # CAP 2: Conservador insuficiente SEM déficit motor nunca libera GO
    # Convênio rigoroso (SulAmérica/Bradesco 8 sem.) com < mínimo e sem déficit motor
    # é negativa certa — cap em GO_COM_RESSALVAS
    _semanas_check = _extrair_semanas(req.tto_conservador)
    _minimo_check  = next((v for k, v in CONVENIO_SEMANAS.items() if k in req.convenio.lower()), SEMANAS_CONSERVADOR_DEFAULT)
    _deficit_check = _detectar_deficit_motor(req.indicacao_clinica)
    if _semanas_check < _minimo_check and not _deficit_check and _semanas_check > 0:
        score = min(score, 74)
        pontos_frageis.append(
            f"Conservador insuficiente ({_semanas_check} sem.) sem déficit motor — "
            f"{req.convenio} não autorizará. Cap de score aplicado."
        )

    # ── CLASSIFICAÇÃO ────────────────────────────────────────────────────────
    if score >= SCORE_THRESHOLDS["GO"]:
        classification, decision_status = "GO", "APROVADO"
    elif score >= SCORE_THRESHOLDS["GO_COM_RESSALVAS"]:
        classification, decision_status = "GO_COM_RESSALVAS", "PENDENTE"
    else:
        classification, decision_status = "NO_GO", "NEGADO"

    # Risco glosa
    n_criticos = sum(1 for p in pontos_frageis if "glosa" in p.lower() or "críti" in p.lower())
    if h.opme_generico_bloqueado or h.opme_incompativel:
        risco_glosa = "alto"
    elif n_criticos == 0 and score >= 75:
        risco_glosa = "baixo"
    elif n_criticos <= 2 and score >= 50:
        risco_glosa = "moderado"
    else:
        risco_glosa = "alto"

    justificativa = JUSTIFICATIVA_BASE.format(
        cid=req.cid_principal,
        indicacao=req.indicacao_clinica[:120],
        semanas=f"{semanas} semanas" if semanas > 0 else "período documentado",
        achados=req.achados_resumo[:120] if req.achados_resumo else "conforme laudo em anexo",
        procedimento=req.procedimento,
    )

    logger.info(
        f"[decide] score={score} cls={classification} "
        f"pendencias={len(pendencias)} tuss={tuss_efetivo}"
    )

    return DecideResponse(
        decision_run_id=f"DR-{str(uuid.uuid4())[:8].upper()}",
        episodio_id=req.episodio_id,
        classification=classification,
        decision_status=decision_status,
        score=score,
        justificativa=justificativa,
        pendencias=pendencias,
        bloqueios=h.bloqueios,
        risco_glosa=risco_glosa,
        pontos_frageis=pontos_frageis,
        proximos_passos=[],
        tuss_normalizado=tuss_efetivo,
        timestamp=datetime.utcnow().isoformat(),
    )


def _extrair_semanas(tto_str: str | None) -> int:
    if not tto_str:
        return 0
    m = re.search(r"(\d+)", tto_str)
    return int(m.group(1)) if m else 0
