"""
app/services/decision_engine.py
Motor de decisão v1 — ACDF / Unimed Cariri.
Score 0–100, 4 blocos, classificação GO/GO_COM_RESSALVAS/NO_GO.
"""

from app.models.decide import DecideRequest, DecideResponse
from datetime import datetime
import uuid
import re

SCORE_THRESHOLDS = {"GO": 75, "GO_COM_RESSALVAS": 50}
SEMANAS_CONSERVADOR_UNIMED = 6

JUSTIFICATIVA_BASE = (
    "Paciente portador de {cid}, com quadro de {indicacao}. "
    "Tratamento conservador realizado por {semanas}. "
    "Achados de imagem: {achados}. "
    "Indicação de {procedimento} em conformidade com protocolo CBHPM e "
    "critérios ANS de cobertura obrigatória. "
    "Material OPME tecnicamente necessário para estabilização e fusão "
    "intersomática conforme diretrizes SBN."
)


def run_decision(req: DecideRequest) -> DecideResponse:
    score = 0
    pendencias: list[str] = []
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
        pontos_frageis.append("Sem achados objetivos — fragilidade crítica para ACDF.")

    if req.crm and req.cbo:
        score += 10
    else:
        pendencias.append("CRM e/ou CBO do solicitante ausentes.")
        pontos_frageis.append("Guia sem CRM/CBO rejeitada por auditoria eletrônica.")

    # BLOCO 2 — Tratamento conservador (20 pts)
    semanas = _extrair_semanas(req.tto_conservador)
    if semanas >= SEMANAS_CONSERVADOR_UNIMED:
        score += 20
    elif semanas > 0:
        score += 10
        pendencias.append(
            f"Conservador ({semanas} sem.) abaixo do mínimo Unimed "
            f"({SEMANAS_CONSERVADOR_UNIMED} sem.). Documentar exceção."
        )
        pontos_frageis.append("Conservador insuficiente — principal causa de glosa em coluna.")
    else:
        pendencias.append(
            f"Tratamento conservador não documentado. "
            f"Unimed exige {SEMANAS_CONSERVADOR_UNIMED} semanas ou justificativa de urgência."
        )
        pontos_frageis.append("Ausência de tto conservador — negativa provável.")

    # BLOCO 3 — OPME (20 pts)
    if req.necessita_opme == "Sim":
        if req.opme_items:
            completos = all(
                item.descricao and item.qtd > 0 for item in req.opme_items
            )
            score += 20 if completos else 8
            if not completos:
                pendencias.append("Itens OPME com descrição ou quantidade incompletas.")
                pontos_frageis.append("OPME incompleto — glosa na fatura hospitalar.")
        else:
            pendencias.append("OPME marcado como necessário mas nenhum item informado.")
            pontos_frageis.append("OPME vazio com flag ativa — inconsistência documental.")
    else:
        score += 20

    # BLOCO 4 — Convênio (20 pts)
    if "unimed" in req.convenio.lower():
        score += 20
    else:
        score += 10
        pendencias.append(f"Convênio '{req.convenio}' — verificar regras específicas.")

    # Classificação
    if score >= SCORE_THRESHOLDS["GO"]:
        classification, decision_status = "GO", "APROVADO"
    elif score >= SCORE_THRESHOLDS["GO_COM_RESSALVAS"]:
        classification, decision_status = "GO_COM_RESSALVAS", "PENDENTE"
    else:
        classification, decision_status = "NO_GO", "NEGADO"

    # Risco glosa
    n_criticos = sum(1 for p in pontos_frageis if "glosa" in p.lower() or "críti" in p.lower())
    if n_criticos == 0 and score >= 75:
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

    return DecideResponse(
        decision_run_id=f"DR-{str(uuid.uuid4())[:8].upper()}",
        episodio_id=req.episodio_id,
        classification=classification,
        decision_status=decision_status,
        score=score,
        justificativa=justificativa,
        pendencias=pendencias,
        risco_glosa=risco_glosa,
        pontos_frageis=pontos_frageis,
        timestamp=datetime.utcnow().isoformat(),
    )


def _extrair_semanas(tto_str: str | None) -> int:
    if not tto_str:
        return 0
    m = re.search(r"(\d+)", tto_str)
    return int(m.group(1)) if m else 0
