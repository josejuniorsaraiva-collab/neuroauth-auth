"""
app/services/auth_package.py
NEUROAUTH — Authorization Package Generator v1.0

Recebe DecideRequest + DecideResponse e gera o pacote completo de autorização:
  1. Checklist pré-envio (pending_items → checklist interativo)
  2. Justificativa clínica (texto pronto para colar na guia SADT)
  3. Justificativa de OPME (por item, formato padrão)
  4. Bloco de cotações (estrutura exigida)
  5. Resumo executivo para o médico

Transforma "não envie assim" em "envie assim".
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Any

from app.models.decide import DecideRequest, DecideResponse

logger = logging.getLogger("neuroauth.auth_package")


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATES DE JUSTIFICATIVA CLÍNICA — por família de procedimento
# ══════════════════════════════════════════════════════════════════════════════

_TEMPLATES_JUSTIFICATIVA: dict[str, str] = {
    "acdf": (
        "Paciente com quadro de {quadro_clinico}, refratário ao tratamento conservador "
        "por {semanas_conservador} semanas, apresentando {deficit_descricao}.\n\n"
        "Exames de imagem ({achados_imagem}) evidenciam compressão radicular em nível "
        "{nivel_cirurgico}, correlacionando-se com a clínica apresentada. "
        "{enmg_bloco}\n\n"
        "Diante da falha do tratamento conservador, progressão clínica e "
        "{indicacao_cirurgica}, há indicação formal de descompressão cirúrgica "
        "via discectomia cervical anterior com artrodese (ACDF).\n\n"
        "Procedimento indicado conforme critérios clínicos e evidência científica vigente."
    ),
    "artrodese_lombar": (
        "Paciente com quadro de {quadro_clinico}, refratário ao tratamento conservador "
        "por {semanas_conservador} semanas, apresentando {deficit_descricao}.\n\n"
        "Exames de imagem ({achados_imagem}) confirmam comprometimento estrutural "
        "com indicação de estabilização cirúrgica. {enmg_bloco}\n\n"
        "Após esgotamento das opções conservadoras documentadas, há indicação "
        "formal de artrodese lombar para descompressão e estabilização segmentar.\n\n"
        "Procedimento indicado conforme critérios clínicos e evidência científica vigente."
    ),
    "generic": (
        "Paciente com quadro de {quadro_clinico}, refratário ao tratamento conservador "
        "por {semanas_conservador} semanas.\n\n"
        "Exames complementares ({achados_imagem}) confirmam a indicação cirúrgica. "
        "{enmg_bloco}\n\n"
        "Após esgotamento das opções conservadoras, há indicação formal do "
        "procedimento proposto ({procedimento}).\n\n"
        "Procedimento indicado conforme critérios clínicos e evidência científica vigente."
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATES DE JUSTIFICATIVA OPME
# ══════════════════════════════════════════════════════════════════════════════

_OPME_JUSTIFICATIVA_BASE = (
    "Material solicitado compatível com técnica cirúrgica proposta, "
    "necessário para {finalidade_tecnica}.\n\n"
    "Cada item foi selecionado conforme necessidade biomecânica específica "
    "do nível abordado, não sendo substituível por material genérico sem "
    "prejuízo técnico ao paciente.\n\n"
    "Solicitação baseada em critérios técnicos e segurança do paciente."
)

_OPME_POR_TIPO: dict[str, str] = {
    "cage": (
        "Dispositivo intersomático (cage) necessário para manutenção da altura "
        "discal, suporte de carga axial e promoção de fusão óssea no nível "
        "abordado. Material em PEEK com propriedades biomecânicas compatíveis "
        "com o módulo de elasticidade do osso cortical, reduzindo risco de subsidência."
    ),
    "placa": (
        "Placa de fixação anterior necessária para estabilização imediata do "
        "segmento operado, prevenindo migração do cage e mantendo alinhamento "
        "cervical durante o período de fusão. Fixação rígida conforme técnica "
        "padrão de ACDF."
    ),
    "parafuso": (
        "Parafusos de fixação cervical anterior necessários para ancoragem da "
        "placa aos corpos vertebrais adjacentes. Quantidade compatível com "
        "placa de nível único (2 parafusos por vértebra, total de 4)."
    ),
    "generic": (
        "Material necessário para execução segura da técnica cirúrgica proposta. "
        "Selecionado conforme indicação técnica específica do caso."
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
# DETECÇÃO DE FAMÍLIA DE PROCEDIMENTO
# ══════════════════════════════════════════════════════════════════════════════

def _detect_procedure_family(procedimento: str) -> str:
    proc = procedimento.lower()
    if any(k in proc for k in ("acdf", "cervical anterior", "discectomia cervical")):
        return "acdf"
    if any(k in proc for k in ("plif", "tlif", "lombar", "artrodese lombar")):
        return "artrodese_lombar"
    return "generic"


def _detect_opme_type(descricao: str) -> str:
    desc = descricao.lower()
    if "cage" in desc or "intersomático" in desc or "intersomatico" in desc:
        return "cage"
    if "placa" in desc:
        return "placa"
    if "parafuso" in desc:
        return "parafuso"
    return "generic"


# ══════════════════════════════════════════════════════════════════════════════
# EXTRAÇÃO DE CONTEXTO CLÍNICO
# ══════════════════════════════════════════════════════════════════════════════

def _extrair_semanas(texto: str) -> int:
    """Extrai número de semanas de tratamento conservador."""
    m = re.search(r"(\d+)\s*semanas?", texto, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*meses?", texto, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 4
    return 0


def _extrair_nivel(texto: str) -> str:
    """Extrai nível cirúrgico (ex: C5-C6, L4-L5)."""
    m = re.search(r"[CcLl]\d-[CcLl]?\d", texto)
    return m.group(0).upper() if m else "nível indicado"


def _tem_enmg(texto: str) -> bool:
    return bool(re.search(r"enmg|eletroneuromiografia|eletromiografia", texto, re.I))


def _extrair_quadro(indicacao: str) -> str:
    """Extrai quadro clínico resumido da indicação."""
    # Tenta pegar até a primeira vírgula ou ponto significativo
    indicacao_clean = indicacao.strip()
    # Se começa com "Paciente", pula essa parte
    if re.match(r"paciente\s", indicacao_clean, re.I):
        # Pula "Paciente masculino, 48 anos, "
        m = re.search(r"(?:anos?,?\s*)(.*)", indicacao_clean, re.I)
        if m:
            indicacao_clean = m.group(1).strip()

    # Pega a primeira frase significativa
    partes = re.split(r"[.;]", indicacao_clean)
    if partes:
        quadro = partes[0].strip().rstrip(",")
        # Capitaliza primeira letra
        if quadro:
            return quadro[0].lower() + quadro[1:] if len(quadro) > 1 else quadro.lower()
    return "quadro clínico descrito"


# ══════════════════════════════════════════════════════════════════════════════
# GERADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def generate_authorization_package(
    req: DecideRequest,
    res: DecideResponse,
) -> dict[str, Any]:
    """
    Gera pacote completo de autorização a partir do resultado do motor.

    Retorna:
    {
        "resumo_executivo": str,
        "gate": str,
        "score": int,
        "pronto_para_envio": bool,
        "checklist": [{"item": str, "resolvido": bool, "auto_gerado": bool}],
        "justificativa_clinica": str,
        "justificativa_opme_geral": str,
        "justificativas_opme_itens": [{"item": str, "justificativa": str}],
        "cotacoes": {"exigidas": int, "presentes": int, "fornecedores": []},
        "documentos_pendentes": [str],
        "texto_sadt_completo": str,
    }
    """
    family = _detect_procedure_family(req.procedimento)
    semanas = _extrair_semanas(req.tto_conservador or "")
    nivel = _extrair_nivel(req.procedimento + " " + (req.achados_resumo or ""))
    tem_enmg = _tem_enmg(req.achados_resumo or "")
    quadro = _extrair_quadro(req.indicacao_clinica)

    # ── 1. Checklist pré-envio ──────────────────────────────────────────────
    checklist = _build_checklist(req, res)

    # ── 2. Justificativa clínica ────────────────────────────────────────────
    template = _TEMPLATES_JUSTIFICATIVA.get(family, _TEMPLATES_JUSTIFICATIVA["generic"])

    # Detectar descrição de déficit
    deficit_descricao = "sinais clínicos objetivos documentados"
    indicacao_lower = (req.indicacao_clinica or "").lower()
    if "deficit" in indicacao_lower or "déficit" in indicacao_lower:
        deficit_descricao = "déficit motor documentado em território compatível"
    elif "dor" in indicacao_lower:
        deficit_descricao = "dor refratária com impacto funcional"

    # ENMG bloco
    enmg_bloco = ""
    if tem_enmg:
        enmg_bloco = "ENMG confirma sofrimento radicular ativo, reforçando a correlação clínico-radiológica."

    # Indicação cirúrgica
    indicacao_cirurgica = "déficit neurológico" if "deficit" in indicacao_lower or "déficit" in indicacao_lower else "progressão clínica documentada"

    justificativa_clinica = template.format(
        quadro_clinico=quadro,
        semanas_conservador=semanas if semanas > 0 else "múltiplas",
        deficit_descricao=deficit_descricao,
        achados_imagem=_resumir_achados(req.achados_resumo or ""),
        nivel_cirurgico=nivel,
        enmg_bloco=enmg_bloco,
        indicacao_cirurgica=indicacao_cirurgica,
        procedimento=req.procedimento,
    )

    # ── 3. Justificativa de OPME ────────────────────────────────────────────
    finalidade = "estabilização segmentar e manutenção da descompressão obtida"
    if family == "artrodese_lombar":
        finalidade = "estabilização e fusão do segmento lombar comprometido"

    justificativa_opme_geral = _OPME_JUSTIFICATIVA_BASE.format(
        finalidade_tecnica=finalidade,
    )

    justificativas_itens = []
    for item in (req.opme_items or []):
        tipo = _detect_opme_type(item.descricao)
        just_item = _OPME_POR_TIPO.get(tipo, _OPME_POR_TIPO["generic"])
        justificativas_itens.append({
            "item": item.descricao,
            "qtd": item.qtd,
            "fabricante": item.fabricante or "",
            "justificativa": just_item,
        })

    # ── 4. Cotações ─────────────────────────────────────────────────────────
    cotacoes_exigidas = 3  # padrão Unimed
    cotacoes = {
        "exigidas": cotacoes_exigidas,
        "presentes": 0,
        "fornecedores": [],
        "alerta": f"Obrigatório: {cotacoes_exigidas} cotações de fornecedores diferentes.",
    }

    # ── 5. Docs pendentes ───────────────────────────────────────────────────
    docs_pendentes = _extract_doc_requirements(res)

    # ── 6. Resumo executivo ─────────────────────────────────────────────────
    pronto = res.classification == "GO" and len(docs_pendentes) == 0
    checklist_pendente = sum(1 for c in checklist if not c["resolvido"])

    if pronto:
        resumo = (
            f"✅ AUTORIZADO — Score {res.score}/100. "
            "Documentação completa. Pronto para envio da guia."
        )
    elif res.classification == "NO_GO":
        resumo = (
            f"🚫 NEGADO — Score {res.score}/100. "
            f"{len(res.bloqueios)} bloqueio(s) detectado(s). "
            "Corrigir antes de resubmeter."
        )
    else:
        resumo = (
            f"⚠️ AUTORIZADO COM RESSALVAS — Score {res.score}/100. "
            f"{checklist_pendente} pendência(s) a resolver antes do envio. "
            "Justificativas clínicas e de OPME foram geradas automaticamente."
        )

    # ── 7. Texto SADT completo ──────────────────────────────────────────────
    texto_sadt = _build_texto_sadt(
        justificativa_clinica, justificativa_opme_geral,
        justificativas_itens, req,
    )

    return {
        "resumo_executivo": resumo,
        "gate": res.classification,
        "score": res.score,
        "decision_run_id": res.decision_run_id,
        "episodio_id": res.episodio_id,
        "motor_version": res.motor_version,
        "pronto_para_envio": pronto,
        "checklist": checklist,
        "checklist_pendente": checklist_pendente,
        "justificativa_clinica": justificativa_clinica,
        "justificativa_opme_geral": justificativa_opme_geral,
        "justificativas_opme_itens": justificativas_itens,
        "cotacoes": cotacoes,
        "documentos_pendentes": docs_pendentes,
        "texto_sadt_completo": texto_sadt,
    }


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _resumir_achados(achados: str) -> str:
    """Resume achados para caber no template."""
    if not achados:
        return "exames complementares"
    # Pega primeira sentença relevante
    partes = re.split(r"[.;]", achados)
    if partes:
        resumo = partes[0].strip()
        if len(resumo) > 100:
            resumo = resumo[:97] + "..."
        return resumo
    return achados[:100]


def _build_checklist(req: DecideRequest, res: DecideResponse) -> list[dict]:
    """Constrói checklist pré-envio a partir das pendências do motor."""
    checklist = []

    # Itens sempre obrigatórios
    checklist.append({
        "item": "Indicação clínica detalhada",
        "resolvido": bool(req.indicacao_clinica and len(req.indicacao_clinica) > 50),
        "auto_gerado": True,
        "categoria": "clinica",
    })
    checklist.append({
        "item": "Achados de imagem documentados",
        "resolvido": bool(req.achados_resumo and len(req.achados_resumo) > 20),
        "auto_gerado": False,
        "categoria": "imagem",
    })
    checklist.append({
        "item": "Tratamento conservador documentado (≥6 semanas)",
        "resolvido": bool(req.tto_conservador and len(req.tto_conservador) > 20),
        "auto_gerado": False,
        "categoria": "conservador",
    })
    checklist.append({
        "item": "CRM e CBO do solicitante",
        "resolvido": bool(req.crm and req.cbo),
        "auto_gerado": False,
        "categoria": "identificacao",
    })

    # Itens específicos de OPME
    if (req.necessita_opme or "").strip().lower() == "sim":
        checklist.append({
            "item": "Justificativa clínica individualizada de OPME",
            "resolvido": False,  # será gerada automaticamente
            "auto_gerado": True,
            "categoria": "opme",
        })
        checklist.append({
            "item": "3 cotações de OPME de fornecedores diferentes",
            "resolvido": False,  # precisa ser anexado manualmente
            "auto_gerado": False,
            "categoria": "opme",
        })

    # Itens derivados das pendências do motor
    for pend in res.pendencias:
        pend_lower = pend.lower()
        if "laudo" in pend_lower and "fisioterapia" in pend_lower:
            checklist.append({
                "item": "Laudo de fisioterapia anexado",
                "resolvido": False,
                "auto_gerado": False,
                "categoria": "documentacao",
            })
        elif "evolução" in pend_lower or "evolucao" in pend_lower:
            checklist.append({
                "item": "Evolução clínica documentada",
                "resolvido": False,
                "auto_gerado": False,
                "categoria": "documentacao",
            })

    return checklist


def _extract_doc_requirements(res: DecideResponse) -> list[str]:
    """Extrai documentos que precisam ser anexados manualmente."""
    docs = []
    for pend in res.pendencias:
        pend_lower = pend.lower()
        if "laudo" in pend_lower:
            docs.append("Laudo de fisioterapia")
        if "evolução" in pend_lower or "evolucao" in pend_lower:
            docs.append("Relatório de evolução clínica")
        if "cotação" in pend_lower or "cotacão" in pend_lower or "cotacao" in pend_lower:
            docs.append("3 cotações de OPME")
    # Deduplica
    return list(dict.fromkeys(docs))


def _build_texto_sadt(
    just_clinica: str,
    just_opme_geral: str,
    just_opme_itens: list[dict],
    req: DecideRequest,
) -> str:
    """Monta o texto completo para preenchimento da guia SADT."""
    blocos = []

    blocos.append("═══ JUSTIFICATIVA CLÍNICA ═══")
    blocos.append(just_clinica)

    if (req.necessita_opme or "").strip().lower() == "sim":
        blocos.append("")
        blocos.append("═══ JUSTIFICATIVA DE OPME ═══")
        blocos.append(just_opme_geral)

        if just_opme_itens:
            blocos.append("")
            blocos.append("── JUSTIFICATIVA POR ITEM ──")
            for i, item in enumerate(just_opme_itens, 1):
                fab = f" ({item['fabricante']})" if item.get("fabricante") else ""
                blocos.append(f"\n{i}. {item['item']}{fab} — Qtd: {item['qtd']}")
                blocos.append(f"   {item['justificativa']}")

    blocos.append("")
    blocos.append("═══ TRATAMENTO CONSERVADOR REALIZADO ═══")
    blocos.append(req.tto_conservador or "Não informado.")

    return "\n".join(blocos)
