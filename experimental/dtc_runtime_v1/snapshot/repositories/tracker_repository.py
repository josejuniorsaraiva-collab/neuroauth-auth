"""
NEUROAUTH — tracker_repository.py
Versão: 1.0.0

Responsabilidade: logging pós-decisão e sugestão automática de gaps.

Regras absolutas:
  - NÃO altera run_motor(), validator_rules.py nem a decisão clínica.
  - Roda APÓS o motor ter retornado e a persistência principal ter ocorrido.
  - Falhas aqui nunca interrompem a resposta ao frontend.
  - Cria as abas no Sheets se ainda não existirem (lazy init).

Abas gerenciadas:
  CASOS_LOG_AUTO      — linha por decisão completa, campos legíveis
  GAPS_SUGERIDOS_AUTO — linha por regra disparada, com sugestão de enriquecimento
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from .sheets_client import (
    _retry_on_quota,
    append_row_by_header,
    ensure_worksheet,
    get_header_row,
    get_worksheet,
)

logger = logging.getLogger("neuroauth.tracker")

# ─── Nomes das abas ────────────────────────────────────────────────────────────

_SHEET_LOG  = "CASOS_LOG_AUTO"
_SHEET_GAPS = "GAPS_SUGERIDOS_AUTO"

ENGINE_VERSION = "v2.0.0"

# ─── Schema CASOS_LOG_AUTO ─────────────────────────────────────────────────────

_LOG_TITLE    = "NEUROAUTH · Log Automático de Decisões"
_LOG_SUBTITLE = "Gerado automaticamente pelo backend — não editar manualmente"
_LOG_HEADERS  = [
    "log_id",
    "timestamp",
    "episodio_id",
    "decision_run_id",
    "profile_id",
    "convenio_id",
    "cod_tuss",
    "cod_cbhpm",
    "cid_principal",
    "niveis",
    "carater",
    "decision_status",
    "score_final",
    "resumo_bloqueios",
    "resumo_alertas",
    "resumo_pendencias",
    "bloqueios_json",
    "alertas_json",
    "pendencias_json",
    "opme_json",
    "clinical_context_json",
    "engine_version",
    "frontend_origin",
]

# ─── Schema GAPS_SUGERIDOS_AUTO ────────────────────────────────────────────────

_GAPS_TITLE    = "NEUROAUTH · Gaps Sugeridos Automaticamente"
_GAPS_SUBTITLE = "Sugestão baseada nas regras disparadas — requer validação humana"
_GAPS_HEADERS  = [
    "gap_id",
    "timestamp",
    "episodio_id",
    "decision_run_id",
    "regra_disparada",
    "gap_tipo",
    "aba_sugerida",
    "campo_sugerido",
    "acao_sugerida",
    "prioridade_sugerida",
    "status_revisao",
]

# ─── Tabela regra → gap ────────────────────────────────────────────────────────

_GAP_MAP: dict[str, dict] = {
    "SYS001": {
        "gap_tipo":          "PROFILE_NAO_CADASTRADO",
        "aba_sugerida":      "20_PROC_MESTRE",
        "campo_sugerido":    "profile_id",
        "acao_sugerida":     "Cadastrar perfil cirúrgico ausente em 20_PROC_MESTRE",
        "prioridade_sugerida": "ALTA",
    },
    "SYS002": {
        "gap_tipo":          "ERRO_CRITICO_MOTOR",
        "aba_sugerida":      "N/A",
        "campo_sugerido":    "N/A",
        "acao_sugerida":     "Verificar logs do Flask — erro interno no motor",
        "prioridade_sugerida": "ALTA",
    },
    "RGL005": {
        "gap_tipo":          "COD_TUSS_AUSENTE",
        "aba_sugerida":      "Formulário / 15_CODIGO_MAPS",
        "campo_sugerido":    "cod_tuss",
        "acao_sugerida":     "Revisar cod_tuss no caso ou mapear em 15_CODIGO_MAPS",
        "prioridade_sugerida": "ALTA",
    },
    "RGL010": {
        "gap_tipo":          "CBHPM_ESPERADO_AUSENTE",
        "aba_sugerida":      "20_PROC_MESTRE",
        "campo_sugerido":    "cod_cbhpm_esperado",
        "acao_sugerida":     "Preencher cod_cbhpm_esperado no perfil do procedimento",
        "prioridade_sugerida": "MEDIA",
    },
    "RGL011": {
        "gap_tipo":          "TUSS_ESPERADO_AUSENTE",
        "aba_sugerida":      "20_PROC_MESTRE",
        "campo_sugerido":    "cod_tuss_esperado",
        "acao_sugerida":     "Preencher cod_tuss_esperado no perfil do procedimento",
        "prioridade_sugerida": "MEDIA",
    },
    "RGL022": {
        "gap_tipo":          "NIVEIS_ACIMA_MAXIMO",
        "aba_sugerida":      "20_PROC_MESTRE",
        "campo_sugerido":    "max_niveis",
        "acao_sugerida":     "Revisar max_niveis no perfil ou ajustar niveis do caso",
        "prioridade_sugerida": "MEDIA",
    },
    "RGL040": {
        "gap_tipo":          "OPME_OBRIGATORIA_AUSENTE",
        "aba_sugerida":      "Formulário / frontend",
        "campo_sugerido":    "opme_context_json",
        "acao_sugerida":     "Selecionar OPME obrigatória antes de enviar o caso",
        "prioridade_sugerida": "ALTA",
    },
    "RGL050": {
        "gap_tipo":          "CID_FORA_PERFIL",
        "aba_sugerida":      "20_PROC_MESTRE",
        "campo_sugerido":    "cids_preferenciais / cids_incompativeis",
        "acao_sugerida":     "Enriquecer cids_preferenciais ou cids_incompativeis no perfil",
        "prioridade_sugerida": "MEDIA",
    },
    "RGL030": {
        "gap_tipo":          "CONVENIO_AUSENTE",
        "aba_sugerida":      "03_CONVENIOS",
        "campo_sugerido":    "convenio_id",
        "acao_sugerida":     "Cadastrar convênio ausente em 03_CONVENIOS",
        "prioridade_sugerida": "ALTA",
    },
}


# ─── Helpers internos ─────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_readable(items: Any) -> str:
    """
    Converte lista de regras disparadas em string legível.
    Aceita lista de dicts com chave 'regra' e/ou 'mensagem'.
    Retorna '' se vazio.
    Exemplo: [{"regra":"RGL022","mensagem":"niveis acima do maximo"}]
             → "RGL022: niveis acima do maximo"
    """
    if not items or not isinstance(items, list):
        return ""
    partes = []
    for item in items:
        if not isinstance(item, dict):
            partes.append(str(item))
            continue
        regra = (
            item.get("codigo")   # campo real do motor (validator_engine.py)
            or item.get("regra") or item.get("rule") or item.get("code") or "?"
        )
        msg   = (
            item.get("motivo")       # campo real do motor
            or item.get("mensagem")
            or item.get("message")
            or item.get("descricao")
            or item.get("description")
            or ""
        )
        partes.append(f"{regra}: {msg}" if msg else str(regra))
    return " | ".join(partes)


def _collect_regras(result: dict) -> list[str]:
    """Extrai todos os códigos de regra disparados (bloqueios + alertas + pendências)."""
    regras: list[str] = []
    for chave in ("bloqueios", "pendencias", "alertas"):
        items = result.get(chave) or []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    r = (
                        item.get("codigo")   # campo real do motor (validator_engine.py)
                        or item.get("regra") or item.get("rule") or item.get("code")
                    )
                    if r:
                        regras.append(str(r))
    # SYS001 / SYS002 aparecem como decision_status especial
    status = result.get("decision_status", "")
    if status in ("SYS001", "SYS002") and status not in regras:
        regras.append(status)
    return regras


def _ensure_log_sheet() -> Any:
    """Garante que CASOS_LOG_AUTO existe com cabeçalhos. Cria se ausente."""
    ws = ensure_worksheet(_SHEET_LOG, rows=5000, cols=len(_LOG_HEADERS) + 2)
    # Verifica se headers já estão presentes (linha 3)
    try:
        existing = get_header_row(ws, head=3)
    except Exception:
        existing = []
    if existing and existing[0] == _LOG_HEADERS[0]:
        return ws
    # Escreve título + subtítulo + headers (linhas 1, 2, 3)
    import gspread.utils as gu
    last_col = gu.rowcol_to_a1(1, len(_LOG_HEADERS))
    last_col_letter = last_col[:-1]  # remove row number
    _retry_on_quota(lambda: ws.batch_update([
        {"range": "A1", "values": [[_LOG_TITLE]]},
        {"range": "A2", "values": [[_LOG_SUBTITLE]]},
        {"range": f"A3:{last_col_letter}3", "values": [_LOG_HEADERS]},
    ]))
    logger.info("ensure_log_sheet: headers gravados em %s", _SHEET_LOG)
    return ws


def _ensure_gaps_sheet() -> Any:
    """Garante que GAPS_SUGERIDOS_AUTO existe com cabeçalhos. Cria se ausente."""
    ws = ensure_worksheet(_SHEET_GAPS, rows=5000, cols=len(_GAPS_HEADERS) + 2)
    try:
        existing = get_header_row(ws, head=3)
    except Exception:
        existing = []
    if existing and existing[0] == _GAPS_HEADERS[0]:
        return ws
    import gspread.utils as gu
    last_col = gu.rowcol_to_a1(1, len(_GAPS_HEADERS))
    last_col_letter = last_col[:-1]
    _retry_on_quota(lambda: ws.batch_update([
        {"range": "A1", "values": [[_GAPS_TITLE]]},
        {"range": "A2", "values": [[_GAPS_SUBTITLE]]},
        {"range": f"A3:{last_col_letter}3", "values": [_GAPS_HEADERS]},
    ]))
    logger.info("ensure_gaps_sheet: headers gravados em %s", _SHEET_GAPS)
    return ws


# ─── Funções públicas ─────────────────────────────────────────────────────────

def log_case_result(
    episodio_id: str,
    run_id: str,
    body: dict,
    result: dict,
) -> None:
    """
    Grava uma linha em CASOS_LOG_AUTO com o resultado completo da decisão.
    Inclui resumos legíveis de bloqueios/alertas/pendências.
    Nunca lança — erros são capturados e logados.
    """
    try:
        ws = _ensure_log_sheet()
        log_id = f"LOG_{uuid.uuid4().hex[:8].upper()}"
        row = {
            "log_id":              log_id,
            "timestamp":           _now_iso(),
            "episodio_id":         episodio_id,
            "decision_run_id":     run_id,
            "profile_id":          body.get("profile_id", ""),
            "convenio_id":         body.get("convenio_id", body.get("convenio", "")),
            "cod_tuss":            body.get("cod_tuss", ""),
            "cod_cbhpm":           body.get("cod_cbhpm", ""),
            "cid_principal":       body.get("cid_principal", body.get("cid", "")),
            "niveis":              body.get("qtd_niveis", body.get("niveis", "")),
            "carater":             body.get("carater_cod", body.get("carater", "")),
            "decision_status":     result.get("decision_status", ""),
            "score_final":         result.get("confidence_global", result.get("score", "")),
            "resumo_bloqueios":    _build_readable(result.get("bloqueios") or []),
            "resumo_alertas":      _build_readable(result.get("alertas") or []),
            "resumo_pendencias":   _build_readable(result.get("pendencias") or []),
            "bloqueios_json":      result.get("bloqueios") or [],
            "alertas_json":        result.get("alertas") or [],
            "pendencias_json":     result.get("pendencias") or [],
            "opme_json":           body.get("opme_context_json", body.get("opmes_selecionados", "")),
            "clinical_context_json": body.get("clinical_context_json", ""),
            "engine_version":      ENGINE_VERSION,
            "frontend_origin":     body.get("_origin", "frontend"),
        }
        append_row_by_header(ws, row, head=3)
        logger.info("log_case_result: gravado %s → %s", log_id, episodio_id)
    except Exception as exc:
        logger.error("log_case_result: falha ao gravar em %s — %s", _SHEET_LOG, exc)


def suggest_gap_candidates(
    episodio_id: str,
    run_id: str,
    result: dict,
) -> None:
    """
    Para cada regra disparada no resultado, grava uma linha em GAPS_SUGERIDOS_AUTO
    com a sugestão de enriquecimento correspondente da tabela _GAP_MAP.
    Regras sem mapeamento geram gap_tipo = REGRA_SEM_MAPEAMENTO para revisão humana.
    Nunca lança — erros são capturados e logados.
    """
    try:
        regras = _collect_regras(result)
        if not regras:
            return

        ws = _ensure_gaps_sheet()
        ts = _now_iso()

        for regra in regras:
            gap_info = _GAP_MAP.get(regra, {
                "gap_tipo":          "REGRA_SEM_MAPEAMENTO",
                "aba_sugerida":      "?",
                "campo_sugerido":    "?",
                "acao_sugerida":     f"Regra {regra} não tem mapeamento — revisar manualmente",
                "prioridade_sugerida": "MEDIA",
            })
            gap_id = f"GAP_{uuid.uuid4().hex[:8].upper()}"
            row = {
                "gap_id":             gap_id,
                "timestamp":          ts,
                "episodio_id":        episodio_id,
                "decision_run_id":    run_id,
                "regra_disparada":    regra,
                "gap_tipo":           gap_info["gap_tipo"],
                "aba_sugerida":       gap_info["aba_sugerida"],
                "campo_sugerido":     gap_info["campo_sugerido"],
                "acao_sugerida":      gap_info["acao_sugerida"],
                "prioridade_sugerida": gap_info["prioridade_sugerida"],
                "status_revisao":     "novo",
            }
            append_row_by_header(ws, row, head=3)
            logger.info(
                "suggest_gap: %s → regra=%s gap_tipo=%s",
                gap_id, regra, gap_info["gap_tipo"],
            )
    except Exception as exc:
        logger.error("suggest_gap_candidates: falha em %s — %s", _SHEET_GAPS, exc)
