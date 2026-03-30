"""
NEUROAUTH — Repositório: Convênio
Versão: 2.0.0 REAL

Responsabilidade: buscar convenio_row pelo convenio_id na planilha 03_CONVENIOS.
Sem regra de negócio. Sem defaults. Sem fallback sintético.

Schema confirmado (head=3, cols A–G):
  A  convenio_id       B  nome_convenio
  C  operadora_grupo   D  usa_portal_tiss
  E  modelo_autorizacao  F  observacoes
  G  ativo

Retorna dict limpo com todas as colunas disponíveis, ou None.
O motor (normalize_case) recebe convenio_row mas não usa nenhuma chave dele
atualmente via MASTER_INHERIT_MAP — o dict é passado por completude e uso futuro.
"""
from __future__ import annotations

import logging

from .sheets_client import get_worksheet, find_row_by_col

logger = logging.getLogger("neuroauth.repo.convenio")

_SHEET = "03_CONVENIOS"
_HEAD  = 3


def get_convenio_row(convenio_id: str) -> dict | None:
    """
    Busca a linha do convênio pelo convenio_id na planilha real.

    Retorna dict com todas as colunas da planilha, ou None se não encontrado
    ou se o registro estiver inativo.

    Sem defaults. Sem regra de negócio. Sem fallback sintético.
    """
    if not convenio_id:
        logger.warning("get_convenio_row: convenio_id vazio")
        return None

    try:
        ws = get_worksheet(_SHEET)
        _row_idx, row = find_row_by_col(ws, "convenio_id", convenio_id, head=_HEAD)
    except Exception as exc:
        logger.error(
            "get_convenio_row: erro ao acessar planilha '%s': %s", _SHEET, exc
        )
        return None

    if row is None:
        logger.warning(
            "get_convenio_row: convenio_id='%s' não encontrado em %s",
            convenio_id, _SHEET,
        )
        return None

    if str(row.get("ativo", "TRUE")).strip().upper() == "FALSE":
        logger.info(
            "get_convenio_row: convenio_id='%s' está inativo — ignorado", convenio_id
        )
        return None

    return dict(row)
