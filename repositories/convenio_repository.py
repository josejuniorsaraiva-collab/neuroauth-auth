"""
NEUROAUTH â RepositÃ³rio: ConvÃªnio
VersÃ£o: 2.1.0
Responsabilidade: buscar convenio_row pelo convenio_id na planilha 03_CONVENIOS.
Sem regra de negÃ³cio. Sem defaults. Sem fallback sintÃ©tico.

Schema confirmado (head=3, cols AâG):
  A  convenio_id
  B  nome_convenio
  C  operadora_grupo
  D  usa_portal_tiss
  E  modelo_autorizacao
  F  observacoes
  G  ativo

MAP_CONVENIO: normaliza variantes regionais para a chave canÃ´nica da planilha.
Ex.: UNIMED_CARIRI â UNIMED (planilha tem a linha "UNIMED")
"""
from __future__ import annotations
import logging
from .sheets_client import get_worksheet, find_row_by_col

logger = logging.getLogger("neuroauth.repo.convenio")

_SHEET = "03_CONVENIOS"
_HEAD = 3

# ââ NormalizaÃ§Ã£o de variantes regionais para chave canÃ´nica da planilha ââââââ
MAP_CONVENIO: dict[str, str] = {
    "UNIMED_CARIRI":     "UNIMED",
    "UNIMED_FORTALEZA":  "UNIMED",
    "UNIMED_CE":         "UNIMED",
    "UNIMED_SUL":        "UNIMED",
    "UNIMED_SP":         "UNIMED",
    "BRADESCO_SAUDE":    "BRADESCO",
    "BRADESCO_TOP":      "BRADESCO",
    "AMIL_ONE":          "AMIL",
    "AMIL_S450":         "AMIL",
    "SULAMERICA_SAUDE":  "SULAMERICA",
    "NOTREDAME_INTERMÃDICA": "NOTREDAME",
    "HAPVIDA_MAIS":      "HAPVIDA",
    "HAPVIDA_ESSENCIAL": "HAPVIDA",
}


def _normalize_convenio(convenio_id: str) -> str:
    """Mapeia variante regional para chave canÃ´nica. Retorna original se nÃ£o mapeado."""
    key = (convenio_id or "").strip().upper()
    return MAP_CONVENIO.get(key, key)


def get_convenio_row(convenio_id: str) -> dict | None:
    """
    Busca a linha do convÃªnio pelo convenio_id na planilha real.
    Aplica MAP_CONVENIO para normalizar variantes regionais antes da busca.
    Retorna dict com todas as colunas da planilha, ou None se nÃ£o encontrado
    ou se o registro estiver inativo.
    """
    if not convenio_id:
        logger.warning("get_convenio_row: convenio_id vazio")
        return None

    normalized = _normalize_convenio(convenio_id)
    if normalized != convenio_id.strip().upper():
        logger.info(
            "get_convenio_row: convenio_id='%s' normalizado para '%s' via MAP_CONVENIO",
            convenio_id,
            normalized,
        )

    try:
        ws = get_worksheet(_SHEET)
        _row_idx, row = find_row_by_col(ws, "convenio_id", normalized, head=_HEAD)
    except Exception as exc:
        logger.error(
            "get_convenio_row: erro ao acessar planilha '%s': %s", _SHEET, exc
        )
        return None

    if row is None:
        logger.warning(
            "get_convenio_row: convenio_id='%s' (normalized='%s') nÃ£o encontrado em %s",
            convenio_id,
            normalized,
            _SHEET,
        )
        return None

    if str(row.get("ativo", "TRUE")).strip().upper() == "FALSE":
        logger.info(
            "get_convenio_row: convenio_id='%s' estÃ¡ inativo â ignorado", convenio_id
        )
        return None

    return dict(row)
