"""
NEUROAUTH — Repositório: Convênio
Versão: 2.0.0

Responsabilidade única: buscar convenio_row pelo convenio_id.
Sem lógica de decisão. Sem acesso ao motor.
"""
from __future__ import annotations
import logging

logger = logging.getLogger("neuroauth.repo.convenio")


def get_convenio_row(convenio_id: str) -> dict | None:
    """
    Retorna a linha de regras da operadora para o convenio_id informado.

    Retorna None se não encontrado.
    Em produção: substituir stub por acesso real sem mudar a assinatura.
    """
    if not convenio_id:
        logger.warning("get_convenio_row chamado com convenio_id vazio")
        return None

    # -------------------------------------------------------------------------
    # STUB — substituir em produção
    # -------------------------------------------------------------------------
    _STUB: dict[str, dict] = {
        "UNIMED_CARIRI": {
            "convenio_id":   "UNIMED_CARIRI",
            "nome":          "Unimed Cariri",
            "exige_guia":    True,
            "formato_guia":  "TISS_3.05",
            "prazo_resposta_horas": 72,
        },
        "BRADESCO_SAUDE": {
            "convenio_id":   "BRADESCO_SAUDE",
            "nome":          "Bradesco Saude",
            "exige_guia":    True,
            "formato_guia":  "TISS_3.05",
            "prazo_resposta_horas": 48,
        },
    }
    # -------------------------------------------------------------------------

    row = _STUB.get(convenio_id)
    if row is None:
        logger.warning("convenio_row nao encontrado para convenio_id='%s'", convenio_id)
    return row
