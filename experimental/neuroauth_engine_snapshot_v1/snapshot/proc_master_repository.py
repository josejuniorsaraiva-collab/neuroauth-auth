"""
NEUROAUTH — Repositório: Procedimento Mestre
Versão: 2.0.0

Responsabilidade única: buscar proc_master_row pelo profile_id.
Não contém lógica de decisão. Não acessa o motor.
Adaptável a Sheets, banco relacional ou cache — sem mudar a interface.
"""
from __future__ import annotations
import logging

logger = logging.getLogger("neuroauth.repo.proc_master")


def get_proc_master_row(profile_id: str) -> dict | None:
    """
    Retorna a linha do cadastro mestre para o profile_id informado.

    Retorna None se não encontrado — o motor trata ausência como SYS001.

    Implementação atual: stub em memória para desenvolvimento/teste.
    Em produção: substituir o corpo pelo acesso real (Sheets via gspread,
    banco via SQLAlchemy, Redis cache, etc.) sem alterar a assinatura.
    """
    if not profile_id:
        logger.warning("get_proc_master_row chamado com profile_id vazio")
        return None

    # -------------------------------------------------------------------------
    # STUB — substituir em produção pelo acesso real ao dado mestre
    # -------------------------------------------------------------------------
    _STUB: dict[str, dict] = {
        "PROF_ACDF_01": {
            "descricao":         "Artrodese cervical anterior com descompressao",
            "especialidade":     "Neurocirurgia",
            "porte":             "7C",
            "porte_anestesico":  "5",
            "via_acesso":        "anterior",
            "codigo_cbhpm":      "3.07.15.39-3",
            "regras_json": {
                "cod_tuss_esperado":          "40808505",
                "cod_cbhpm_esperado":         "3.07.15.39-3",
                "multinivel":                 True,
                "min_niveis":                 1,
                "max_niveis":                 3,
                "lateralidade_obrigatoria":   False,
                "opme_obrigatoria":           True,
                "opme_materiais_permitidos":  ["CAGE_PEEK_01", "PLACA_CERV_02", "PARAFUSO_CERV_03"],
                "opme_quantidade_por_niveis": True,
                "carater_obrigatorio":        True,
                "aceita_urgencia":            True,
                "cids_preferenciais":         ["M50.1", "M50.0", "M47.21"],
                "cids_incompativeis":         ["C00", "C01", "C02"],
            },
        },
        "PROF_LAMI_01": {
            "descricao":        "Laminectomia lombar",
            "especialidade":    "Neurocirurgia",
            "porte":            "6B",
            "porte_anestesico": "4",
            "via_acesso":       "posterior",
            "codigo_cbhpm":     "3.07.16.40-1",
            "regras_json": {
                "cod_tuss_esperado":        "40808610",
                "multinivel":               True,
                "min_niveis":               1,
                "max_niveis":               5,
                "lateralidade_obrigatoria": False,
                "opme_obrigatoria":         False,
                "carater_obrigatorio":      False,
                "cids_preferenciais":       ["M51.1", "M47.16", "G55"],
                "cids_incompativeis":       [],
            },
        },
    }
    # -------------------------------------------------------------------------

    row = _STUB.get(profile_id)
    if row is None:
        logger.warning("proc_master_row nao encontrado para profile_id='%s'", profile_id)
    return row
