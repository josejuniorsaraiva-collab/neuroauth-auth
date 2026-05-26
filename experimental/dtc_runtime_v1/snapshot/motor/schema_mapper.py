"""
NEUROAUTH — CHIP 1: Canonical Schema Mapper
Versão: 2.0.0

Missão: transformar qualquer entrada bruta em objeto canônico único.
Nunca decide. Nunca preenche campos protegidos por inferência.
Apenas normaliza, mapeia e marca.
"""

from __future__ import annotations

import json
from typing import Any

ENGINE_VERSION = "v2.0.0"

CAMPOS_PROTEGIDOS: frozenset[str] = frozenset({
    "COD_TUSS", "CID_PRINCIPAL", "OPME_JSON",
    "PROFILE_ID", "CONVENIO_ID", "CARATER", "LATERALIDADE",
})

ALIAS_MAP: dict[str, list[str]] = {
    "CASE_ID":          ["case_id", "episodio_id", "id_caso", "id"],
    "USER_ID":          ["user_id", "usuario_id", "medico_id"],
    "PROFILE_ID":       ["profile_id", "procedimento_id", "proc_id"],
    "PROC_NOME":        ["proc_nome", "descricao", "nome", "procedimento", "procedimento_nome"],
    "PROC_DESCRICAO":   ["proc_descricao", "descricao_detalhada", "descritivo"],
    "COD_TUSS":         ["cod_tuss", "codigo_tuss", "tuss", "tuss_codigo"],
    "COD_CBHPM":        ["cod_cbhpm", "codigo_cbhpm", "cbhpm"],
    "CID_PRINCIPAL":    ["cid_principal", "cid", "diagnostico_cid"],
    "CID_SECUNDARIOS":  ["cid_secundarios", "cids_secundarios", "cid_secundario"],
    "CONVENIO_ID":      ["convenio_id", "convenio", "operadora", "payer"],
    "CARATER":          ["carater", "urgencia_eletivo", "tipo_atendimento", "carater_cod"],
    "NIVEIS":           ["niveis", "quantidade_niveis", "num_niveis"],
    "LATERALIDADE":     ["lateralidade", "lado"],
    "TIPO_ANESTESIA":   ["tipo_anestesia", "anestesia"],
    "VIA_ACESSO":       ["via_acesso", "abordagem"],
    "ESPECIALIDADE":    ["especialidade"],
    "PORTE":            ["porte"],
    "PORTE_ANESTESICO": ["porte_anestesico"],
    "FILME":            ["filme", "qt_filme"],
    "NECESSITA_OPME":   ["necessita_opme", "usa_opme"],
    "OPME_FREQUENTE":   ["opme_frequente"],
    "OPME_JSON":        ["opme_json", "materiais_json", "implantes_json", "opme_context_json"],
    "DADOS_PACIENTE":   ["dados_paciente", "dados_paciente_json", "paciente_json"],
    "CONTEXTO_CLINICO": ["contexto_clinico", "clinical_context_json"],
    "REGRAS":           ["regras", "regras_json"],
}

# Campos herdáveis do proc_master_row → chave no mestre
# Campos protegidos são EXCLUÍDOS explicitamente desta herança
MASTER_INHERIT_MAP: dict[str, str] = {
    "PROC_NOME":        "descricao",
    "COD_CBHPM":        "codigo_cbhpm",
    "VIA_ACESSO":       "via_acesso",
    "ESPECIALIDADE":    "especialidade",
    "PORTE":            "porte",
    "PORTE_ANESTESICO": "porte_anestesico",
    "FILME":            "filme",
    "NECESSITA_OPME":   "opme_frequente",
    "OPME_FREQUENTE":   "opme_frequente",
    "REGRAS":           "regras_json",
    "TIPO_ANESTESIA":   "tipo_anestesia",
}

CONFIANCA_CONFIRMADO = 1.0
CONFIANCA_INFERIDO   = 0.92
CONFIANCA_AUSENTE    = 0.0


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (dict, list)):
        return len(value) == 0
    return False


def _resolve_from_dict(source: dict, aliases: list[str]) -> tuple[Any, str | None]:
    for alias in aliases:
        if alias in source:
            val = source[alias]
            if not _is_empty(val):
                return val, alias
    return None, None


def _build_empty_canonical() -> dict:
    return {
        "CASE_ID": "", "USER_ID": "", "PROFILE_ID": "",
        "PROC_NOME": "", "PROC_DESCRICAO": "",
        "COD_TUSS": "", "COD_CBHPM": "",
        "CID_PRINCIPAL": "", "CID_SECUNDARIOS": [],
        "CONVENIO_ID": "", "CARATER": "", "NIVEIS": None,
        "LATERALIDADE": "", "TIPO_ANESTESIA": "", "VIA_ACESSO": "",
        "ESPECIALIDADE": "", "PORTE": "", "PORTE_ANESTESICO": "",
        "FILME": "", "NECESSITA_OPME": False, "OPME_FREQUENTE": "",
        "OPME_JSON": {}, "DADOS_PACIENTE": {}, "CONTEXTO_CLINICO": {},
        "REGRAS": {}, "STATUS_FONTE": {}, "CONFIANCA": {}, "SOURCE_MAP": {},
    }


def _map_aliases(raw_case: dict, canonical: dict):
    status_fonte: dict[str, str] = {}
    source_map: dict[str, str] = {}
    for canon_field, aliases in ALIAS_MAP.items():
        value, found_key = _resolve_from_dict(raw_case, aliases)
        if value is not None:
            canonical[canon_field] = value
            status_fonte[canon_field] = "confirmado"
            source_map[canon_field] = f"raw_case.{found_key}"
        else:
            status_fonte[canon_field] = "ausente"
            source_map[canon_field] = "nenhuma"
    return canonical, status_fonte, source_map


def _enrich_from_master(canonical, status_fonte, source_map, proc_master_row):
    if not proc_master_row:
        return canonical, status_fonte, source_map
    for canon_field, master_key in MASTER_INHERIT_MAP.items():
        if canon_field in CAMPOS_PROTEGIDOS:
            continue  # nunca herdar campos protegidos
        if status_fonte.get(canon_field) == "confirmado":
            continue  # nunca sobrescrever dado confirmado
        master_value = proc_master_row.get(master_key)
        if not _is_empty(master_value):
            canonical[canon_field] = master_value
            status_fonte[canon_field] = "inferido"
            source_map[canon_field] = f"proc_master_row.{master_key}"
    return canonical, status_fonte, source_map


def _normalize_types(canonical: dict) -> dict:
    if canonical["NIVEIS"] is not None:
        try:
            canonical["NIVEIS"] = int(canonical["NIVEIS"])
        except (ValueError, TypeError):
            canonical["NIVEIS"] = None
    if isinstance(canonical["NECESSITA_OPME"], str):
        canonical["NECESSITA_OPME"] = canonical["NECESSITA_OPME"].lower() in ("true", "1", "sim", "yes")
    if isinstance(canonical["CID_SECUNDARIOS"], str):
        canonical["CID_SECUNDARIOS"] = [c.strip() for c in canonical["CID_SECUNDARIOS"].split(",") if c.strip()]
    for f in ("OPME_JSON", "DADOS_PACIENTE", "CONTEXTO_CLINICO", "REGRAS"):
        if isinstance(canonical[f], str):
            try:
                canonical[f] = json.loads(canonical[f])
            except Exception:
                canonical[f] = {}
    return canonical


def _compute_confidence(status_fonte: dict) -> dict[str, float]:
    mapping = {"confirmado": CONFIANCA_CONFIRMADO, "inferido": CONFIANCA_INFERIDO}
    return {f: mapping.get(s, CONFIANCA_AUSENTE) for f, s in status_fonte.items()}


def normalize_case(
    raw_case: dict,
    proc_master_row: dict | None = None,
    convenio_row: dict | None = None,
    session_user_id: str = "",
) -> dict:
    """
    CHIP 1 — Ponto de entrada.
    Retorna objeto canônico completo com STATUS_FONTE, CONFIANCA e SOURCE_MAP.
    Garantias: campos protegidos nunca inferidos; stateless; nunca lança exceção.
    """
    canonical = _build_empty_canonical()
    proc_master_row = proc_master_row or {}

    canonical, status_fonte, source_map = _map_aliases(raw_case, canonical)
    canonical, status_fonte, source_map = _enrich_from_master(
        canonical, status_fonte, source_map, proc_master_row
    )
    if _is_empty(canonical["USER_ID"]) and session_user_id:
        canonical["USER_ID"] = session_user_id
        status_fonte["USER_ID"] = "confirmado"
        source_map["USER_ID"] = "session_user_id"

    canonical = _normalize_types(canonical)
    canonical["STATUS_FONTE"] = status_fonte
    canonical["CONFIANCA"]    = _compute_confidence(status_fonte)
    canonical["SOURCE_MAP"]   = source_map
    return canonical
