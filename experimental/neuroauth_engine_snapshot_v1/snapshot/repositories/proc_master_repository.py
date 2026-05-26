"""
NEUROAUTH — Repositório: Procedimento Mestre
Versão: 2.0.0 REAL

Responsabilidade: buscar proc_master_row pelo profile_id na planilha 20_PROC_MESTRE.
Sem lógica de decisão. Sem inferência. Sem fallback sintético.

Schema confirmado (head=3, cols A–S):
  A  profile_id              B  nome_clinico
  C  especialidade           D  subespecialidade
  E  descricao_curta         F  ativo
  G  permite_opme            H  permite_multinivel
  I  min_niveis              J  max_niveis
  K  exige_cid_principal     L  exige_lateralidade
  M  exige_carater_urgencia  N  tipo_acesso
  O  grupo_documental_padrao P  observacoes_tecnicas
  Q  versao_regra            R  created_at
  S  updated_at

Mapeamento obrigatório → chaves esperadas pelo motor (MASTER_INHERIT_MAP):
  nome_clinico            → descricao
  tipo_acesso             → via_acesso
  permite_opme            → regras_json.opme_obrigatoria
  permite_multinivel      → regras_json.multinivel
  min_niveis              → regras_json.min_niveis
  max_niveis              → regras_json.max_niveis
  exige_lateralidade      → regras_json.lateralidade_obrigatoria
  exige_carater_urgencia  → regras_json.carater_obrigatorio

Colunas ausentes na planilha (cod_tuss_esperado, codigo_cbhpm, porte, etc.)
retornam string vazia ou lista vazia. O motor trata ausência sem lançar exceção:
regras que dependem desses valores simplesmente não disparam.
"""
from __future__ import annotations

import logging

from .sheets_client import get_worksheet, find_row_by_col, read_all_records

logger = logging.getLogger("neuroauth.repo.proc_master")

_SHEET = "20_PROC_MESTRE"
_HEAD  = 3


# ─── Helpers internos ─────────────────────────────────────────────────────────

def _bool(val: str) -> bool:
    return str(val).strip().upper() in ("TRUE", "1", "SIM", "YES")


def _int_or_none(val: str) -> int | None:
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def _build_regras_json(row: dict) -> dict:
    """
    Monta regras_json a partir das colunas flat da planilha.
    Campos não presentes no schema atual retornam valores neutros:
    - string vazia → regras de código (RGL010/011) não disparam
    - lista vazia  → regras de CID/OPME material não disparam
    - False        → restrições não obrigatórias não disparam
    """
    return {
        # Colunas disponíveis na planilha
        "multinivel":               _bool(row.get("permite_multinivel", "")),
        "min_niveis":               _int_or_none(row.get("min_niveis", "")),
        "max_niveis":               _int_or_none(row.get("max_niveis", "")),
        "opme_obrigatoria":         _bool(row.get("permite_opme", "")),
        "lateralidade_obrigatoria": _bool(row.get("exige_lateralidade", "")),
        "carater_obrigatorio":      _bool(row.get("exige_carater_urgencia", "")),
        # Colunas ausentes no schema atual — valores neutros explícitos
        "cod_tuss_esperado":          "",
        "cod_cbhpm_esperado":         "",
        "opme_materiais_permitidos":  [],
        "opme_quantidade_por_niveis": False,
        "aceita_urgencia":            True,
        "cids_preferenciais":         [],
        "cids_incompativeis":         [],
    }


# ─── Interface pública ────────────────────────────────────────────────────────

def get_proc_master_row(profile_id: str) -> dict | None:
    """
    Busca a linha de proc_master_row pelo profile_id na planilha real.

    Retorna dict com chaves no formato esperado pelo motor (MASTER_INHERIT_MAP),
    ou None se profile_id não encontrado ou registro inativo.

    Sem defaults sintéticos. Sem fallback em memória.
    """
    if not profile_id:
        logger.warning("get_proc_master_row: profile_id vazio")
        return None

    try:
        ws = get_worksheet(_SHEET)
        _row_idx, row = find_row_by_col(ws, "profile_id", profile_id, head=_HEAD)
    except Exception as exc:
        logger.error(
            "get_proc_master_row: erro ao acessar planilha '%s': %s", _SHEET, exc
        )
        return None

    if row is None:
        logger.warning(
            "get_proc_master_row: profile_id='%s' não encontrado em %s",
            profile_id, _SHEET,
        )
        return None

    if str(row.get("ativo", "TRUE")).strip().upper() == "FALSE":
        logger.info(
            "get_proc_master_row: profile_id='%s' está inativo — ignorado", profile_id
        )
        return None

    # Mapeia colunas da planilha → chaves esperadas pelo motor
    return {
        # Chaves lidas por MASTER_INHERIT_MAP em schema_mapper.py
        "descricao":        row.get("nome_clinico", ""),       # col B → PROC_NOME
        "especialidade":    row.get("especialidade", ""),      # col C → ESPECIALIDADE
        "via_acesso":       row.get("tipo_acesso", ""),        # col N → VIA_ACESSO
        "porte":            row.get("porte", ""),              # ausente na planilha → ""
        "porte_anestesico": row.get("porte_anestesico", ""),   # ausente → ""
        "filme":            row.get("filme", ""),              # ausente → ""
        "opme_frequente":   row.get("permite_opme", ""),       # col G → OPME_FREQUENTE
        "codigo_cbhpm":     row.get("codigo_cbhpm", ""),       # ausente → ""
        "tipo_anestesia":   row.get("tipo_anestesia", ""),     # ausente → ""
        "regras_json":      _build_regras_json(row),           # montado a partir de cols flat
        # Campos adicionais (contexto, não lidos pelo motor)
        "_profile_id":          profile_id,
        "_subespecialidade":    row.get("subespecialidade", ""),
        "_descricao_curta":     row.get("descricao_curta", ""),
        "_grupo_documental":    row.get("grupo_documental_padrao", ""),
        "_versao_regra":        row.get("versao_regra", ""),
    }


def get_profiles_requiring_opme() -> list[str]:
    """
    Retorna lista de profile_ids ativos onde permite_opme=TRUE.
    Usado pelo /decision/config para informar o frontend quais
    perfis devem exibir o bloco OPME.
    Fallback hardcoded se a planilha não estiver acessível.
    """
    _FALLBACK = [
        "ACDF_1_NIVEL",
        "ACDF_2_NIVEIS",
        "MICRODISCECTOMIA_LOMBAR",
        "TUMOR_CEREBRAL_RESECCAO",
        "PROF_ACDF_01",
    ]
    try:
        ws = get_worksheet(_SHEET)
        records = read_all_records(ws, head=_HEAD)
        result = [
            r["profile_id"]
            for r in records
            if _bool(r.get("permite_opme", ""))
            and str(r.get("ativo", "TRUE")).strip().upper() != "FALSE"
            and r.get("profile_id", "").strip()
        ]
        if result:
            logger.info("get_profiles_requiring_opme: %d perfis com OPME obrigatória", len(result))
            return result
        logger.warning("get_profiles_requiring_opme: planilha vazia — usando fallback")
    except Exception as exc:
        logger.error("get_profiles_requiring_opme: erro ao acessar planilha: %s — usando fallback", exc)
    return _FALLBACK
