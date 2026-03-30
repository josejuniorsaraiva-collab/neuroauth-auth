"""
NEUROAUTH — Repositório: Decisão
Versão: 2.0.0 REAL

Responsabilidade: ler episódios e persistir runs e resultados nas planilhas reais.
Sem lógica de decisão. Sem acesso direto ao motor.

Planilhas utilizadas (head=3):
  22_EPISODIOS (leitura + atualização)  — acesso por header name, nunca por posição.
  21_DECISION_RUNS (append)             — acesso por header name.

Schema confirmado 22_EPISODIOS (cols A–T):
  A  episodio_id          B  paciente_id          C  profile_id
  D  convenio_id          E  hospital_id          F  carater
  G  niveis               H  cid_principal        I  cid_secundarios_json
  J  cbo_executor         K  opme_context_json    L  clinical_context_json
  M  decision_status      N  decision_run_id      O  score_confianca
  P  sugestao_principal_json  Q  alternativas_json  R  status_operacional
  S  created_at           T  updated_at

Schema confirmado 21_DECISION_RUNS (cols A–K):
  A  decision_run_id      B  episodio_id          C  profile_id
  D  input_context_json   E  opcoes_geradas_json  F  opcao_escolhida_json
  G  score_final          H  alertas_json         I  bloqueios_json
  J  motor_version        K  created_at

Nota sobre _run_cache:
  O schema de 21_DECISION_RUNS não inclui todos os campos produzidos pelo motor
  (ex.: decision_status, confidence_global, pendencias_json, campos_inferidos_json).
  _run_cache armazena o run completo em memória durante o processo para que
  get_decision_run() retorne o dict íntegro para auditoria intra-processo.
  Não é fallback sintético: a persistência real acontece em 21_DECISION_RUNS.
  Em caso de reinicialização do processo, get_decision_run() lê da planilha
  e retorna o subconjunto disponível no schema.

SYS001 e SYS002 são persistidos como NO_GO — nunca silenciados.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from .sheets_client import (
    get_worksheet,
    get_header_row,
    find_row_by_col,
    append_row_by_header,
    update_row_fields,
)

logger = logging.getLogger("neuroauth.repo.decision")

_EPISODIOS_SHEET = "22_EPISODIOS"
_RUNS_SHEET      = "21_DECISION_RUNS"
_HEAD            = 3

# Cache intra-processo: run completo (inclui campos fora do schema da planilha).
# Não é fallback sintético. A fonte canônica é a planilha.
_run_cache: dict[str, dict] = {}


# ─── Interface pública ────────────────────────────────────────────────────────

def create_episodio(data: dict) -> str:
    """
    Cria novo episódio em 22_EPISODIOS via append_row_by_header.
    Retorna o episodio_id gravado.
    Exceções propagam — sem swallow silencioso.
    """
    episodio_id = data.get("episodio_id", "")
    ws = get_worksheet(_EPISODIOS_SHEET)
    append_row_by_header(ws, data, head=_HEAD)
    logger.info(
        "episodio criado: %s profile=%s convenio=%s",
        episodio_id, data.get("profile_id", ""), data.get("convenio_id", ""),
    )
    return episodio_id


def get_episodio(episodio_id: str) -> dict | None:
    """
    Lê episódio de 22_EPISODIOS por header name (nunca por posição).
    Garante que profile_id vem da col C e convenio_id da col D,
    independente do layout da planilha.

    Retorna None se não encontrado.
    """
    if not episodio_id:
        logger.warning("get_episodio: episodio_id vazio")
        return None

    try:
        ws = get_worksheet(_EPISODIOS_SHEET)
        _idx, row = find_row_by_col(ws, "episodio_id", episodio_id, head=_HEAD)
    except Exception as exc:
        logger.error("get_episodio: erro ao acessar %s: %s", _EPISODIOS_SHEET, exc)
        return None

    if row is None:
        logger.warning(
            "get_episodio: episodio_id='%s' não encontrado em %s",
            episodio_id, _EPISODIOS_SHEET,
        )
        return None

    return dict(row)


def save_decision_run(episodio_id: str, payload: dict, result: dict) -> str:
    """
    Persiste run completo em 21_DECISION_RUNS.
    Grava os campos que existem como colunas na planilha.
    Mantém dict completo em _run_cache para get_decision_run() intra-processo.
    Retorna decision_run_id gerado.

    SYS001 e SYS002 são persistidos — nunca silenciados.
    """
    run_id  = f"RUN_{uuid.uuid4().hex[:12].upper()}"
    now     = datetime.now(timezone.utc).isoformat()
    raw_case = payload.get("raw_case", {})

    # ── Linha gravada na planilha (schema confirmado 21_DECISION_RUNS) ────────
    row_data = {
        "decision_run_id":    run_id,
        "episodio_id":        episodio_id,
        "profile_id":         raw_case.get("profile_id", ""),
        "input_context_json": json.dumps(raw_case, ensure_ascii=False),
        # opcoes_geradas_json → autopreenchimentos produzidos pelo motor
        "opcoes_geradas_json": json.dumps(
            result.get("autopreenchimentos", []), ensure_ascii=False
        ),
        # opcao_escolhida_json → campos validados sem bloqueio
        "opcao_escolhida_json": json.dumps(
            result.get("campos_ok", []), ensure_ascii=False
        ),
        "score_final":     str(result.get("confidence_global", 0.0)),
        "alertas_json":    json.dumps(result.get("alertas",   []), ensure_ascii=False),
        "bloqueios_json":  json.dumps(result.get("bloqueios", []), ensure_ascii=False),
        "motor_version":   result.get("engine_version", ""),
        "created_at":      now,
    }

    ws = get_worksheet(_RUNS_SHEET)
    append_row_by_header(ws, row_data, head=_HEAD)
    logger.info(
        "decision_run gravado: run_id=%s episodio=%s status=%s",
        run_id, episodio_id, result.get("decision_status"),
    )

    # ── Cache intra-processo: dict completo com todos os campos do motor ──────
    _run_cache[run_id] = {
        "decision_run_id":         run_id,
        "episodio_id":             episodio_id,
        "profile_id":              raw_case.get("profile_id", ""),
        "decision_status":         result.get("decision_status", "NO_GO"),
        "confidence_global":       result.get("confidence_global", 0.0),
        "bloqueios_json":          result.get("bloqueios",          []),
        "pendencias_json":         result.get("pendencias",         []),
        "alertas_json":            result.get("alertas",            []),
        "campos_inferidos_json":   result.get("campos_inferidos",   []),
        "autopreenchimentos_json": result.get("autopreenchimentos", []),
        "engine_version":          result.get("engine_version",     ""),
        "created_at":              now,
    }

    return run_id


def save_decision_result(episodio_id: str, result: dict) -> None:
    """
    Atualiza o episódio em 22_EPISODIOS com o resultado da decisão.
    Usa update_row_fields por header name — seguro mesmo se colunas mudarem de posição.

    SYS001 e SYS002 são persistidos como NO_GO — nunca silenciados.
    """
    try:
        ws = get_worksheet(_EPISODIOS_SHEET)
        row_idx, _ = find_row_by_col(ws, "episodio_id", episodio_id, head=_HEAD)

        if row_idx is None:
            logger.error(
                "save_decision_result: episodio_id='%s' não encontrado — "
                "resultado NAO persistido",
                episodio_id,
            )
            return

        headers = get_header_row(ws, head=_HEAD)
        now     = datetime.now(timezone.utc).isoformat()

        updates = {
            "decision_status":        result.get("decision_status", "NO_GO"),
            "score_confianca":        str(result.get("confidence_global", 0.0)),
            "sugestao_principal_json": json.dumps(
                result.get("resumo_operacional", ""), ensure_ascii=False
            ),
            "alternativas_json":      json.dumps(
                result.get("alertas", []), ensure_ascii=False
            ),
            "decision_run_id":        result.get("_run_id", ""),
            "updated_at":             now,
        }

        update_row_fields(ws, row_idx, headers, updates)

        logger.info(
            "episodio '%s' atualizado: status=%s confianca=%s",
            episodio_id,
            result.get("decision_status"),
            result.get("confidence_global", 0.0),
        )

    except Exception as exc:
        logger.error(
            "save_decision_result: erro ao atualizar %s: %s",
            _EPISODIOS_SHEET, exc,
        )


def update_episodio_status(episodio_id: str, run_id: str, result: dict) -> None:
    """
    Atualiza decision_run_id e updated_at no episódio após persistência do run.
    Chamado pela rota após save_decision_run().
    """
    try:
        ws = get_worksheet(_EPISODIOS_SHEET)
        row_idx, _ = find_row_by_col(ws, "episodio_id", episodio_id, head=_HEAD)

        if row_idx is None:
            logger.warning(
                "update_episodio_status: episodio_id='%s' não encontrado", episodio_id
            )
            return

        headers = get_header_row(ws, head=_HEAD)
        update_row_fields(ws, row_idx, headers, {
            "decision_run_id": run_id,
            "updated_at":      datetime.now(timezone.utc).isoformat(),
        })

    except Exception as exc:
        logger.error("update_episodio_status: erro: %s", exc)


def get_decision_run(run_id: str) -> dict | None:
    """
    Retorna run pelo ID.

    Prioridade 1 — cache intra-processo: retorna dict completo (todos os campos do motor).
    Prioridade 2 — leitura de 21_DECISION_RUNS: retorna subconjunto do schema da planilha
                   (campos fora do schema são retornados como None/lista vazia).

    Usado em auditoria e testes. Nunca lança exceção.
    """
    if run_id in _run_cache:
        return _run_cache[run_id]

    # Leitura da planilha após reinicialização do processo
    try:
        ws = get_worksheet(_RUNS_SHEET)
        _, row = find_row_by_col(ws, "decision_run_id", run_id, head=_HEAD)
        if row:
            return {
                "decision_run_id":         row.get("decision_run_id", run_id),
                "episodio_id":             row.get("episodio_id", ""),
                "profile_id":              row.get("profile_id", ""),
                # Campos não no schema da planilha — retornados como None/vazio
                "decision_status":         None,
                "confidence_global":       None,
                "bloqueios_json":          _safe_json(row.get("bloqueios_json", "[]")),
                "pendencias_json":         [],
                "alertas_json":            _safe_json(row.get("alertas_json",   "[]")),
                "campos_inferidos_json":   [],
                "autopreenchimentos_json": [],
                "engine_version":          row.get("motor_version", ""),
                "created_at":              row.get("created_at", ""),
            }
    except Exception as exc:
        logger.error("get_decision_run: erro ao ler %s: %s", _RUNS_SHEET, exc)

    return None


# ─── Helper interno ───────────────────────────────────────────────────────────

def _safe_json(val: str) -> list | dict:
    try:
        return json.loads(val) if val else []
    except (json.JSONDecodeError, TypeError):
        return []
