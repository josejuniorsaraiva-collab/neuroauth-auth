"""
NEUROAUTH — Rotas: POST /episodios/<episodio_id>/decide + GET /episodios/summary

Versão: 2.1.0

Responsabilidade:
  POST /episodios/<episodio_id>/decide — executa motor para episódio existente
  GET  /episodios/summary             — métricas agregadas (go_rate, total)
"""

from __future__ import annotations

import json
import logging
import os
import traceback as _tb
import concurrent.futures

from flask import Blueprint, jsonify, make_response, request

from motor import run_motor
from repositories import (
    get_episodio,
    get_proc_master_row,
    get_convenio_row,
    save_decision_run,
    save_decision_result,
    update_episodio_status,
)
from repositories.sheets_client import get_worksheet

logger = logging.getLogger("neuroauth.routes.episodios")

episodios_bp = Blueprint("episodios", __name__, url_prefix="/episodios")

_NEUROAUTH_KEY: str | None = os.getenv("NEUROAUTH_API_KEY")
_SHEETS_TIMEOUT = float(os.getenv("SHEETS_TIMEOUT", "8"))

_RUNS_SHEET = "21_DECISION_RUNS"
_HEAD = 3


# ─── Helpers ──────────────────────────────────────────────────────────────────────────────

def _cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization,X-Neuroauth-Key"
    return response


def _check_api_key():
    if not _NEUROAUTH_KEY:
        return None
    key = request.headers.get("X-Neuroauth-Key", "")
    if key != _NEUROAUTH_KEY:
        logger.warning("_check_api_key: chave invalida ip=%s", request.remote_addr)
        return _cors(jsonify({
            "decision_status": "ERRO_AUTORIZACAO",
            "error_code": "SYS_API_KEY_INVALID",
            "erro": "X-Neuroauth-Key ausente ou invalida.",
        })), 401
    return None


def _sheets_call(fn, *args):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(fn, *args)
        try:
            return future.result(timeout=_SHEETS_TIMEOUT)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Sheets call '{fn.__name__}' excedeu {_SHEETS_TIMEOUT}s")


def _safe_json(val):
    try:
        return json.loads(val) if val else []
    except Exception:
        return []


# ─── CORS preflight ────────────────────────────────────────────────────────────────────────────

@episodios_bp.route("/<path:dummy>", methods=["OPTIONS"])
def options_handler(dummy=""):
    return _cors(make_response("", 204))


# ─── POST /episodios/<episodio_id>/decide ───────────────────────────────────────────────────────────────────────────────────────

@episodios_bp.post("/<episodio_id>/decide")
def episodio_decide(episodio_id: str):
    """
    POST /episodios/<episodio_id>/decide

    Executa o motor para um episódio existente em 22_EPISODIOS.
    Persiste run em 21_DECISION_RUNS e atualiza 22_EPISODIOS.
    Retorna output completo do motor.
    """
    key_err = _check_api_key()
    if key_err:
        return key_err

    # 1. Carregar episódio
    try:
        episodio = _sheets_call(get_episodio, episodio_id)
    except Exception as exc:
        logger.error("EPISODIO_LOAD_FAIL epis=%s err=%s", episodio_id, exc)
        return _cors(jsonify({
            "decision_status": "ERRO_INTERNO",
            "error_code": "SYS_EPISODIO_LOAD_FAIL",
            "erro": str(exc),
            "traceback_short": _tb.format_exc()[-400:],
        })), 500

    if episodio is None:
        return _cors(jsonify({"erro": f"episodio '{episodio_id}' nao encontrado"})), 404

    # 2. Identificadores
    profile_id = episodio.get("profile_id", "")
    convenio_id = episodio.get("convenio_id", "")
    session_user_id = episodio.get("usuario_id", "")

    # 3. Dados mestres
    try:
        proc_master_row = _sheets_call(get_proc_master_row, profile_id) if profile_id else None
        convenio_row = _sheets_call(get_convenio_row, convenio_id) if convenio_id else None
    except Exception as exc:
        logger.error("MASTER_LOOKUP_FAIL epis=%s err=%s", episodio_id, exc)
        return _cors(jsonify({
            "decision_status": "ERRO_INTERNO",
            "error_code": "SYS_MASTER_LOOKUP_FAIL",
            "erro": str(exc),
            "traceback_short": _tb.format_exc()[-400:],
        })), 500

    if proc_master_row is None:
        logger.warning(
            "episodio_decide '%s': proc_master_row ausente para profile_id='%s' — SYS001",
            episodio_id, profile_id,
        )

    # 4. raw_case
    raw_case = {k: v for k, v in episodio.items() if k not in (
        "decision_status", "score_confianca", "decision_run_id",
        "sugestao_principal_json", "alternativas_json", "updated_at",
    )}

    payload = {
        "raw_case": raw_case,
        "proc_master_row": proc_master_row,
        "convenio_row": convenio_row,
        "session_user_id": session_user_id,
    }

    # 5. Motor
    result = run_motor(
        raw_case=raw_case,
        proc_master_row=proc_master_row,
        convenio_row=convenio_row,
        session_user_id=session_user_id,
    )

    # 6. Persistir
    run_id = None
    try:
        run_id = save_decision_run(episodio_id, payload, result)
        result["_run_id"] = run_id
        save_decision_result(episodio_id, result)
        update_episodio_status(episodio_id, run_id, result)
    except Exception as exc:
        logger.error("PERSISTENCE_FAIL episodio_id=%s error=%s", episodio_id, exc)
        result["_run_id"] = run_id or "ERR_PERSIST"
        result["_persistence_warning"] = f"{type(exc).__name__}: {str(exc)[:200]}"

    result["motor_version"] = "2.1.0"
    logger.info(
        "episodio_decide '%s': status=%s run_id=%s confianca=%.3f",
        episodio_id, result.get("decision_status"), run_id,
        result.get("confidence_global", 0.0),
    )
    return _cors(jsonify(result)), 200


# ─── GET /episodios/summary ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

@episodios_bp.get("/summary")
def episodios_summary():
    """
    GET /episodios/summary

    Lê 21_DECISION_RUNS e retorna métricas agregadas:
      total_decisions, go_count, go_rate (0.0–1.0)

    GO = linha sem bloqueios (bloqueios_json vazio ou []).
    """
    try:
        def _read_runs():
            ws = get_worksheet(_RUNS_SHEET)
            return ws.get_all_records(head=_HEAD)

        rows = _sheets_call(_read_runs)
        total = len(rows)
        go_count = sum(
            1 for r in rows
            if not _safe_json(r.get("bloqueios_json", "[]"))
        )
        go_rate = round(go_count / total, 4) if total > 0 else 0.0

        return _cors(jsonify({
            "total_decisions": total,
            "go_count": go_count,
            "go_rate": go_rate,
        })), 200

    except Exception as exc:
        logger.error("SUMMARY_FAIL err=%s", exc)
        return _cors(jsonify({
            "error_code": "SYS_SUMMARY_FAIL",
            "erro": str(exc),
            "traceback_short": _tb.format_exc()[-400:],
        })), 500
