"""
NEUROAUTH — Control HUB Routes
Versão: 1.0.0

Endpoints:
  GET  /hub/decision_runs   — lista 21_DECISION_RUNS (filtros opcionais)
  GET  /hub/episodes        — lista 22_EPISODIOS (filtros opcionais)
  GET  /hub/metrics         — métricas agregadas
  PATCH /hub/runs/<run_id>/action — gate humano: APROVADO | SEGURADO | REVISAO

Auth: X-Neuroauth-Key (mesmo padrão do decision_routes.py)
CORS: * (mesmo padrão do sistema)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

from flask import Blueprint, jsonify, request, make_response

from repositories.sheets_client import get_worksheet, get_header_row

logger = logging.getLogger("neuroauth.routes.hub")

hub_bp = Blueprint("hub", __name__, url_prefix="/hub")

_RUNS_SHEET     = "21_DECISION_RUNS"
_EPIS_SHEET     = "22_EPISODIOS"
_HEAD           = 3
_NEUROAUTH_KEY: str | None = os.getenv("NEUROAUTH_API_KEY")


# ─── CORS helper ──────────────────────────────────────────────────────────────
def _cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization,X-Neuroauth-Key"
    return response


# ─── Auth ─────────────────────────────────────────────────────────────────────
def _check_api_key():
    if not _NEUROAUTH_KEY:
        return None  # modo aberto (dev)
    key = request.headers.get("X-Neuroauth-Key", "")
    if key != _NEUROAUTH_KEY:
        return _cors(jsonify({
            "error": "X-Neuroauth-Key ausente ou inválida.",
            "error_code": "HUB_AUTH_FAIL",
        })), 401
    return None


# ─── CORS preflight ────────────────────────────────────────────────────────────
@hub_bp.route("/<path:dummy>", methods=["OPTIONS"])
def options_handler(dummy=""):
    return _cors(make_response("", 204))


# ─── Sheets reader ────────────────────────────────────────────────────────────
def _read_sheet_as_dicts(sheet_name: str, head: int = 3) -> list[dict]:
    """Lê sheet completa e retorna lista de dicts por header name."""
    ws = get_worksheet(sheet_name)
    headers = get_header_row(ws, head=head)
    all_values = ws.get_all_values()
    rows = all_values[head:]  # skip header rows
    result = []
    for row in rows:
        if not any(str(c).strip() for c in row):
            continue  # skip linhas vazias
        d = {}
        for i, h in enumerate(headers):
            d[h] = row[i] if i < len(row) else ""
        result.append(d)
    return result


def _update_cell_by_col(sheet_name: str, id_col: str, id_val: str,
                         col_name: str, new_val: str, head: int = 3) -> bool:
    """Atualiza célula específica por id_col=id_val, coluna col_name."""
    from repositories.sheets_client import find_row_by_col, update_row_fields
    ws = get_worksheet(sheet_name)
    row_idx, _ = find_row_by_col(ws, id_col, id_val, head=head)
    if row_idx is None:
        return False
    headers = get_header_row(ws, head=head)
    update_row_fields(ws, row_idx, headers, {col_name: new_val})
    return True


# ─── GET /hub/decision_runs ───────────────────────────────────────────────────
@hub_bp.get("/decision_runs")
def list_decision_runs():
    """
    GET /hub/decision_runs
    Query params: gate, profile_id, limit (default 50)
    Retorna lista de runs ordenados por created_at desc.
    """
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    gate_filter    = request.args.get("gate", "").upper().strip()
    profile_filter = request.args.get("profile_id", "").strip()
    limit          = min(int(request.args.get("limit", 50)), 200)

    try:
        rows = _read_sheet_as_dicts(_RUNS_SHEET)
    except Exception as exc:
        logger.error("list_decision_runs: Sheets error: %s", exc)
        return _cors(jsonify({"error": "Falha ao ler planilha.", "detail": str(exc)})), 503

    # Normalizar e filtrar
    runs = []
    for r in rows:
        run_id    = r.get("decision_run_id", "")
        ep_id     = r.get("episodio_id", "")
        profile   = r.get("profile_id", "")
        gate      = r.get("decision_status", r.get("gate", ""))
        score     = r.get("score_final", r.get("score_clinico", ""))
        alertas   = r.get("alertas_json", "[]")
        bloqueios = r.get("bloqueios_json", "[]")
        motor_v   = r.get("motor_version", "")
        created   = r.get("created_at", "")
        hub_action = r.get("hub_action", "")

        if not run_id:
            continue
        if gate_filter and gate_filter not in gate.upper():
            continue
        if profile_filter and profile_filter.upper() not in profile.upper():
            continue

        # Parse score
        try:
            score_f = float(score) if score else None
        except ValueError:
            score_f = None

        # Parse alertas para risco_glosa heurístico
        try:
            alertas_list = json.loads(alertas) if alertas else []
            risco_glosa = len(alertas_list) * 15  # heurístico: cada alerta = +15%
            risco_glosa = min(risco_glosa, 100)
        except Exception:
            risco_glosa = 0

        runs.append({
            "decision_run_id": run_id,
            "episodio_id":     ep_id,
            "profile_id":      profile,
            "gate":            gate,
            "score":           score_f,
            "risco_glosa":     risco_glosa,
            "motor_version":   motor_v,
            "created_at":      created,
            "hub_action":      hub_action,
            "alertas":         alertas_list if isinstance(alertas_list, list) else [],
            "bloqueios":       _safe_json(bloqueios),
        })

    # Ordenar por created_at desc
    runs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    runs = runs[:limit]

    return _cors(jsonify({
        "total": len(runs),
        "limit": limit,
        "items": runs,
    })), 200


# ─── GET /hub/episodes ────────────────────────────────────────────────────────
@hub_bp.get("/episodes")
def list_episodes():
    """
    GET /hub/episodes
    Query params: status, convenio_id, limit (default 50)
    """
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    status_filter  = request.args.get("status", "").strip()
    convenio_filter = request.args.get("convenio_id", "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)

    try:
        rows = _read_sheet_as_dicts(_EPIS_SHEET)
    except Exception as exc:
        logger.error("list_episodes: Sheets error: %s", exc)
        return _cors(jsonify({"error": "Falha ao ler planilha.", "detail": str(exc)})), 503

    episodes = []
    for r in rows:
        ep_id      = r.get("episodio_id", "")
        paciente   = r.get("paciente_id", "")
        profile    = r.get("profile_id", "")
        convenio   = r.get("convenio_id", "")
        cid        = r.get("cid_principal", "")
        status     = r.get("decision_status", "")
        status_op  = r.get("status_operacional", "")
        run_id     = r.get("decision_run_id", "")
        score      = r.get("score_confianca", "")
        created    = r.get("created_at", "")

        if not ep_id:
            continue
        if status_filter and status_filter.upper() not in status.upper():
            continue
        if convenio_filter and convenio_filter.upper() not in convenio.upper():
            continue

        # Iniciais do paciente (2 primeiras letras)
        iniciais = ""
        if paciente:
            parts = paciente.strip().split()
            if len(parts) >= 2:
                iniciais = f"{parts[0][:2].upper()}.{parts[-1][:2].upper()}."
            else:
                iniciais = parts[0][:3].upper() if parts else "??"

        # Extrair procedimento do clinical_context_json
        procedimento = ""
        try:
            ctx = json.loads(r.get("clinical_context_json", "{}") or "{}")
            procedimento = ctx.get("procedimento", "") or ctx.get("indicacao_clinica", "")
        except Exception:
            pass

        try:
            score_f = float(score) if score else None
        except ValueError:
            score_f = None

        episodes.append({
            "episodio_id":    ep_id,
            "paciente":       iniciais,
            "profile_id":     profile,
            "convenio_id":    convenio,
            "cid_principal":  cid,
            "procedimento":   procedimento[:80] if procedimento else "",
            "decision_status": status,
            "status_operacional": status_op,
            "decision_run_id": run_id,
            "score_confianca": score_f,
            "created_at":     created,
        })

    episodes.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    episodes = episodes[:limit]

    return _cors(jsonify({
        "total": len(episodes),
        "limit": limit,
        "items": episodes,
    })), 200


# ─── GET /hub/metrics ─────────────────────────────────────────────────────────
@hub_bp.get("/metrics")
def hub_metrics():
    """
    GET /hub/metrics
    Agrega dados de 21_DECISION_RUNS para métricas do cockpit.
    """
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    try:
        runs_rows = _read_sheet_as_dicts(_RUNS_SHEET)
    except Exception as exc:
        logger.error("hub_metrics: Sheets error: %s", exc)
        return _cors(jsonify({"error": "Falha ao ler planilha.", "detail": str(exc)})), 503

    total = 0
    gate_dist: dict[str, int] = {}
    scores: list[float] = []
    risco_glosas: list[float] = []
    volume_by_day: dict[str, int] = {}
    motor_version = "?"

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    for r in runs_rows:
        run_id = r.get("decision_run_id", "")
        if not run_id:
            continue

        total += 1
        gate = r.get("decision_status", r.get("gate", "UNKNOWN"))
        gate_dist[gate] = gate_dist.get(gate, 0) + 1

        try:
            sc = float(r.get("score_final", r.get("score_clinico", "")) or 0)
            scores.append(sc)
        except ValueError:
            pass

        alertas_raw = r.get("alertas_json", "[]")
        try:
            alertas_list = json.loads(alertas_raw) if alertas_raw else []
            rg = min(len(alertas_list) * 15, 100)
            risco_glosas.append(float(rg))
        except Exception:
            pass

        motor_version = r.get("motor_version", motor_version) or motor_version

        # Volume por dia (últimos 7 dias)
        created = r.get("created_at", "")
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                if dt >= cutoff:
                    day = dt.strftime("%Y-%m-%d")
                    volume_by_day[day] = volume_by_day.get(day, 0) + 1
            except Exception:
                pass

    avg_score = round(sum(scores) / len(scores), 4) if scores else None
    avg_risco = round(sum(risco_glosas) / len(risco_glosas), 1) if risco_glosas else None

    # Preencher dias sem dados nos últimos 7 dias
    daily_series = []
    for i in range(7):
        d = (datetime.now(timezone.utc) - timedelta(days=6 - i)).strftime("%Y-%m-%d")
        daily_series.append({"date": d, "count": volume_by_day.get(d, 0)})

    return _cors(jsonify({
        "total_runs":       total,
        "gate_distribution": gate_dist,
        "avg_score":        avg_score,
        "avg_risco_glosa":  avg_risco,
        "volume_by_day":    daily_series,
        "motor_version":    motor_version,
    })), 200


# ─── PATCH /hub/runs/<run_id>/action ─────────────────────────────────────────
@hub_bp.patch("/runs/<run_id>/action")
def hub_action(run_id: str):
    """
    PATCH /hub/runs/<run_id>/action
    Body: {"action": "APROVADO|SEGURADO|REVISAO", "nota": "...opcional"}
    Grava hub_action em 21_DECISION_RUNS (cria coluna se não existir).
    """
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    body = request.get_json(silent=True) or {}
    action = str(body.get("action", "")).upper().strip()

    VALID_ACTIONS = {"APROVADO", "SEGURADO", "REVISAO"}
    if action not in VALID_ACTIONS:
        return _cors(jsonify({
            "error": f"action inválida: '{action}'. Use: {sorted(VALID_ACTIONS)}",
        })), 400

    nota = str(body.get("nota", "")).strip()[:200]
    now  = datetime.now(timezone.utc).isoformat()
    val  = f"{action} | {now}" + (f" | {nota}" if nota else "")

    try:
        updated = _update_cell_by_col(
            _RUNS_SHEET, "decision_run_id", run_id, "hub_action", val
        )
    except Exception as exc:
        logger.error("hub_action: Sheets error run_id=%s: %s", run_id, exc)
        return _cors(jsonify({"error": "Falha ao gravar planilha.", "detail": str(exc)})), 503

    if not updated:
        return _cors(jsonify({
            "error": f"run_id '{run_id}' não encontrado em {_RUNS_SHEET}",
        })), 404

    logger.info("hub_action: run_id=%s action=%s", run_id, action)
    return _cors(jsonify({
        "run_id":   run_id,
        "action":   action,
        "recorded": now,
    })), 200


# ─── Helper ───────────────────────────────────────────────────────────────────
def _safe_json(val):
    try:
        return json.loads(val) if val else []
    except Exception:
        return []
