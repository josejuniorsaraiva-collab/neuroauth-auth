"""
routes/hub_routes.py — NEUROAUTH CONTROL HUB
Versão: 1.0.0
Responsabilidade: endpoints de monitoramento e gate humano em tempo real.
Requer JWT Bearer (mesmo segredo de gateway_routes.py via JWT_SECRET env var).

Endpoints:
    GET  /hub/decision_runs   — lista DECISION_RUNS com filtros opcionais
    GET  /hub/episodes        — lista EPISODIOS com filtros opcionais
    GET  /hub/metrics         — métricas agregadas
    PATCH /hub/runs/<run_id>/action — gate humano: aprovar/segurar/revisar
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps

from flask import Blueprint, jsonify, make_response, request

from repositories.sheets_client import (
    find_row_by_col,
    get_header_row,
    get_worksheet,
    read_all_records,
    update_row_fields,
)

logger = logging.getLogger("neuroauth.hub")
hub_bp = Blueprint("hub", __name__, url_prefix="/hub")

# ─── Constantes ──────────────────────────────────────────────────────────────
_RUNS_SHEET = "21_DECISION_RUNS"
_EPISODIOS_SHEET = "22_EPISODIOS"
_HEAD = 3
JWT_SECRET = os.environ.get("JWT_SECRET", "neuroauth-default-secret-CHANGE-ME")

# ─── CORS ────────────────────────────────────────────────────────────────────
def _cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,PATCH,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return response


# ─── JWT (inline para evitar import circular com gateway_routes) ─────────────
def _verify_jwt(token: str) -> dict | None:
    import base64
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, sig = parts
        sig_input = f"{header}.{body}".encode()
        expected = base64.urlsafe_b64encode(
            hmac.new(JWT_SECRET.encode(), sig_input, hashlib.sha256).digest()
        ).rstrip(b"=").decode()
        if not hmac.compare_digest(sig, expected):
            return None
        padding = 4 - len(body) % 4
        payload = json.loads(base64.urlsafe_b64decode(body + "=" * padding))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


def require_jwt(f):
    """Decorator: exige JWT válido no header Authorization: Bearer <token>."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return _cors(jsonify({"error": "missing_token"})), 401
        payload = _verify_jwt(auth[7:])
        if not payload:
            return _cors(jsonify({"error": "invalid_token"})), 401
        request.jwt_payload = payload
        return f(*args, **kwargs)
    return decorated


# ─── Helpers internos ────────────────────────────────────────────────────────
def _safe_json(val) -> list | dict:
    """Parse seguro de JSON string. Retorna [] em caso de erro."""
    try:
        return json.loads(val) if val else []
    except Exception:
        return []


def _infer_gate(row: dict) -> str:
    """Infere o gate a partir dos bloqueios e alertas do run."""
    bloqueios = _safe_json(row.get("bloqueios_json", "[]"))
    alertas = _safe_json(row.get("alertas_json", "[]"))
    score = _safe_float(row.get("score_final", 0))

    if bloqueios:
        return "NO_GO"
    if alertas:
        return "GO_COM_RESSALVAS"
    if score > 0 and score < 0.3:
        return "PRE_ANALISE"
    return "GO"


def _extract_gate_reason(row: dict) -> str:
    """Extrai texto do primeiro bloqueio ou alerta."""
    for field in ("bloqueios_json", "alertas_json"):
        items = _safe_json(row.get(field, "[]"))
        if not items:
            continue
        first = items[0]
        if isinstance(first, dict):
            return first.get("mensagem", first.get("message", str(first)))[:200]
        return str(first)[:200]
    return ""


def _extract_risco_glosa(row: dict) -> float:
    """Extrai risco_glosa dos alertas/bloqueios. Fallback: score invertido."""
    for field in ("bloqueios_json", "alertas_json"):
        items = _safe_json(row.get(field, "[]"))
        for item in items:
            if isinstance(item, dict):
                risco = item.get("risco_glosa") or item.get("risk_score")
                if risco is not None:
                    return round(float(risco), 2)
    # Fallback: (1 - score) * 100 como proxy de risco
    score = _safe_float(row.get("score_final", 0))
    return round((1.0 - score) * 100, 1) if score > 0 else 50.0


def _safe_float(val) -> float:
    try:
        return float(val) if val not in (None, "", "None") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _rowcol_to_a1(row: int, col: int) -> str:
    import gspread.utils
    return gspread.utils.rowcol_to_a1(row, col)


# ─── CORS preflight ──────────────────────────────────────────────────────────
@hub_bp.route("/<path:dummy>", methods=["OPTIONS"])
def options_handler(dummy=""):
    return _cors(make_response("", 204))


# ─── GET /hub/decision_runs ──────────────────────────────────────────────────
@hub_bp.get("/decision_runs")
@require_jwt
def list_decision_runs():
    """
    Lista DECISION_RUNS com filtros opcionais.
    Query params: convenio, gate, limit (default 50, max 200)
    """
    gate_filter = request.args.get("gate", "").strip().upper()
    convenio_filter = request.args.get("convenio", "").strip().upper()
    limit = min(int(request.args.get("limit", 50) or 50), 200)

    try:
        ws = get_worksheet(_RUNS_SHEET)
        rows = read_all_records(ws, head=_HEAD)
    except Exception as exc:
        logger.error("hub.list_decision_runs: Sheets error: %s", exc)
        return _cors(jsonify({"error": "sheets_unavailable", "detail": str(exc)})), 503

    result = []
    for row in reversed(rows):  # mais recente primeiro
        gate = _infer_gate(row)
        if gate_filter and gate != gate_filter:
            continue

        # Extrai convenio_id do input_context_json
        input_ctx = _safe_json(row.get("input_context_json", "{}"))
        convenio = ""
        if isinstance(input_ctx, dict):
            convenio = input_ctx.get("convenio_id", "")
        if convenio_filter and convenio_filter not in convenio.upper():
            continue

        score = _safe_float(row.get("score_final", 0))

        result.append({
            "decision_run_id": row.get("decision_run_id", ""),
            "episode_id": row.get("episodio_id", ""),
            "gate": gate,
            "score_clinico": round(score, 4),
            "camada3_risco": _extract_risco_glosa(row),
            "timestamp": row.get("created_at", ""),
            "versao_motor": row.get("motor_version", ""),
            "gate_reason": _extract_gate_reason(row),
            "tempo_ms": None,
            "convenio": convenio,
            "hub_action": row.get("hub_action", ""),
            "hub_nota": row.get("hub_nota", ""),
        })

        if len(result) >= limit:
            break

    return _cors(jsonify(result)), 200


# ─── GET /hub/episodes ───────────────────────────────────────────────────────
@hub_bp.get("/episodes")
@require_jwt
def list_episodes():
    """
    Lista EPISODIOS com filtros opcionais.
    Query params: status, convenio, limit (default 50, max 200)
    """
    status_filter = request.args.get("status", "").strip().upper()
    convenio_filter = request.args.get("convenio", "").strip().upper()
    limit = min(int(request.args.get("limit", 50) or 50), 200)

    try:
        ws = get_worksheet(_EPISODIOS_SHEET)
        rows = read_all_records(ws, head=_HEAD)
    except Exception as exc:
        logger.error("hub.list_episodes: Sheets error: %s", exc)
        return _cors(jsonify({"error": "sheets_unavailable", "detail": str(exc)})), 503

    result = []
    for row in reversed(rows):
        status = row.get("decision_status", "").strip().upper()
        if status_filter and status != status_filter:
            continue

        convenio = row.get("convenio_id", "")
        if convenio_filter and convenio_filter not in convenio.upper():
            continue

        # Iniciais seguras: 2 primeiros chars do paciente_id (nunca nome completo)
        pid = row.get("paciente_id", "") or row.get("profile_id", "")
        iniciais = (pid[:2].upper() if pid else "??")

        result.append({
            "episode_id": row.get("episodio_id", ""),
            "paciente_iniciais": iniciais,
            "cid": row.get("cid_principal", ""),
            "procedimento": row.get("profile_id", ""),
            "convenio": convenio,
            "status": status,
            "created_at": row.get("created_at", ""),
        })

        if len(result) >= limit:
            break

    return _cors(jsonify(result)), 200


# ─── GET /hub/metrics ────────────────────────────────────────────────────────
@hub_bp.get("/metrics")
@require_jwt
def get_metrics():
    """Métricas agregadas dos DECISION_RUNS."""
    try:
        ws = get_worksheet(_RUNS_SHEET)
        rows = read_all_records(ws, head=_HEAD)
    except Exception as exc:
        logger.error("hub.get_metrics: Sheets error: %s", exc)
        return _cors(jsonify({"error": "sheets_unavailable", "detail": str(exc)})), 503

    total_runs = len(rows)
    gate_distribution: dict[str, int] = {"GO": 0, "GO_COM_RESSALVAS": 0, "NO_GO": 0, "PRE_ANALISE": 0}
    scores: list[float] = []
    riscos: list[float] = []
    volume_by_day: dict[str, int] = defaultdict(int)
    motor_version = ""

    for row in rows:
        gate = _infer_gate(row)
        gate_distribution[gate] = gate_distribution.get(gate, 0) + 1

        score = _safe_float(row.get("score_final", 0))
        if score > 0:
            scores.append(score)

        risco = _extract_risco_glosa(row)
        riscos.append(risco)

        created_at = row.get("created_at", "")
        if created_at and len(created_at) >= 10:
            try:
                date_str = created_at[:10]
                volume_by_day[date_str] += 1
            except Exception:
                pass

        mv = row.get("motor_version", "")
        if mv:
            motor_version = mv

    avg_score = round(sum(scores) / len(scores), 4) if scores else 0.0
    avg_risk_glosa = round(sum(riscos) / len(riscos), 2) if riscos else 0.0

    sorted_days = sorted(volume_by_day.items())[-7:]
    volume_list = [{"date": d, "count": c} for d, c in sorted_days]

    return _cors(jsonify({
        "total_runs": total_runs,
        "gate_distribution": gate_distribution,
        "avg_score": avg_score,
        "avg_risk_glosa": avg_risk_glosa,
        "avg_tempo_ms": None,
        "volume_by_day": volume_list,
        "motor_version": motor_version,
    })), 200


# ─── PATCH /hub/runs/<run_id>/action ─────────────────────────────────────────
@hub_bp.patch("/runs/<run_id>/action")
@require_jwt
def patch_run_action(run_id: str):
    """
    Gate humano: grava hub_action em 21_DECISION_RUNS.
    Cria colunas hub_action / hub_nota / hub_updated_at se não existirem.
    Body: {"action": "APROVADO"|"SEGURADO"|"REVISAO", "nota": "string opcional"}
    """
    data = request.get_json(silent=True) or {}
    action = data.get("action", "").strip().upper()
    nota = str(data.get("nota", "") or "")

    if action not in ("APROVADO", "SEGURADO", "REVISAO"):
        return _cors(jsonify({
            "error": "invalid_action",
            "valid": ["APROVADO", "SEGURADO", "REVISAO"],
        })), 400

    try:
        ws = get_worksheet(_RUNS_SHEET)
        headers = get_header_row(ws, head=_HEAD)

        # Garantir colunas hub_* no header (linha _HEAD)
        hub_cols = ["hub_action", "hub_nota", "hub_updated_at"]
        added: list[str] = []
        for col in hub_cols:
            if col not in headers:
                next_col_idx = len(headers) + 1
                cell = _rowcol_to_a1(_HEAD, next_col_idx)
                ws.update_acell(cell, col)
                headers.append(col)
                added.append(col)
        if added:
            logger.info("hub PATCH: colunas adicionadas em %s: %s", _RUNS_SHEET, added)

        # Localizar linha do run
        row_idx, _ = find_row_by_col(ws, "decision_run_id", run_id, head=_HEAD)
        if row_idx is None:
            return _cors(jsonify({"error": "run_not_found", "run_id": run_id})), 404

        # Gravar ação humana
        now = datetime.now(timezone.utc).isoformat()
        user_email = getattr(request, "jwt_payload", {}).get("sub", "unknown")

        update_row_fields(ws, row_idx, headers, {
            "hub_action": action,
            "hub_nota": nota,
            "hub_updated_at": now,
        })

        logger.info("hub PATCH: run_id=%s action=%s by=%s", run_id, action, user_email)

        return _cors(jsonify({
            "ok": True,
            "run_id": run_id,
            "action": action,
            "updated_at": now,
            "by": user_email,
        })), 200

    except Exception as exc:
        logger.error("hub.patch_run_action: %s", exc)
        return _cors(jsonify({"error": "sheets_error", "detail": str(exc)})), 503
