"""
app/routers/hub.py
NEUROAUTH CONTROL HUB — FastAPI router
Versão: 1.0.0

Endpoints:
  GET   /hub/metrics         — métricas agregadas de 21_DECISION_RUNS
  GET   /hub/decision_runs   — lista 21_DECISION_RUNS (filtros opcionais)
  GET   /hub/episodes        — lista 22_EPISODIOS (filtros opcionais)
  PATCH /hub/runs/{run_id}/action — gate humano: APROVADO | SEGURADO | REVISAO

Auth: JWT via get_current_user (mesmo padrão do sistema)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import gspread
from fastapi import APIRouter, Depends, HTTPException, Query
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

from app.core.config import settings
from app.core.security import get_current_user

logger = logging.getLogger("neuroauth.hub")
router = APIRouter()

_RUNS_SHEET = "21_DECISION_RUNS"
_EPIS_SHEET = "22_EPISODIOS"
_HEAD       = 3

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ── Sheets helpers ─────────────────────────────────────────────────────────────

def _get_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    return gspread.authorize(creds)


def _read_sheet_as_dicts(sheet_name: str, head: int = _HEAD) -> list[dict]:
    gc = _get_client()
    ss = gc.open_by_key(settings.SPREADSHEET_ID)
    ws = ss.worksheet(sheet_name)
    all_values = ws.get_all_values()
    if len(all_values) <= head:
        return []
    headers = [str(h).strip() for h in all_values[head - 1]]
    rows = all_values[head:]
    result = []
    for row in rows:
        if not any(str(c).strip() for c in row):
            continue
        d = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        result.append(d)
    return result


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val) if val not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _safe_json(val: Any) -> list:
    try:
        return json.loads(val) if val else []
    except Exception:
        return []


# ── Models ─────────────────────────────────────────────────────────────────────

class HubActionRequest(BaseModel):
    action: str
    nota: Optional[str] = ""


# ── GET /hub/metrics ───────────────────────────────────────────────────────────

@router.get("/metrics")
async def hub_metrics(user: dict = Depends(get_current_user)):
    """Métricas agregadas de 21_DECISION_RUNS para o cockpit."""
    try:
        runs_rows = _read_sheet_as_dicts(_RUNS_SHEET)
    except Exception as exc:
        logger.error("hub_metrics: Sheets error: %s", exc)
        raise HTTPException(status_code=503, detail=f"Falha ao ler planilha: {exc}")

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
        sc = _safe_float(r.get("score_final") or r.get("score_clinico"))
        if sc is not None:
            scores.append(sc)
        alertas_list = _safe_json(r.get("alertas_json", "[]"))
        risco_glosas.append(min(len(alertas_list) * 15, 100))
        motor_version = r.get("motor_version", motor_version) or motor_version
        created = r.get("created_at", "")
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                if dt >= cutoff:
                    day = dt.strftime("%Y-%m-%d")
                    volume_by_day[day] = volume_by_day.get(day, 0) + 1
            except Exception:
                pass

    daily_series = [
        {
            "date": (datetime.now(timezone.utc) - timedelta(days=6 - i)).strftime("%Y-%m-%d"),
            "count": volume_by_day.get(
                (datetime.now(timezone.utc) - timedelta(days=6 - i)).strftime("%Y-%m-%d"), 0
            ),
        }
        for i in range(7)
    ]

    return {
        "total_runs":        total,
        "gate_distribution": gate_dist,
        "avg_score":         round(sum(scores) / len(scores), 4) if scores else None,
        "avg_risco_glosa":   round(sum(risco_glosas) / len(risco_glosas), 1) if risco_glosas else None,
        "volume_by_day":     daily_series,
        "motor_version":     motor_version,
    }


# ── GET /hub/decision_runs ─────────────────────────────────────────────────────

@router.get("/decision_runs")
async def list_decision_runs(
    gate: str = Query(""),
    profile_id: str = Query(""),
    limit: int = Query(50, le=200),
    user: dict = Depends(get_current_user),
):
    """Lista runs de 21_DECISION_RUNS com filtros opcionais."""
    try:
        rows = _read_sheet_as_dicts(_RUNS_SHEET)
    except Exception as exc:
        logger.error("list_decision_runs: Sheets error: %s", exc)
        raise HTTPException(status_code=503, detail=f"Falha ao ler planilha: {exc}")

    runs = []
    for r in rows:
        run_id = r.get("decision_run_id", "")
        if not run_id:
            continue
        r_gate    = r.get("decision_status", r.get("gate", ""))
        r_profile = r.get("profile_id", "")
        if gate and gate.upper() not in r_gate.upper():
            continue
        if profile_id and profile_id.upper() not in r_profile.upper():
            continue
        alertas_list = _safe_json(r.get("alertas_json", "[]"))
        runs.append({
            "decision_run_id": run_id,
            "episodio_id":     r.get("episodio_id", ""),
            "profile_id":      r_profile,
            "gate":            r_gate,
            "score":           _safe_float(r.get("score_final") or r.get("score_clinico")),
            "risco_glosa":     min(len(alertas_list) * 15, 100),
            "motor_version":   r.get("motor_version", ""),
            "created_at":      r.get("created_at", ""),
            "hub_action":      r.get("hub_action", ""),
            "alertas":         alertas_list,
            "bloqueios":       _safe_json(r.get("bloqueios_json", "[]")),
        })

    runs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return {"total": len(runs[:limit]), "limit": limit, "items": runs[:limit]}


# ── GET /hub/episodes ──────────────────────────────────────────────────────────

@router.get("/episodes")
async def list_episodes(
    status: str = Query(""),
    convenio_id: str = Query(""),
    limit: int = Query(50, le=200),
    user: dict = Depends(get_current_user),
):
    """Lista episódios de 22_EPISODIOS com filtros opcionais."""
    try:
        rows = _read_sheet_as_dicts(_EPIS_SHEET)
    except Exception as exc:
        logger.error("list_episodes: Sheets error: %s", exc)
        raise HTTPException(status_code=503, detail=f"Falha ao ler planilha: {exc}")

    episodes = []
    for r in rows:
        ep_id = r.get("episodio_id", "")
        if not ep_id:
            continue
        r_status   = r.get("decision_status", "")
        r_convenio = r.get("convenio_id", "")
        if status and status.upper() not in r_status.upper():
            continue
        if convenio_id and convenio_id.upper() not in r_convenio.upper():
            continue
        paciente = r.get("paciente_id", "")
        parts    = paciente.strip().split()
        iniciais = (
            f"{parts[0][:2].upper()}.{parts[-1][:2].upper()}."
            if len(parts) >= 2 else (parts[0][:3].upper() if parts else "??")
        )
        procedimento = ""
        try:
            ctx = json.loads(r.get("clinical_context_json", "{}") or "{}")
            procedimento = ctx.get("procedimento", "") or ctx.get("indicacao_clinica", "")
        except Exception:
            pass
        episodes.append({
            "episodio_id":        ep_id,
            "paciente":           iniciais,
            "profile_id":         r.get("profile_id", ""),
            "convenio_id":        r_convenio,
            "cid_principal":      r.get("cid_principal", ""),
            "procedimento":       (procedimento or "")[:80],
            "decision_status":    r_status,
            "status_operacional": r.get("status_operacional", ""),
            "decision_run_id":    r.get("decision_run_id", ""),
            "score_confianca":    _safe_float(r.get("score_confianca")),
            "created_at":         r.get("created_at", ""),
        })

    episodes.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return {"total": len(episodes[:limit]), "limit": limit, "items": episodes[:limit]}
