"""
app/routers/hub.py
NEUROAUTH CONTROL HUB — FastAPI Router v1.0.0

Endpoints:
  GET  /hub/decision_runs        — lista 21_DECISION_RUNS
  GET  /hub/episodes             — lista 22_EPISODIOS
  GET  /hub/metrics              — métricas agregadas
  PATCH /hub/runs/{run_id}/action — gate humano

Auth: Bearer JWT via get_current_user (mesmo padrão de metrics.py)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import gspread
from fastapi import APIRouter, Depends, HTTPException, Query
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

from app.core.config import settings
from app.core.security import get_current_user

logger = logging.getLogger("neuroauth.hub")
router = APIRouter()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

TAB_RUNS = "21_DECISION_RUNS"
TAB_EPIS = "22_EPISODIOS"

# ── Sheets client ─────────────────────────────────────────────────────────────
def _get_sheet(tab: str) -> gspread.Worksheet:
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(settings.SPREADSHEET_ID)
    return ss.worksheet(tab)


def _safe_str(v) -> str:
    return (v or "").strip()


def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ── Models ────────────────────────────────────────────────────────────────────
class RunItem(BaseModel):
    decision_run_id: str
    episodio_id: str
    classification: str
    decision_status: str
    score: Optional[float]
    risk_level: str
    motor_version: str
    created_at: str
    hub_action: str


class EpisodeItem(BaseModel):
    episodio_id: str
    paciente: str
    profile_id: str
    convenio_id: str
    cid_principal: str
    decision_status: str
    score_confianca: Optional[float]
    created_at: str


class HubMetrics(BaseModel):
    total_runs: int
    gate_distribution: dict
    avg_score: Optional[float]
    avg_risk_score: Optional[float]
    volume_by_day: list
    motor_version: str


class ActionRequest(BaseModel):
    action: str
    nota: Optional[str] = ""


class ActionResponse(BaseModel):
    run_id: str
    action: str
    recorded: str


# ── GET /hub/decision_runs ────────────────────────────────────────────────────
@router.get("/decision_runs")
async def list_decision_runs(
    gate: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    user: dict = Depends(get_current_user),
):
    """Lista runs de 21_DECISION_RUNS, mais recentes primeiro."""
    try:
        ws = _get_sheet(TAB_RUNS)
        all_rows = ws.get_all_values()
    except Exception as e:
        logger.error("hub decision_runs: Sheets error: %s", e)
        raise HTTPException(status_code=503, detail=f"Sheets unavailable: {e}")

    items = []
    for row in all_rows:
        if not row or not _safe_str(row[0]).startswith("DR-"):
            continue
        run_id  = _safe_str(row[0])
        ep_id   = _safe_str(row[1]) if len(row) > 1 else ""
        ts      = _safe_str(row[2]) if len(row) > 2 else ""
        classif = _safe_str(row[3]) if len(row) > 3 else ""
        status  = _safe_str(row[4]) if len(row) > 4 else ""
        score   = _safe_float(row[5]) if len(row) > 5 else None
        risk    = _safe_str(row[6]) if len(row) > 6 else ""
        motor_v = _safe_str(row[7]) if len(row) > 7 else ""
        hub_act = _safe_str(row[8]) if len(row) > 8 else ""

        gate_val = classif or status
        if gate and gate.upper() not in gate_val.upper():
            continue

        items.append(RunItem(
            decision_run_id=run_id,
            episodio_id=ep_id,
            classification=classif,
            decision_status=status,
            score=score,
            risk_level=risk,
            motor_version=motor_v,
            created_at=ts,
            hub_action=hub_act,
        ))

    items.sort(key=lambda x: x.created_at, reverse=True)
    page = items[:limit]
    return {"total": len(items), "limit": limit, "items": [i.dict() for i in page]}


# ── GET /hub/episodes ─────────────────────────────────────────────────────────
@router.get("/episodes")
async def list_episodes(
    limit: int = Query(50, le=200),
    user: dict = Depends(get_current_user),
):
    """Lista episódios de 22_EPISODIOS via header discovery."""
    try:
        ws = _get_sheet(TAB_EPIS)
        all_rows = ws.get_all_values()
    except Exception as e:
        logger.error("hub episodes: Sheets error: %s", e)
        raise HTTPException(status_code=503, detail=f"Sheets unavailable: {e}")

    # Header discovery — skip até encontrar linha com "episodio_id"
    header_idx = None
    for i, row in enumerate(all_rows):
        if any("episodio_id" in str(c).lower() for c in row):
            header_idx = i
            break

    if header_idx is None:
        return {"total": 0, "limit": limit, "items": []}

    headers = [_safe_str(h).lower() for h in all_rows[header_idx]]
    data_rows = all_rows[header_idx + 1:]

    def col(row, name):
        try:
            idx = headers.index(name)
            return _safe_str(row[idx]) if idx < len(row) else ""
        except ValueError:
            return ""

    items = []
    for row in data_rows:
        ep_id = col(row, "episodio_id")
        if not ep_id:
            continue

        paciente = col(row, "paciente_id") or col(row, "nome_paciente")
        parts = paciente.strip().split()
        iniciais = (
            f"{parts[0][:2].upper()}.{parts[-1][:2].upper()}."
            if len(parts) >= 2 else parts[0][:3].upper() if parts else "??"
        )

        items.append(EpisodeItem(
            episodio_id=ep_id,
            paciente=iniciais,
            profile_id=col(row, "profile_id"),
            convenio_id=col(row, "convenio_id"),
            cid_principal=col(row, "cid_principal"),
            decision_status=col(row, "decision_status"),
            score_confianca=_safe_float(col(row, "score_confianca")),
            created_at=col(row, "created_at"),
        ))

    items.sort(key=lambda x: x.created_at, reverse=True)
    page = items[:limit]
    return {"total": len(items), "limit": limit, "items": [i.dict() for i in page]}


# ── GET /hub/metrics ──────────────────────────────────────────────────────────
@router.get("/metrics", response_model=HubMetrics)
async def hub_metrics(
    user: dict = Depends(get_current_user),
):
    """Métricas agregadas de 21_DECISION_RUNS para o cockpit."""
    try:
        ws = _get_sheet(TAB_RUNS)
        all_rows = ws.get_all_values()
    except Exception as e:
        logger.error("hub metrics: Sheets error: %s", e)
        raise HTTPException(status_code=503, detail=f"Sheets unavailable: {e}")

    total = 0
    gate_dist: dict = {}
    scores: list = []
    risks: list = []
    vbd: dict = {}
    motor_v = "?"
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    for row in all_rows:
        if not row or not _safe_str(row[0]).startswith("DR-"):
            continue
        total += 1
        classif = _safe_str(row[3]) if len(row) > 3 else "UNKNOWN"
        status  = _safe_str(row[4]) if len(row) > 4 else ""
        gate    = classif or status or "UNKNOWN"
        gate_dist[gate] = gate_dist.get(gate, 0) + 1

        sc = _safe_float(row[5]) if len(row) > 5 else None
        if sc is not None:
            scores.append(sc)

        risk_str = _safe_str(row[6]) if len(row) > 6 else ""
        risk_map = {"baixo": 10, "moderado": 35, "alto": 65, "crítico": 90}
        if risk_str.lower() in risk_map:
            risks.append(float(risk_map[risk_str.lower()]))

        mv = _safe_str(row[7]) if len(row) > 7 else ""
        if mv:
            motor_v = mv

        ts = _safe_str(row[2]) if len(row) > 2 else ""
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt >= cutoff:
                    day = dt.strftime("%Y-%m-%d")
                    vbd[day] = vbd.get(day, 0) + 1
            except Exception:
                pass

    series = [
        {"date": (datetime.now(timezone.utc) - timedelta(days=6 - i)).strftime("%Y-%m-%d"),
         "count": vbd.get((datetime.now(timezone.utc) - timedelta(days=6 - i)).strftime("%Y-%m-%d"), 0)}
        for i in range(7)
    ]

    return HubMetrics(
        total_runs=total,
        gate_distribution=gate_dist,
        avg_score=round(sum(scores) / len(scores), 4) if scores else None,
        avg_risk_score=round(sum(risks) / len(risks), 1) if risks else None,
        volume_by_day=series,
        motor_version=motor_v,
    )


# ── PATCH /hub/runs/{run_id}/action ──────────────────────────────────────────
@router.patch("/runs/{run_id}/action", response_model=ActionResponse)
async def hub_action(
    run_id: str,
    body: ActionRequest,
    user: dict = Depends(get_current_user),
):
    """Gate humano: APROVADO | SEGURADO | REVISAO"""
    VALID = {"APROVADO", "SEGURADO", "REVISAO"}
    action = body.action.upper().strip()
    if action not in VALID:
        raise HTTPException(status_code=400, detail=f"action inválida: {action}. Use: {sorted(VALID)}")

    try:
        ws = _get_sheet(TAB_RUNS)
        all_rows = ws.get_all_values()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sheets unavailable: {e}")

    # Encontrar linha do run_id
    row_idx = None
    for i, row in enumerate(all_rows):
        if row and _safe_str(row[0]) == run_id:
            row_idx = i + 1  # gspread 1-indexed
            break

    if row_idx is None:
        raise HTTPException(status_code=404, detail=f"run_id '{run_id}' não encontrado")

    now = datetime.now(timezone.utc).isoformat()
    val = f"{action} | {now}" + (f" | {body.nota}" if body.nota else "")

    # Gravar na coluna 9 (índice 8, 0-based) = hub_action
    # Se a coluna não existir ainda, o valor será escrito na posição correta
    try:
        ws.update_cell(row_idx, 9, val)  # col 9 = hub_action
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Falha ao gravar: {e}")

    logger.info("hub_action: run_id=%s action=%s user=%s", run_id, action, user.get("email", ""))
    return ActionResponse(run_id=run_id, action=action, recorded=now)
