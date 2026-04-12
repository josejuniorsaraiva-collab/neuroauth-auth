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


def _parse_date(val: str) -> str:
    """Normaliza datas para ISO — aceita ISO8601, DD/MM/YYYY HH:MM e variantes."""
    if not val or val in ("[]", "{}"):
        return ""
    val = val.strip()
    # já ISO
    try:
        datetime.fromisoformat(val.replace("Z", "+00:00"))
        return val
    except Exception:
        pass
    # DD/MM/YYYY HH:MM  ou  DD/MM/YYYY
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(val, fmt).isoformat()
        except Exception:
            pass
    return ""


def _normalize_run(r: dict) -> dict:
    """
    Detecta o schema do run pelo prefixo do decision_run_id e retorna
    um dict normalizado com chaves canônicas.

    Schemas conhecidos:
      RUN_*  — schema v2.0.0 (colunas alinhadas com o header)
      DR-*   — schema legado (colunas deslocadas)
      outros — tenta RUN_ primeiro, cai no DR- como fallback
    """
    run_id = r.get("decision_run_id", "")

    if run_id.startswith("RUN_"):
        # Schema moderno — colunas corretas
        alertas  = _safe_json(r.get("alertas_json",  "[]"))
        bloqueios = _safe_json(r.get("bloqueios_json", "[]"))
        score_raw = r.get("score_final", "")
        score = _safe_float(score_raw)
        # score 0.0-1.0 → converte para 0-100
        if score is not None and score <= 1.0:
            score = round(score * 100, 1)
        return {
            "decision_run_id": run_id,
            "episodio_id":     r.get("episodio_id", ""),
            "profile_id":      r.get("profile_id", ""),
            "gate":            "",   # v2 não tem gate direto — deixa vazio por enquanto
            "score":           score,
            "risco_glosa":     min(len(alertas) * 15, 100),
            "motor_version":   r.get("motor_version", ""),
            "created_at":      _parse_date(r.get("created_at", "")),
            "hub_action":      r.get("hub_action", ""),
            "alertas":         alertas,
            "bloqueios":       bloqueios,
        }
    else:
        # Schema legado DR- (colunas deslocadas)
        # col2=created_at, col3=gate, col4=hub_action, col5=score(int),
        # col6=risco_texto, col7=resumo_clinico, col8=alertas_glosa, col9=alertas_extra
        gate      = r.get("input_context_json", "")
        score_raw = r.get("opcao_escolhida_json", "")
        score_int = _safe_float(score_raw)
        risco_txt = r.get("score_final", "")
        risco_map = {"baixo": 10, "moderado": 40, "alto": 75}
        risco_num = risco_map.get(risco_txt.lower(), 0) if risco_txt else 0
        alerta1   = r.get("bloqueios_json", "")
        alerta2   = r.get("motor_version", "")
        alertas_txt = " | ".join(a for a in [alerta1, alerta2] if a)
        return {
            "decision_run_id": run_id,
            "episodio_id":     r.get("episodio_id", ""),
            "profile_id":      "",
            "gate":            gate,
            "score":           score_int,
            "risco_glosa":     risco_num,
            "motor_version":   "legado",
            "created_at":      _parse_date(r.get("profile_id", "")),
            "hub_action":      r.get("opcoes_geradas_json", ""),
            "alertas":         [alertas_txt] if alertas_txt else [],
            "bloqueios":       [],
        }


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
        n = _normalize_run(r)
        total += 1
        gate = n["gate"] or "SEM_GATE"
        gate_dist[gate] = gate_dist.get(gate, 0) + 1
        if n["score"] is not None:
            scores.append(n["score"])
        risco_glosas.append(n["risco_glosa"])
        if n["motor_version"] and n["motor_version"] != "legado":
            motor_version = n["motor_version"]
        created = n["created_at"]
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
        n = _normalize_run(r)
        if gate and gate.upper() not in n["gate"].upper():
            continue
        if profile_id and profile_id.upper() not in n["profile_id"].upper():
            continue
        runs.append(n)

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
