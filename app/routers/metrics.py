"""
app/routers/metrics.py
GET /metrics — painel operacional do NEUROAUTH

Lê 21_DECISION_RUNS e retorna métricas para o painel.
Design: leitura única, resposta < 1s, nunca quebra o painel.
Campos ausentes retornam arrays vazios ou zero — nunca 500.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import gspread
from fastapi import APIRouter, Depends
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

from app.core.config import settings
from app.core.security import get_current_user

logger = logging.getLogger("neuroauth.metrics")
router = APIRouter()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Índices de colunas em 21_DECISION_RUNS (0-based, linha real começa em índice 4)
# decision_run_id | episodio_id | timestamp | classification | decision_status
# score | risco_glosa | justificativa | ...
COL_RUN_ID     = 0
COL_EPISODE_ID = 1
COL_TIMESTAMP  = 2
COL_CLASS      = 3
COL_STATUS     = 4
COL_SCORE      = 5
COL_RISCO      = 6


# ── MODELS ────────────────────────────────────────────────────────────────────

class CaseItem(BaseModel):
    episode_id: str
    decision_run_id: str
    classification: str
    score: Optional[int] = None
    risk_level: str
    decision_status: str
    updated_at: str


class ClassDist(BaseModel):
    label: str
    value: int


class ScoreItem(BaseModel):
    episode_id: str
    score: int
    classification: str
    updated_at: str


class MetricsSummary(BaseModel):
    total: int
    go: int
    go_com_ressalvas: int
    no_go: int
    risco_alto: int
    risco_critico: int


class MetricsResponse(BaseModel):
    generated_at: str
    summary: MetricsSummary
    classification_distribution: list[ClassDist]
    recent_scores: list[ScoreItem]
    recent_cases: list[CaseItem]
    recent_errors: list[dict]


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _safe_int(val: str) -> Optional[int]:
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_str(val: str) -> str:
    return (val or "").strip()


def _get_runs_sheet() -> gspread.Worksheet:
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(settings.SPREADSHEET_ID)
    return ss.worksheet("21_DECISION_RUNS")


def _parse_rows(all_rows: list) -> list[dict]:
    """
    Converte linhas brutas em dicts normalizados.
    Ignora linhas de cabeçalho/instrução (linhas 1-4).
    Dados válidos: row[COL_RUN_ID] começa com 'DR-'
    """
    parsed = []
    for row in all_rows:
        if not row or not _safe_str(row[0]).startswith("DR-"):
            continue
        parsed.append({
            "decision_run_id": _safe_str(row[COL_RUN_ID]),
            "episode_id":      _safe_str(row[COL_EPISODE_ID]) if len(row) > COL_EPISODE_ID else "",
            "updated_at":      _safe_str(row[COL_TIMESTAMP])  if len(row) > COL_TIMESTAMP  else "",
            "classification":  _safe_str(row[COL_CLASS])      if len(row) > COL_CLASS      else "",
            "decision_status": _safe_str(row[COL_STATUS])     if len(row) > COL_STATUS     else "",
            "score":           _safe_int(row[COL_SCORE])      if len(row) > COL_SCORE      else None,
            "risk_level":      _safe_str(row[COL_RISCO])      if len(row) > COL_RISCO      else "",
        })
    return parsed


# ── ENDPOINT ──────────────────────────────────────────────────────────────────

@router.get("", response_model=MetricsResponse)
async def get_metrics(
    limit: int = 20,
    user: dict = Depends(get_current_user),
):
    """
    Retorna métricas operacionais do painel NEUROAUTH.
    - Leitura única de 21_DECISION_RUNS
    - Nunca retorna 500 por campo ausente
    - Resposta alvo < 1s
    """
    generated_at = datetime.now(timezone.utc).isoformat()

    try:
        ws = _get_runs_sheet()
        all_rows = ws.get_all_values()
    except Exception as e:
        logger.error("metrics: falha ao ler Sheets: %s", e)
        # Retornar resposta vazia mas válida — não quebrar o painel
        return MetricsResponse(
            generated_at=generated_at,
            summary=MetricsSummary(total=0,go=0,go_com_ressalvas=0,no_go=0,risco_alto=0,risco_critico=0),
            classification_distribution=[],
            recent_scores=[],
            recent_cases=[],
            recent_errors=[{"error": str(e), "at": generated_at}],
        )

    rows = _parse_rows(all_rows)

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    total     = len(rows)
    go        = sum(1 for r in rows if r["classification"] == "GO")
    go_res    = sum(1 for r in rows if r["classification"] == "GO_COM_RESSALVAS")
    no_go     = sum(1 for r in rows if r["classification"] == "NO_GO")
    r_alto    = sum(1 for r in rows if r["risk_level"] == "alto")
    r_critico = sum(1 for r in rows if r["risk_level"] == "crítico")

    summary = MetricsSummary(
        total=total,
        go=go,
        go_com_ressalvas=go_res,
        no_go=no_go,
        risco_alto=r_alto,
        risco_critico=r_critico,
    )

    # ── DISTRIBUIÇÃO ──────────────────────────────────────────────────────────
    distribution = [
        ClassDist(label="GO",                value=go),
        ClassDist(label="GO_COM_RESSALVAS",  value=go_res),
        ClassDist(label="NO_GO",             value=no_go),
    ]

    # ── CASOS RECENTES (últimos N, ordem inversa) ─────────────────────────────
    recent = rows[-limit:] if len(rows) >= limit else rows
    recent_reversed = list(reversed(recent))

    recent_cases = [
        CaseItem(
            episode_id=r["episode_id"],
            decision_run_id=r["decision_run_id"],
            classification=r["classification"],
            score=r["score"],
            risk_level=r["risk_level"],
            decision_status=r["decision_status"],
            updated_at=r["updated_at"],
        )
        for r in recent_reversed
    ]

    # ── SÉRIE DE SCORES ───────────────────────────────────────────────────────
    recent_scores = [
        ScoreItem(
            episode_id=r["episode_id"],
            score=r["score"] if r["score"] is not None else 0,
            classification=r["classification"],
            updated_at=r["updated_at"],
        )
        for r in recent_reversed
        if r["score"] is not None
    ]

    logger.info(
        "metrics: total=%d go=%d res=%d no_go=%d",
        total, go, go_res, no_go,
    )

    return MetricsResponse(
        generated_at=generated_at,
        summary=summary,
        classification_distribution=distribution,
        recent_scores=recent_scores,
        recent_cases=recent_cases,
        recent_errors=[],
    )
