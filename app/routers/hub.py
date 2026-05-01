"""
app/routers/hub.py
NEUROAUTH CONTROL HUB — FastAPI Router v1.1.0

Endpoints:
  GET  /hub/decision_runs            — lista 21_DECISION_RUNS
  GET  /hub/episodes                 — lista 22_EPISODIOS
  GET  /hub/metrics                  — métricas agregadas
  PATCH /hub/runs/{run_id}/action    — gate humano
  GET  /hub/casos                    — lista 22_EPISODIOS com filtros de cirurgião
  GET  /hub/producao                 — linhas da aba PRODUCAO com filtros opcionais
  POST /hub/casos/{caso_id}/surgeons — atribui equipe cirúrgica + calcula produção

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
from app.models.surgeon import CirurgiaoPayload
from app.services.surgeon_validator import validate_cirurgiao
from app.services.surgeon_producao import calcular_producao, gravar_producao
from repositories.sheets_client import (
    get_worksheet as sc_get_worksheet,
    read_all_records as sc_read_all_records,
    find_row_by_col as sc_find_row_by_col,
    update_row_fields as sc_update_row_fields,
    get_header_row as sc_get_header_row,
)

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


# ═════════════════════════════════════════════════════════════════════════════
# SURGEON ATTRIBUTION — v1.1.0
# ═════════════════════════════════════════════════════════════════════════════

TAB_EPIS_SC  = "22_EPISODIOS"   # alias para sheets_client functions
TAB_PRODUCAO = "PRODUCAO"
_SC_HEAD     = 3                # head=3 é o padrão de 22_EPISODIOS e novas abas


# ── GET /hub/casos ────────────────────────────────────────────────────────────
@router.get("/casos")
async def listar_casos(
    cirurgiao_principal: Optional[str] = Query(None, description="Filtrar por ID do cirurgião principal"),
    cirurgiao_auxiliar: Optional[str]  = Query(None, description="Filtrar por ID de qualquer auxiliar"),
    papel: Optional[str]               = Query(None, description="'principal' | 'auxiliar' — filtra por papel"),
    ordem_auxiliar: Optional[int]      = Query(None, description="Filtra auxiliar de ordem específica (1-3)"),
    limit: int                         = Query(50, le=200),
    user: dict = Depends(get_current_user),
):
    """
    Lista episódios (22_EPISODIOS) com filtros de equipe cirúrgica.
    Os campos cirurgiao_principal_id e cirurgioes_auxiliares devem existir
    na planilha 22_EPISODIOS para que os filtros funcionem.
    Sem filtros → comportamento idêntico a /hub/episodes.
    """
    try:
        ws   = sc_get_worksheet(TAB_EPIS_SC)
        rows = sc_read_all_records(ws, head=_SC_HEAD)
    except Exception as e:
        logger.error("hub /casos: Sheets error: %s", e)
        raise HTTPException(status_code=503, detail=f"Sheets unavailable: {e}")

    filtered = []
    for row in rows:
        ep_id = row.get("episodio_id", "")
        if not ep_id:
            continue

        cir_princ = (row.get("cirurgiao_principal_id") or "").strip()
        cir_aux_raw = (row.get("cirurgioes_auxiliares") or "").strip()

        # Aplicar filtros
        if cirurgiao_principal and cir_princ.upper() != cirurgiao_principal.upper():
            if papel and papel.lower() == "principal":
                continue
            if not papel:
                continue

        if cirurgiao_auxiliar:
            aux_match = cirurgiao_auxiliar.upper() in cir_aux_raw.upper()
            if papel and papel.lower() == "auxiliar" and not aux_match:
                continue
            if not papel and not aux_match and cir_princ.upper() != cirurgiao_auxiliar.upper():
                continue

        if papel:
            p = papel.lower()
            if p == "principal" and cir_princ.upper() != (cirurgiao_principal or cir_princ).upper():
                continue
            if p == "auxiliar" and not cir_aux_raw:
                continue

        if ordem_auxiliar is not None:
            # Auxiliares armazenados como "CIR_002(1);CIR_003(2)" ou JSON
            if f"({ordem_auxiliar})" not in cir_aux_raw and f'"ordem": {ordem_auxiliar}' not in cir_aux_raw:
                continue

        # Anonimizar paciente
        paciente = (row.get("paciente_id") or row.get("nome_paciente") or "").strip()
        parts = paciente.split()
        iniciais = (
            f"{parts[0][:2].upper()}.{parts[-1][:2].upper()}."
            if len(parts) >= 2 else parts[0][:3].upper() if parts else "??"
        )

        filtered.append({
            "episodio_id":            ep_id,
            "paciente":               iniciais,
            "convenio_id":            row.get("convenio_id", ""),
            "cid_principal":          row.get("cid_principal", ""),
            "decision_status":        row.get("decision_status", ""),
            "cirurgiao_principal_id": cir_princ,
            "cirurgioes_auxiliares":  cir_aux_raw,
            "created_at":             row.get("created_at", ""),
            "data_procedimento":      row.get("data_procedimento", ""),
        })

    filtered.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    page = filtered[:limit]
    return {"total": len(filtered), "limit": limit, "items": page}


# ── GET /hub/producao ─────────────────────────────────────────────────────────
@router.get("/producao")
async def producao_por_cirurgiao(
    cirurgiao_id: Optional[str]  = Query(None, description="Filtrar por ID do cirurgião"),
    papel: Optional[str]         = Query(None, description="PRINCIPAL | AUXILIAR_1 | AUXILIAR_2 | AUXILIAR_3"),
    data_inicio: Optional[str]   = Query(None, description="Data início ISO (YYYY-MM-DD)"),
    data_fim: Optional[str]      = Query(None, description="Data fim ISO (YYYY-MM-DD)"),
    caso_id: Optional[str]       = Query(None, description="Filtrar por caso/episódio específico"),
    limit: int                   = Query(100, le=500),
    user: dict = Depends(get_current_user),
):
    """
    Retorna linhas da aba PRODUCAO com filtros opcionais.
    Cada linha representa a produção de um cirurgião em um caso específico.
    """
    try:
        ws   = sc_get_worksheet(TAB_PRODUCAO)
        rows = sc_read_all_records(ws, head=_SC_HEAD)
    except Exception as e:
        logger.error("hub /producao: Sheets error: %s", e)
        raise HTTPException(status_code=503, detail=f"Sheets unavailable: {e}")

    filtered = []
    for row in rows:
        # Filtros
        if cirurgiao_id and (row.get("cirurgiao_id") or "").upper() != cirurgiao_id.upper():
            continue
        if papel and (row.get("papel") or "").upper() != papel.upper():
            continue
        if caso_id and (row.get("caso_id") or "") != caso_id:
            continue

        dp = row.get("data_procedimento", "")
        if data_inicio and dp and dp < data_inicio:
            continue
        if data_fim and dp and dp > data_fim:
            continue

        filtered.append({
            "caso_id":             row.get("caso_id", ""),
            "cirurgiao_id":        row.get("cirurgiao_id", ""),
            "papel":               row.get("papel", ""),
            "ordem_auxiliar":      row.get("ordem_auxiliar", ""),
            "valor_base":          _safe_float(row.get("valor_base")),
            "percentual_aplicado": _safe_float(row.get("percentual_aplicado")),
            "valor_calculado":     _safe_float(row.get("valor_calculado")),
            "data_procedimento":   dp,
            "operadora":           row.get("operadora", ""),
            "status_autorizacao":  row.get("status_autorizacao", ""),
            "status_pagamento":    row.get("status_pagamento", ""),
        })

    # Somar totais
    total_calculado = sum(
        r["valor_calculado"] for r in filtered if r["valor_calculado"] is not None
    )

    filtered.sort(key=lambda x: x.get("data_procedimento", ""), reverse=True)
    page = filtered[:limit]

    return {
        "total":             len(filtered),
        "limit":             limit,
        "total_calculado":   round(total_calculado, 2),
        "items":             page,
    }


# ── POST /hub/casos/{caso_id}/surgeons ───────────────────────────────────────
class SurgeonAssignRequest(BaseModel):
    """Payload de atribuição de equipe cirúrgica + dados de produção."""
    equipe:           CirurgiaoPayload
    valor_base:       float = 0.0
    operadora:        str   = ""
    porte:            str   = "TODOS"
    data_procedimento: Optional[str] = None


class SurgeonAssignResponse(BaseModel):
    ok:                 bool
    pendencias:         list
    principal_nome:     str
    auxiliares_display: str
    producao:           list
    gravado:            bool


@router.post("/casos/{caso_id}/surgeons", response_model=SurgeonAssignResponse)
async def atribuir_equipe(
    caso_id: str,
    body: SurgeonAssignRequest,
    user: dict = Depends(get_current_user),
):
    """
    Atribui equipe cirúrgica a um caso e calcula produção paralela.

    1. Valida principal + auxiliares contra CIRURGIOES.
    2. Grava cirurgiao_principal_id e cirurgioes_auxiliares em 22_EPISODIOS.
    3. Calcula produção por papel (PRINCIPAL + AUXILIAR_1..3).
    4. Grava linhas de produção em PRODUCAO.
    """
    equipe_dict = body.equipe.to_dict()

    # 1. Validar equipe
    validacao = validate_cirurgiao(equipe_dict)
    if not validacao["ok"]:
        logger.warning(
            "atribuir_equipe: caso=%s validação falhou: %s",
            caso_id, validacao["pendencias"],
        )
        return SurgeonAssignResponse(
            ok=False,
            pendencias=validacao["pendencias"],
            principal_nome=validacao.get("principal_nome", ""),
            auxiliares_display=validacao.get("auxiliares_display", ""),
            producao=[],
            gravado=False,
        )

    # 2. Gravar em 22_EPISODIOS (se a aba tiver essas colunas)
    try:
        ws_ep = sc_get_worksheet(TAB_EPIS_SC)
        row_idx, _ = sc_find_row_by_col(ws_ep, "episodio_id", caso_id, head=_SC_HEAD)
        if row_idx is not None:
            header = sc_get_header_row(ws_ep, head=_SC_HEAD)
            sc_update_row_fields(ws_ep, row_idx, header, {
                "cirurgiao_principal_id": equipe_dict["cirurgiao_principal"],
                "cirurgioes_auxiliares":  str(equipe_dict["cirurgioes_auxiliares"]),
            })
            logger.info("atribuir_equipe: 22_EPISODIOS atualizado para caso=%s", caso_id)
        else:
            logger.warning(
                "atribuir_equipe: caso '%s' não encontrado em 22_EPISODIOS — apenas produção gravada",
                caso_id,
            )
    except Exception as exc:
        logger.warning("atribuir_equipe: falha ao atualizar 22_EPISODIOS: %s — continuando", exc)

    # 3. Calcular produção
    linhas = calcular_producao(
        caso_id=caso_id,
        valor_base=body.valor_base,
        operadora=body.operadora,
        porte=body.porte,
        principal_id=equipe_dict["cirurgiao_principal"],
        auxiliares=equipe_dict["cirurgioes_auxiliares"],
        data_proc=body.data_procedimento,
    )

    # 4. Gravar PRODUCAO
    gravado = gravar_producao(linhas)

    logger.info(
        "atribuir_equipe: caso=%s equipe=%s+%d aux gravado=%s",
        caso_id,
        equipe_dict["cirurgiao_principal"],
        len(equipe_dict["cirurgioes_auxiliares"]),
        gravado,
    )

    return SurgeonAssignResponse(
        ok=True,
        pendencias=[],
        principal_nome=validacao["principal_nome"],
        auxiliares_display=validacao["auxiliares_display"],
        producao=linhas,
        gravado=gravado,
    )
