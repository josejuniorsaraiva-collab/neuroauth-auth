"""
app/routers/audit.py
GET /audit/query — reconstrução de caso para auditoria.
Busca por decision_run_id ou episodio_id.
Requer JWT válido (founder only).
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from app.core.security import require_authorized
from app.core.config import settings
from app.services.sheets_store import (
    _get_client,
    TAB_DECISION_RUNS,
    TAB_EPISODIOS,
    EPISODIOS_HEADER_ROW,
)

router = APIRouter()
logger = logging.getLogger("neuroauth.audit")


@router.get("/query")
async def audit_query(
    decision_run_id: str = Query(None, description="DR-XXXXXXXX"),
    episode_id: str = Query(None, alias="episodio_id", description="EP-XXXXXXXX-YYYYYY"),
    user: dict = Depends(require_authorized),
):
    """
    Reconstrução completa de caso para auditoria.
    Busca em 21_DECISION_RUNS e 22_EPISODIOS.
    Retorna todos os campos disponíveis para reconstruir 100% do caso.
    """
    if not decision_run_id and not episode_id:
        raise HTTPException(
            status_code=400,
            detail="Informe decision_run_id ou episodio_id.",
        )

    try:
        gc = _get_client()
        ss = gc.open_by_key(settings.SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"[audit] Sheets connection error: {e}")
        raise HTTPException(status_code=502, detail="Erro ao conectar com Sheets.")

    result = {
        "query": {
            "decision_run_id": decision_run_id,
            "episode_id": episode_id,
        },
        "decision_runs": [],
        "episodio": None,
        "integridade": {
            "encontrado_21": False,
            "encontrado_22": False,
            "correlacao_ok": False,
        },
    }

    # ── 1. Buscar em 21_DECISION_RUNS ────────────────────────────
    try:
        ws_runs = ss.worksheet(TAB_DECISION_RUNS)
        runs_data = ws_runs.get_all_values()

        if len(runs_data) > 0:
            # Encontrar header row dinamicamente — busca linha com "decision_run_id"
            runs_header = None
            header_idx = 0
            for idx, row in enumerate(runs_data):
                if any(cell.strip().lower() == "decision_run_id" for cell in row):
                    runs_header = [c.strip().lower() for c in row]
                    header_idx = idx
                    break

            if runs_header is None:
                # Fallback: usar índices fixos (mesma lógica do metrics.py)
                logger.warning("[audit] Header 'decision_run_id' não encontrado em 21_DECISION_RUNS, usando índices fixos")
                FIXED_COLS = ["decision_run_id", "episodio_id", "timestamp",
                              "classification", "decision_status", "score",
                              "risco_glosa", "justificativa", "pendencias",
                              "pontos_frageis", "cid_principal", "procedimento",
                              "convenio", "crm_solicitante"]
                for row in runs_data:
                    if not row or not row[0].strip().startswith("DR-"):
                        continue
                    row_dict = dict(zip(FIXED_COLS, row))
                    match = False
                    if decision_run_id and row_dict.get("decision_run_id") == decision_run_id:
                        match = True
                    if episode_id and row_dict.get("episodio_id") == episode_id:
                        match = True
                    if match:
                        if row_dict.get("score"):
                            try:
                                row_dict["score"] = int(float(row_dict["score"]))
                            except (ValueError, TypeError):
                                pass
                        result["decision_runs"].append(row_dict)
            else:
                for row in runs_data[header_idx + 1:]:
                    row_dict = dict(zip(runs_header, row))
                    match = False
                    if decision_run_id and row_dict.get("decision_run_id") == decision_run_id:
                        match = True
                    if episode_id and row_dict.get("episodio_id") == episode_id:
                        match = True
                    if match:
                        if row_dict.get("score"):
                            try:
                                row_dict["score"] = int(float(row_dict["score"]))
                            except (ValueError, TypeError):
                                pass
                        result["decision_runs"].append(row_dict)

        result["integridade"]["encontrado_21"] = len(result["decision_runs"]) > 0
    except Exception as e:
        logger.error(f"[audit] 21_DECISION_RUNS read error: {e}")
        result["decision_runs_error"] = str(e)[:200]

    # ── 2. Buscar em 22_EPISODIOS ────────────────────────────────
    try:
        ws_ep = ss.worksheet(TAB_EPISODIOS)
        header_row = ws_ep.row_values(EPISODIOS_HEADER_ROW)
        col_map = {
            name.strip().lower(): idx
            for idx, name in enumerate(header_row)
            if name.strip()
        }

        # Determinar qual ID buscar
        search_id = episode_id
        search_col = "episodio_id"

        # Se não temos episode_id, derivar dos decision_runs
        if not search_id and result["decision_runs"]:
            search_id = result["decision_runs"][0].get("episodio_id")

        if search_id and search_col in col_map:
            col_idx = col_map[search_col]
            all_data = ws_ep.get_all_values()
            for row in all_data[EPISODIOS_HEADER_ROW:]:  # skip header rows
                if len(row) > col_idx and row[col_idx] == search_id:
                    ep_dict = {
                        header_row[i].strip().lower(): row[i]
                        for i in range(min(len(header_row), len(row)))
                        if header_row[i].strip()
                    }
                    result["episodio"] = ep_dict
                    result["integridade"]["encontrado_22"] = True
                    break

    except Exception as e:
        logger.error(f"[audit] 22_EPISODIOS read error: {e}")
        result["episodio_error"] = str(e)[:200]

    # ── 3. Verificar correlação ──────────────────────────────────
    if result["decision_runs"] and result["episodio"]:
        ep_run_id = result["episodio"].get("decision_run_id", "")
        latest_run_id = result["decision_runs"][-1].get("decision_run_id", "")
        result["integridade"]["correlacao_ok"] = ep_run_id == latest_run_id
        result["integridade"]["run_id_21"] = latest_run_id
        result["integridade"]["run_id_22"] = ep_run_id

    # ── 4. Veredicto ─────────────────────────────────────────────
    checks = result["integridade"]
    if checks["encontrado_21"] and checks["encontrado_22"] and checks["correlacao_ok"]:
        result["veredicto"] = "INTEGRO"
    elif checks["encontrado_21"] and checks["encontrado_22"]:
        result["veredicto"] = "DIVERGENCIA_CORRELACAO"
    elif checks["encontrado_21"]:
        result["veredicto"] = "EPISODIO_NAO_ENCONTRADO"
    elif checks["encontrado_22"]:
        result["veredicto"] = "RUN_NAO_ENCONTRADO"
    else:
        result["veredicto"] = "NAO_ENCONTRADO"

    logger.info(
        f"[audit] query run={decision_run_id} ep={episode_id} "
        f"veredicto={result['veredicto']}"
    )

    return result
