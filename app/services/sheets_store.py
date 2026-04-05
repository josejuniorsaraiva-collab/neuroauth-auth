"""
app/services/sheets_store.py
Persistência em Google Sheets.

Correção da quebra 4:
- Não assume posição fixa de colunas em 22_EPISODIOS
- Faz header discovery na linha HEADER_ROW para encontrar índices reais
- Apenas 21_DECISION_RUNS usa append_row (estrutura controlada por nós)
"""

import gspread
import json
import logging
from google.oauth2.service_account import Credentials
from app.models.decide import DecideRequest, DecideResponse
from app.core.config import settings
from datetime import datetime

logger = logging.getLogger("neuroauth.sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

TAB_DECISION_RUNS = "21_DECISION_RUNS"
TAB_EPISODIOS     = "22_EPISODIOS"

# Linha de cabeçalho real em 22_EPISODIOS (1-indexed)
# Ajustar se a estrutura da aba usar linha diferente
EPISODIOS_HEADER_ROW = 3

# Colunas que o engine precisa encontrar em 22_EPISODIOS
COL_EPISODIO_ID       = "episodio_id"
COL_DECISION_STATUS   = "decision_status"
COL_LAST_RUN_ID       = "ultimo_decision_run_id"
COL_UPDATED_AT        = "updated_at"


def _get_client() -> gspread.Client:
    creds_dict = json.loads(settings.GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def persist_decision(req: DecideRequest, res: DecideResponse) -> bool:
    """
    1. Append em 21_DECISION_RUNS (estrutura fixa — controlada pelo sistema)
    2. Update em 22_EPISODIOS (estrutura existente — descoberta dinamicamente)
    Nunca lança exceção para o caller.
    """
    try:
        gc = _get_client()
        ss = gc.open_by_key(settings.SPREADSHEET_ID)

        _append_decision_run(ss, req, res)
        _update_episodio(ss, res.episodio_id, res.decision_status, res.decision_run_id)
        return True

    except Exception as e:
        logger.error(f"[sheets_store] Falha: {type(e).__name__}: {str(e)[:200]}")
        return False


def _append_decision_run(
    ss: gspread.Spreadsheet,
    req: DecideRequest,
    res: DecideResponse,
) -> None:
    """
    Append linha em 21_DECISION_RUNS.
    Estrutura controlada por nós — sem ambiguidade de colunas.
    Se a aba não existir, cria com cabeçalho.
    """
    try:
        ws = ss.worksheet(TAB_DECISION_RUNS)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=TAB_DECISION_RUNS, rows=1000, cols=20)
        ws.append_row([
            "decision_run_id", "episodio_id", "timestamp",
            "classification", "decision_status", "score",
            "risco_glosa", "justificativa",
            "pendencias", "pontos_frageis",
            "cid_principal", "procedimento", "convenio", "crm_solicitante",
        ])

    ws.append_row([
        res.decision_run_id,
        res.episodio_id,
        res.timestamp,
        res.classification,
        res.decision_status,
        res.score,
        res.risco_glosa,
        res.justificativa[:500],
        " | ".join(res.pendencias),
        " | ".join(res.pontos_frageis),
        req.cid_principal,
        req.procedimento,
        req.convenio,
        req.crm,
    ], value_input_option="USER_ENTERED")


def _update_episodio(
    ss: gspread.Spreadsheet,
    episodio_id: str,
    decision_status: str,
    decision_run_id: str,
) -> None:
    """
    Atualiza 22_EPISODIOS por header discovery — não assume posição de coluna.

    Fluxo:
    1. Ler linha EPISODIOS_HEADER_ROW para descobrir índices reais
    2. Encontrar episodio_id na coluna correta
    3. Atualizar células pelo índice descoberto
    4. Se episodio_id não existe → append nova linha
    """
    ws = ss.worksheet(TAB_EPISODIOS)

    # 1. Descobrir mapa de colunas pelo cabeçalho
    header_row = ws.row_values(EPISODIOS_HEADER_ROW)
    col_map = {name.strip().lower(): idx + 1 for idx, name in enumerate(header_row) if name.strip()}

    # Verificar colunas obrigatórias
    missing = [c for c in [COL_EPISODIO_ID, COL_DECISION_STATUS] if c not in col_map]
    if missing:
        logger.error(
            f"[sheets_store] 22_EPISODIOS: colunas ausentes: {missing}. "
            f"Cabeçalho encontrado na linha {EPISODIOS_HEADER_ROW}: {header_row}"
        )
        return

    ep_col_idx = col_map[COL_EPISODIO_ID]

    # 2. Encontrar linha do episodio_id (busca a partir de EPISODIOS_HEADER_ROW + 1)
    all_ep_ids = ws.col_values(ep_col_idx)
    try:
        # col_values é 1-indexed na posição, mas lista é 0-indexed
        row_idx = all_ep_ids.index(episodio_id) + 1
    except ValueError:
        # Episódio não encontrado → criar linha nova
        logger.info(f"[sheets_store] episodio_id '{episodio_id}' não encontrado → append")
        new_row = [""] * len(header_row)
        new_row[ep_col_idx - 1] = episodio_id
        if COL_DECISION_STATUS in col_map:
            new_row[col_map[COL_DECISION_STATUS] - 1] = decision_status
        if COL_LAST_RUN_ID in col_map:
            new_row[col_map[COL_LAST_RUN_ID] - 1] = decision_run_id
        if COL_UPDATED_AT in col_map:
            new_row[col_map[COL_UPDATED_AT] - 1] = datetime.utcnow().isoformat()
        ws.append_row(new_row, value_input_option="USER_ENTERED")
        return

    # 3. Atualizar apenas as células necessárias
    updates = []
    now_iso = datetime.utcnow().isoformat()

    if COL_DECISION_STATUS in col_map:
        updates.append(gspread.Cell(row_idx, col_map[COL_DECISION_STATUS], decision_status))
    if COL_LAST_RUN_ID in col_map:
        updates.append(gspread.Cell(row_idx, col_map[COL_LAST_RUN_ID], decision_run_id))
    if COL_UPDATED_AT in col_map:
        updates.append(gspread.Cell(row_idx, col_map[COL_UPDATED_AT], now_iso))

    if updates:
        ws.update_cells(updates, value_input_option="USER_ENTERED")
    else:
        logger.warning(f"[sheets_store] Nenhuma coluna atualizável encontrada em 22_EPISODIOS.")
