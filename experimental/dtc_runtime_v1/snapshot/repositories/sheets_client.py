"""
NEUROAUTH — sheets_client.py
Versão: 1.1.0

Responsabilidade: único ponto de acesso ao Google Sheets.
Sem lógica de negócio. Sem mapeamento de campos de domínio.
Apenas: autenticar, ler, escrever, atualizar.

Garantias:
- Cache de workbook por 120s — evita explosão de quota 429 em chamadas consecutivas.
- head=3 por padrão (linha 1=título, 2=subtítulo, 3=headers).
- find_row_by_col → (None, None) se não encontrar; nunca lança.
- append_row_by_header → ignora chaves ausentes no header.
- update_row_fields → ignora campos sem coluna correspondente.
- Caminhos de credencial resolvidos por prioridade (local → Render secrets → env var).
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger("neuroauth.sheets_client")

# ─── Configuração ─────────────────────────────────────────────────────────────

SPREADSHEET_ID = "1tId-AZorbeESHhlvOZei7_UbR0pMj0TMwsH0_lTCGLQ"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_CREDS_CANDIDATES = [
    # Desenvolvimento local (Mac)
    os.path.expanduser("~/Downloads/neuroauth-auth-ae97fe63dec3.json"),
    # Render (secrets file — nome explícito)
    "/etc/secrets/neuroauth-auth-ae97fe63dec3.json",
    # Render (secrets file — nome genérico 'credentials.json')
    "/etc/secrets/credentials.json",
    # Variável de ambiente GOOGLE_APPLICATION_CREDENTIALS (padrão Google)
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
    # Variável de ambiente personalizada NEUROAUTH
    os.environ.get("GOOGLE_SHEETS_CREDS_PATH", ""),
]


# ─── Conexão ──────────────────────────────────────────────────────────────────

def _resolve_creds() -> str:
    for path in _CREDS_CANDIDATES:
        if path and os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "Credenciais Google Sheets não encontradas. "
        f"Caminhos tentados: {[p for p in _CREDS_CANDIDATES if p]}"
    )


# ─── Retry com backoff para quota 429 ────────────────────────────────────────

_RETRY_MAX     = 5      # tentativas extras (total = 1 + 5)
_RETRY_BASE_S  = 2.0    # espera base em segundos

def _retry_on_quota(fn):
    """
    Executa fn() com retry exponencial apenas em HTTP 429 (Quota exceeded).
    Qualquer outro erro propaga imediatamente — sem mascarar erros de negócio.
    """
    last_exc = None
    for attempt in range(_RETRY_MAX + 1):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            try:
                status = exc.response.status_code
            except Exception:
                status = None
            if status == 429 and attempt < _RETRY_MAX:
                wait = _RETRY_BASE_S * (2 ** attempt) + random.uniform(0.0, 1.0)
                logger.warning(
                    "Sheets quota 429 — aguardando %.1fs (tentativa %d/%d)",
                    wait, attempt + 1, _RETRY_MAX,
                )
                time.sleep(wait)
                last_exc = exc
            else:
                raise
    raise last_exc  # type: ignore[misc]


# ─── Cache de workbook ────────────────────────────────────────────────────────

_wb_cache: dict = {"wb": None, "ts": 0.0}
_WB_CACHE_TTL = 120.0  # segundos — uma request típica dura ~15s, TTL 120s é seguro


def _get_workbook() -> gspread.Spreadsheet:
    """
    Retorna o workbook autenticado.
    Reutiliza o objeto em cache se criado há menos de _WB_CACHE_TTL segundos,
    eliminando chamadas redundantes a open_by_key (fetch de metadados) que
    consomem quota de leitura do Sheets API.
    """
    now = time.monotonic()
    if _wb_cache["wb"] is not None and (now - _wb_cache["ts"]) < _WB_CACHE_TTL:
        return _wb_cache["wb"]
    creds = Credentials.from_service_account_file(_resolve_creds(), scopes=SCOPES)
    gc = gspread.authorize(creds)
    wb = gc.open_by_key(SPREADSHEET_ID)
    _wb_cache["wb"] = wb
    _wb_cache["ts"] = now
    logger.debug("workbook cache renovado")
    return wb


def get_worksheet(sheet_name: str) -> gspread.Worksheet:
    """Retorna worksheet autenticado pelo nome da aba. Com retry em quota 429."""
    return _retry_on_quota(lambda: _get_workbook().worksheet(sheet_name))


def ensure_worksheet(
    sheet_name: str,
    rows: int = 1000,
    cols: int = 30,
) -> gspread.Worksheet:
    """
    Retorna o worksheet com o nome dado.
    Se a aba não existir, cria com as dimensões indicadas.
    Nunca lança WorksheetNotFound — ideal para sheets de logging criados on-demand.
    """
    try:
        return _retry_on_quota(lambda: _get_workbook().worksheet(sheet_name))
    except gspread.exceptions.WorksheetNotFound:
        logger.info("ensure_worksheet: aba '%s' não encontrada — criando", sheet_name)
        wb = _get_workbook()
        return _retry_on_quota(lambda: wb.add_worksheet(title=sheet_name, rows=rows, cols=cols))


# ─── Leitura ──────────────────────────────────────────────────────────────────

def get_header_row(ws: gspread.Worksheet, head: int = 3) -> list[str]:
    """
    Retorna a lista de headers da linha `head` (1-based).
    Células vazias são retornadas como string vazia para preservar posição.
    """
    return [str(h).strip() for h in _retry_on_quota(lambda: ws.row_values(head))]


def read_all_records(ws: gspread.Worksheet, head: int = 3) -> list[dict]:
    """
    Lê todas as linhas de dado com keys = headers da linha `head`.
    Linhas completamente vazias são ignoradas.
    Colunas com header vazio são ignoradas na indexação.
    """
    all_values = _retry_on_quota(lambda: ws.get_all_values())
    if len(all_values) < head:
        return []

    headers = [str(h).strip() for h in all_values[head - 1]]
    records: list[dict] = []

    for row in all_values[head:]:           # linhas de dado = após o header
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        row_dict = {
            headers[i]: str(padded[i]).strip()
            for i in range(len(headers))
            if headers[i]                   # ignora colunas sem header
        }
        if any(v for v in row_dict.values()):
            records.append(row_dict)

    return records


def find_row_by_col(
    ws: gspread.Worksheet,
    col_name: str,
    value: str,
    head: int = 3,
) -> tuple[int, dict] | tuple[None, None]:
    """
    Localiza a primeira linha onde col_name == value (comparação strip).
    Retorna (row_index_1based, row_dict) ou (None, None) se não encontrar.
    Nunca lança exceção.
    """
    all_values = _retry_on_quota(lambda: ws.get_all_values())
    if len(all_values) < head:
        return None, None

    headers = [str(h).strip() for h in all_values[head - 1]]

    if col_name not in headers:
        logger.warning("find_row_by_col: coluna '%s' não encontrada no header", col_name)
        return None, None

    col_idx = headers.index(col_name)

    for abs_idx, row in enumerate(all_values[head:], start=head + 1):  # 1-based
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        if str(padded[col_idx]).strip() == str(value).strip():
            row_dict = {
                headers[i]: str(padded[i]).strip()
                for i in range(len(headers))
                if headers[i]
            }
            return abs_idx, row_dict

    return None, None


# ─── Escrita ──────────────────────────────────────────────────────────────────

def append_row_by_header(
    ws: gspread.Worksheet,
    data: dict,
    head: int = 3,
) -> None:
    """
    Acrescenta linha ao worksheet na ordem dos headers da linha `head`.
    Campos ausentes em `data` ficam em branco.
    Campos extras em `data` que não existem no header são silenciosamente ignorados.
    Dicts e listas são serializados como JSON.

    Usa batch_update com posição explícita (len(get_all_values)+1) em vez de
    append_row, para evitar ambiguidade do values.append API em planilhas com
    head=3 (rows 1-2 de título/subtítulo antes dos headers).
    """
    headers = get_header_row(ws, head)
    row: list[str] = []
    for h in headers:
        val: Any = data.get(h, "")
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
        row.append(str(val) if val is not None else "")

    # Calcula próxima linha vazia após os dados existentes
    all_values = _retry_on_quota(lambda: ws.get_all_values())
    next_row = len(all_values) + 1

    # Escreve a linha inteira com batch_update (mesmo mecanismo de update_row_fields)
    row_range = "A{}:{}".format(
        next_row,
        gspread.utils.rowcol_to_a1(next_row, len(row)),
    )
    _retry_on_quota(lambda: ws.batch_update([{"range": row_range, "values": [row]}]))


def update_row_fields(
    ws: gspread.Worksheet,
    row_idx: int,
    header_row: list[str],
    updates: dict,
) -> None:
    """
    Atualiza campos específicos na linha `row_idx` (1-based).
    `header_row` deve ser a lista obtida via get_header_row().
    Campos em `updates` sem coluna correspondente são silenciosamente ignorados.
    Dicts e listas são serializados como JSON.
    Usa batch_update para minimizar roundtrips.
    """
    if not updates:
        return

    batch: list[dict] = []
    for field, value in updates.items():
        if field not in header_row:
            logger.debug(
                "update_row_fields: campo '%s' não existe no header, ignorado", field
            )
            continue
        col_idx = header_row.index(field) + 1       # 1-based
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        cell = gspread.utils.rowcol_to_a1(row_idx, col_idx)
        batch.append({"range": cell, "values": [[str(value) if value is not None else ""]]})

    if batch:
        _retry_on_quota(lambda: ws.batch_update(batch))
