"""
app/services/runner.py
NEUROAUTH — Runner idempotente com fila e lock explícito

Fluxo: captura → lock → processa → persiste → verifica → conclui
Aba Sheets: 23_RUNNER_QUEUE

Estados do episódio na fila:
  pendente → lockado → processando → persistido → verificado → concluido | erro

Regras inegociáveis:
  - 1 episódio = 1 trace_id = 1 decisão por ciclo
  - não reprocessar episódio concluído
  - não reprocessar episódio lockado por outro ciclo
  - idempotency_key = episode_id + payload_hash + engine_version
  - só marca concluído após verify_success
  - libera lock ao final, mesmo em erro controlado
  - nunca duplica decision_run_id no mesmo ciclo lógico
"""

import uuid
import hashlib
import logging
import json
from datetime import datetime, timezone
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from app.core.config import settings
from app.models.decide import DecideRequest
from app.services.decision_engine import run_decision
from app.services.sheets_store import persist_decision, verify_persistence
from app.services.structured_logger import NeuroLog

logger = logging.getLogger("neuroauth.runner")

ENGINE_VERSION   = "v2.2"
TAB_RUNNER_QUEUE = "23_RUNNER_QUEUE"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Estados válidos — ordem de progressão
ESTADOS = ["pendente","lockado","processando","persistido","verificado","concluido","erro"]
ESTADOS_FINAIS   = {"concluido", "erro"}
ESTADOS_BLOQUEIO = {"lockado", "processando", "concluido"}


# ── CLIENTE SHEETS ────────────────────────────────────────────────────────────

def _get_queue_sheet() -> gspread.Worksheet:
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(settings.SPREADSHEET_ID)
    try:
        return ss.worksheet(TAB_RUNNER_QUEUE)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=TAB_RUNNER_QUEUE, rows=2000, cols=20)
        ws.append_row([
            "queue_item_id", "episode_id", "trace_id",
            "idempotency_key", "lock_owner", "lock_at",
            "attempt_count", "last_attempt_at",
            "final_status", "decision_run_id",
            "error_message", "created_at", "updated_at",
        ])
        return ws


# ── IDEMPOTENCY KEY ───────────────────────────────────────────────────────────

def _build_idempotency_key(req: DecideRequest) -> str:
    """episode_id + payload_hash + engine_version"""
    payload_hash = hashlib.sha256(
        f"{req.cid_principal}{req.procedimento}{req.convenio}"
        f"{req.indicacao_clinica[:60]}".encode()
    ).hexdigest()[:12]
    return f"{req.episodio_id}_{payload_hash}_{ENGINE_VERSION}"


# ── LEITURA DA FILA ───────────────────────────────────────────────────────────

def _get_queue_item(ws: gspread.Worksheet, episode_id: str) -> Optional[dict]:
    """Retorna o item da fila para episode_id, ou None se não existe."""
    rows = ws.get_all_values()
    if not rows:
        return None
    headers = rows[0]
    for row in rows[1:]:
        d = dict(zip(headers, row))
        if d.get("episode_id") == episode_id:
            return d
    return None


def _find_row_index(ws: gspread.Worksheet, episode_id: str) -> Optional[int]:
    """Retorna o número da linha (1-indexed) do episode_id, ou None."""
    ep_col = ws.col_values(2)  # coluna B = episode_id
    if episode_id in ep_col:
        return ep_col.index(episode_id) + 1
    return None


# ── OPERAÇÕES DE FILA ─────────────────────────────────────────────────────────

def _enqueue(ws: gspread.Worksheet, req: DecideRequest, idem_key: str) -> str:
    """Cria item na fila com status pendente. Retorna queue_item_id."""
    queue_item_id = f"QI-{str(uuid.uuid4())[:8].upper()}"
    now = datetime.now(timezone.utc).isoformat()
    ws.append_row([
        queue_item_id,          # queue_item_id
        req.episodio_id,        # episode_id
        "",                     # trace_id (preenchido ao processar)
        idem_key,               # idempotency_key
        "",                     # lock_owner
        "",                     # lock_at
        "0",                    # attempt_count
        "",                     # last_attempt_at
        "pendente",             # final_status
        "",                     # decision_run_id
        "",                     # error_message
        now,                    # created_at
        now,                    # updated_at
    ], value_input_option="USER_ENTERED")
    return queue_item_id


def _update_queue_row(
    ws: gspread.Worksheet,
    row_idx: int,
    updates: dict,
) -> None:
    """Atualiza campos específicos de uma linha da fila pelo índice."""
    headers = ws.row_values(1)
    col_map = {name: i + 1 for i, name in enumerate(headers) if name}

    cells = []
    now = datetime.now(timezone.utc).isoformat()
    updates["updated_at"] = now

    for field, value in updates.items():
        if field in col_map:
            cells.append(gspread.Cell(row_idx, col_map[field], str(value)))

    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


# ── RUNNER PRINCIPAL ──────────────────────────────────────────────────────────

def run_episode(req: DecideRequest) -> dict:
    """
    Processa 1 episódio com idempotência e lock.
    Retorna dict com resultado e rastreabilidade.

    Fluxo:
      1. Verificar idempotência (não reprocessar concluído/lockado)
      2. Adquirir lock
      3. Rodar decisão
      4. Persistir
      5. Verificar persistência
      6. Marcar concluído / erro
      7. Liberar lock
    """
    trace_id  = f"TR-{str(uuid.uuid4())[:12].upper()}"
    idem_key  = _build_idempotency_key(req)

    log = NeuroLog(
        trace_id=trace_id,
        episode_id=req.episodio_id,
        service_name="neuroauth.runner",
    )

    result = {
        "trace_id":        trace_id,
        "episode_id":      req.episodio_id,
        "idempotency_key": idem_key,
        "status":          "erro",
        "decision_run_id": None,
        "error":           None,
    }

    try:
        ws = _get_queue_sheet()
    except Exception as e:
        log.error("get_queue_sheet", type(e).__name__, str(e)[:200])
        result["error"] = f"Falha ao conectar fila: {e}"
        return result

    # ── STEP 1: Verificar item existente ─────────────────────────────────────
    item = _get_queue_item(ws, req.episodio_id)
    row_idx = _find_row_index(ws, req.episodio_id)

    if item:
        status_atual = item.get("final_status", "")
        existing_idem = item.get("idempotency_key", "")

        # Episódio já concluído: não reprocessar
        if status_atual == "concluido":
            logger.info(
                f"[runner] SKIP ep={req.episodio_id} — já concluído "
                f"run={item.get('decision_run_id')}"
            )
            result["status"] = "skipped_already_done"
            result["decision_run_id"] = item.get("decision_run_id")
            return result

        # Episódio lockado por outro ciclo: não entrar
        if status_atual in ("lockado", "processando"):
            logger.warning(
                f"[runner] SKIP ep={req.episodio_id} — lockado "
                f"por={item.get('lock_owner')} em={item.get('lock_at')}"
            )
            result["status"] = "skipped_locked"
            result["error"] = "Episódio lockado por outro ciclo"
            return result

        # Mesma idempotency_key já processada com sucesso: não duplicar
        if existing_idem == idem_key and status_atual == "concluido":
            result["status"] = "skipped_idempotent"
            result["decision_run_id"] = item.get("decision_run_id")
            return result

    # ── STEP 2: Criar ou re-usar item na fila ────────────────────────────────
    if not item:
        _enqueue(ws, req, idem_key)
        row_idx = _find_row_index(ws, req.episodio_id)

    if not row_idx:
        result["error"] = "Não foi possível localizar linha na fila após enqueue"
        return result

    lock_owner = f"runner-{trace_id}"

    # ── STEP 3: Adquirir lock ─────────────────────────────────────────────────
    _update_queue_row(ws, row_idx, {
        "final_status":   "lockado",
        "lock_owner":     lock_owner,
        "lock_at":        datetime.now(timezone.utc).isoformat(),
        "trace_id":       trace_id,
    })
    logger.info(f"[runner] LOCK ep={req.episodio_id} owner={lock_owner}")

    try:
        # ── STEP 4: Processar ────────────────────────────────────────────────
        _update_queue_row(ws, row_idx, {
            "final_status":    "processando",
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
        })

        # Incrementar attempt_count
        item_atual = _get_queue_item(ws, req.episodio_id)
        attempt = int(item_atual.get("attempt_count", "0") or "0") + 1
        _update_queue_row(ws, row_idx, {"attempt_count": str(attempt)})

        log.emit("decision_started", status="ok", details={
            "engine_version":  ENGINE_VERSION,
            "attempt_count":   attempt,
            "idempotency_key": idem_key,
        })

        res = run_decision(req)
        log.set_run_id(res.decision_run_id)
        result["decision_run_id"] = res.decision_run_id

        log.emit("decision_result", status="ok", details={
            "final_decision":  res.classification,
            "score":           res.score,
            "risco_glosa":     res.risco_glosa,
        })

        # ── STEP 5: Persistir ─────────────────────────────────────────────────
        log.emit("persist_start", status="ok", details={
            "target_ledger":  "21_DECISION_RUNS",
            "target_episode": "22_EPISODIOS",
        })

        persist_ok = persist_decision(req, res)
        if not persist_ok:
            raise RuntimeError("persist_decision retornou False")

        _update_queue_row(ws, row_idx, {"final_status": "persistido"})
        log.emit("persist_success", status="ok", details={
            "persisted_decision_run_id": res.decision_run_id,
        })

        # ── STEP 6: Verificar ─────────────────────────────────────────────────
        verify = verify_persistence(req, res, max_retries=3)
        if verify["veredicto"] not in ("OK", "OK_SEM_CORRELACAO"):
            raise RuntimeError(
                f"verify_persistence BLOQUEADO: {verify['detalhes']}"
            )

        _update_queue_row(ws, row_idx, {"final_status": "verificado"})
        log.emit("verify_success", status="ok", details={
            "verification_method": "retry_read",
            "correlation_ok":      verify["veredicto"] == "OK",
            "veredicto":           verify["veredicto"],
        })

        # ── STEP 7: Concluir ──────────────────────────────────────────────────
        _update_queue_row(ws, row_idx, {
            "final_status":    "concluido",
            "decision_run_id": res.decision_run_id,
            "lock_owner":      "",          # liberar lock
            "lock_at":         "",
        })

        result["status"] = "concluido"
        logger.info(
            f"[runner] DONE ep={req.episodio_id} "
            f"run={res.decision_run_id} trace={trace_id}"
        )
        return result

    except Exception as exc:
        # ── ERRO: registrar + liberar lock ────────────────────────────────────
        error_msg = f"{type(exc).__name__}: {str(exc)[:200]}"
        log.error(
            failed_stage="run_episode",
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )
        _update_queue_row(ws, row_idx, {
            "final_status": "erro",
            "error_message": error_msg,
            "lock_owner":   "",       # liberar lock mesmo em erro
            "lock_at":      "",
        })
        result["error"] = error_msg
        logger.error(f"[runner] ERROR ep={req.episodio_id} trace={trace_id}: {error_msg}")
        return result
