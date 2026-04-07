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

Contrato interno (Noite 8 hardening):
  - _load_queue_rows(ws): única fonte de verdade para leitura da fila
  - _get_queue_item(ws, ep_id) -> (item | None, row_idx | None): tuple — sem dupla leitura
  - _find_row_index usa _load_queue_rows, nunca col_values isolado
  - _recover_expired_lock recebe item já resolvido, sem releitura implícita
  - _is_lock_expired aceita None de forma segura
"""

import uuid
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from app.core.config import settings
from app.models.decide import DecideRequest
from app.services.decision_engine import run_decision
from app.services.sheets_store import persist_decision, verify_persistence
from app.services.structured_logger import NeuroLog

logger = logging.getLogger("neuroauth.runner")

ENGINE_VERSION   = "v2.2"
LOCK_TTL_SECONDS = 300   # lock expira após 5 min — libera locks órfãos
MAX_ATTEMPTS     = 3     # máximo de tentativas antes de erro definitivo
RETRY_STATES     = {"pendente", "erro"}  # únicos estados que podem entrar no retry
TAB_RUNNER_QUEUE = "23_RUNNER_QUEUE"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

ESTADOS = ["pendente","lockado","processando","persistido","verificado","concluido","erro"]
ESTADOS_FINAIS   = {"concluido", "erro"}
ESTADOS_BLOQUEIO = {"lockado", "processando", "concluido"}

# Cabeçalho canônico da fila — ordem definida aqui, nunca assumida da planilha
QUEUE_HEADERS = [
    "queue_item_id", "episode_id", "trace_id",
    "idempotency_key", "lock_owner", "lock_at",
    "attempt_count", "last_attempt_at",
    "final_status", "decision_run_id",
    "error_message", "created_at", "updated_at",
]


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
        ws.append_row(QUEUE_HEADERS)
        return ws


# ── IDEMPOTENCY KEY ───────────────────────────────────────────────────────────

def _build_idempotency_key(req: DecideRequest) -> str:
    """episode_id + payload_hash + engine_version"""
    payload_hash = hashlib.sha256(
        f"{req.cid_principal}{req.procedimento}{req.convenio}"
        f"{req.indicacao_clinica[:60]}".encode()
    ).hexdigest()[:12]
    return f"{req.episodio_id}_{payload_hash}_{ENGINE_VERSION}"


# ── LEITURA DA FILA — ÚNICA FONTE DE VERDADE ─────────────────────────────────

def _load_queue_rows(ws: gspread.Worksheet) -> Tuple[list, list]:
    """
    Lê todas as linhas da fila em UMA ÚNICA chamada à API.
    Retorna (headers, data_rows) onde data_rows é lista de dicts.

    Contrato: todos os consumidores devem usar esta função.
    Nunca chamar ws.col_values() ou ws.get_all_values() diretamente
    para operações de busca — isso evita dupla fonte de verdade.
    """
    all_rows = ws.get_all_values()
    if not all_rows:
        return [], []
    headers = all_rows[0]
    data_rows = [dict(zip(headers, row)) for row in all_rows[1:] if any(row)]
    return headers, data_rows


def _get_queue_item(
    ws: gspread.Worksheet,
    episode_id: str,
) -> Tuple[Optional[dict], Optional[int]]:
    """
    Retorna (item, row_idx) para o episode_id dado.
    row_idx é 1-indexed (linha real na planilha, incluindo cabeçalho).
    Retorna (None, None) se não encontrado.

    Contrato: UMA única leitura via _load_queue_rows — sem col_values separado.
    """
    headers, data_rows = _load_queue_rows(ws)
    if not headers:
        return None, None

    ep_col_name = "episode_id"
    if ep_col_name not in headers:
        logger.warning(f"[runner] Coluna '{ep_col_name}' ausente na fila")
        return None, None

    for i, row_dict in enumerate(data_rows):
        if row_dict.get("episode_id") == episode_id:
            # row_idx: +1 para cabeçalho, +1 para 1-indexed
            row_idx = i + 2
            return row_dict, row_idx

    return None, None


# ── OPERAÇÕES DE FILA ─────────────────────────────────────────────────────────

def _enqueue(ws: gspread.Worksheet, req: DecideRequest, idem_key: str) -> str:
    """Cria item na fila com status pendente. Retorna queue_item_id."""
    queue_item_id = f"QI-{str(uuid.uuid4())[:8].upper()}"
    now = datetime.now(timezone.utc).isoformat()
    ws.append_row([
        queue_item_id, req.episodio_id, "",
        idem_key, "", "", "0", "",
        "pendente", "", "", now, now,
    ], value_input_option="USER_ENTERED")
    return queue_item_id


def _update_queue_row(
    ws: gspread.Worksheet,
    row_idx: int,
    updates: dict,
) -> None:
    """
    Atualiza campos específicos de uma linha da fila pelo índice.
    Lê cabeçalho via row_values(1) — operação leve, apenas 1 linha.
    """
    headers = ws.row_values(1)
    col_map = {name: i + 1 for i, name in enumerate(headers) if name}

    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    cells = [
        gspread.Cell(row_idx, col_map[field], str(value))
        for field, value in updates.items()
        if field in col_map
    ]
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


# ── TTL / LOCK ÓRFÃO ─────────────────────────────────────────────────────────

def _is_lock_expired(item: Optional[dict]) -> bool:
    """
    Retorna True se o lock está vencido (age > LOCK_TTL_SECONDS).
    Aceita None de forma segura — retorna False (sem item = sem lock).
    Lock sem lock_at é considerado não expirado (seguro por padrão).
    """
    if item is None:
        return False
    lock_at_str = item.get("lock_at", "")
    if not lock_at_str:
        return False
    try:
        lock_at = datetime.fromisoformat(lock_at_str.replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - lock_at).total_seconds()
        return age > LOCK_TTL_SECONDS
    except Exception:
        return False


def _recover_expired_lock(
    ws: gspread.Worksheet,
    row_idx: int,
    item: dict,
    log: "NeuroLog",
) -> None:
    """
    Recupera lock expirado: reseta para 'pendente' e emite evento auditável.

    Contrato:
    - Recebe item já resolvido (sem releitura implícita da planilha)
    - row_idx deve ser o índice real da linha do item
    - Não faz nova leitura do sheet — usa os dados já carregados em item
    - Se item é None ou row_idx inválido: registra aviso e retorna sem ação
    """
    if item is None or not row_idx:
        logger.warning("[runner] _recover_expired_lock chamado com item=None ou row_idx inválido")
        return

    episode_id     = item.get("episode_id", "?")
    old_lock_owner = item.get("lock_owner", "")

    _update_queue_row(ws, row_idx, {
        "final_status": "pendente",
        "lock_owner":   "",
        "lock_at":      "",
    })
    log.emit("lock_expired", status="recovered", details={
        "recovered_from_owner": old_lock_owner,
        "episode_id":           episode_id,
        "ttl_seconds":          LOCK_TTL_SECONDS,
    })
    logger.warning(
        f"[runner] LOCK_EXPIRED_RECOVERED ep={episode_id} "
        f"old_owner={old_lock_owner} ttl={LOCK_TTL_SECONDS}s"
    )


# ── RUNNER PRINCIPAL ──────────────────────────────────────────────────────────

def run_episode(req: DecideRequest) -> dict:
    """
    Processa 1 episódio com idempotência e lock.
    Fluxo: verifica → lock → processa → persiste → verifica → conclui
    """
    trace_id = f"TR-{str(uuid.uuid4())[:12].upper()}"
    idem_key = _build_idempotency_key(req)

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
        "lock_recovered":  False,
    }

    try:
        ws = _get_queue_sheet()
    except Exception as e:
        log.error("get_queue_sheet", type(e).__name__, str(e)[:200])
        result["error"] = f"Falha ao conectar fila: {e}"
        return result

    # STEP 1: Leitura única da fila — item + row_idx juntos, sem dupla fonte
    item, row_idx = _get_queue_item(ws, req.episodio_id)

    if item:
        status_atual  = item.get("final_status", "")
        attempt_count = int(item.get("attempt_count", "0") or "0")

        # Episódio concluído: não reprocessar
        if status_atual == "concluido":
            result["status"] = "skipped_already_done"
            result["decision_run_id"] = item.get("decision_run_id")
            logger.info(f"[runner] SKIP concluido ep={req.episodio_id}")
            return result

        # MAX_ATTEMPTS atingido: bloqueio definitivo
        # Condição: != 'concluido' para permitir que episódios bem-sucedidos não sejam bloqueados
        if attempt_count >= MAX_ATTEMPTS and status_atual != "concluido":
            logger.error(f"[runner] MAX_ATTEMPTS ep={req.episodio_id} attempt={attempt_count}")
            if row_idx:
                _update_queue_row(ws, row_idx, {
                    "final_status":  "erro",
                    "error_message": f"MAX_ATTEMPTS={MAX_ATTEMPTS} atingido sem sucesso",
                })
            result["status"] = "erro"
            result["error"]  = f"MAX_ATTEMPTS={MAX_ATTEMPTS} atingido"
            return result

        # Episódio lockado: verificar TTL
        if status_atual in ("lockado", "processando"):
            if _is_lock_expired(item):
                # item e row_idx já resolvidos — sem releitura
                _recover_expired_lock(ws, row_idx, item, log)
                result["lock_recovered"] = True
                # Continua processamento normalmente
            else:
                result["status"] = "skipped_locked"
                result["error"]  = "Episódio lockado por outro ciclo"
                logger.warning(f"[runner] SKIP lockado ep={req.episodio_id} owner={item.get('lock_owner')}")
                return result

    # STEP 2: Criar item se não existe
    if not item:
        _enqueue(ws, req, idem_key)
        # Reler para obter row_idx após enqueue — única exceção de releitura necessária
        item, row_idx = _get_queue_item(ws, req.episodio_id)

    if not row_idx:
        result["error"] = "Não foi possível localizar linha na fila após enqueue"
        return result

    lock_owner = f"runner-{trace_id}"

    # STEP 3: Adquirir lock
    _update_queue_row(ws, row_idx, {
        "final_status": "lockado",
        "lock_owner":   lock_owner,
        "lock_at":      datetime.now(timezone.utc).isoformat(),
        "trace_id":     trace_id,
    })

    try:
        # STEP 4: Processar
        _update_queue_row(ws, row_idx, {
            "final_status":    "processando",
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
        })

        # Incrementar attempt_count a partir do item já carregado
        # Reler apenas o campo necessário — evitar get_all_values de novo
        current_attempt = int((item or {}).get("attempt_count", "0") or "0")
        attempt = current_attempt + 1
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
            "final_decision": res.classification,
            "score":          res.score,
            "risco_glosa":    res.risco_glosa,
        })

        # STEP 5: Persistir
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

        # STEP 6: Verificar
        verify = verify_persistence(req, res, max_retries=3)
        if verify["veredicto"] not in ("OK", "OK_SEM_CORRELACAO"):
            raise RuntimeError(f"verify_persistence BLOQUEADO: {verify['detalhes']}")

        _update_queue_row(ws, row_idx, {"final_status": "verificado"})
        log.emit("verify_success", status="ok", details={
            "verification_method": "retry_read",
            "correlation_ok":      verify["veredicto"] == "OK",
            "veredicto":           verify["veredicto"],
        })

        # STEP 7: Concluir + liberar lock
        _update_queue_row(ws, row_idx, {
            "final_status":    "concluido",
            "decision_run_id": res.decision_run_id,
            "lock_owner":      "",
            "lock_at":         "",
        })

        result["status"] = "concluido"
        logger.info(f"[runner] DONE ep={req.episodio_id} run={res.decision_run_id} trace={trace_id}")
        return result

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {str(exc)[:200]}"
        log.error("run_episode", type(exc).__name__, str(exc)[:200])
        # Liberar lock apenas se temos row_idx válido
        if row_idx:
            _update_queue_row(ws, row_idx, {
                "final_status":  "erro",
                "error_message": error_msg,
                "lock_owner":    "",
                "lock_at":       "",
            })
        result["error"] = error_msg
        logger.error(f"[runner] ERROR ep={req.episodio_id} trace={trace_id}: {error_msg}")
        return result


# ── RELATÓRIO DE INTEGRIDADE ──────────────────────────────────────────────────

def batch_integrity_report(results: list) -> dict:
    total      = len(results)
    concluidos = [r for r in results if r["status"] == "concluido"]
    erros      = [r for r in results if r["status"] == "erro"]
    skipped    = [r for r in results if r["status"].startswith("skipped")]
    locks_rec  = [r for r in results if r.get("lock_recovered")]

    trace_ids = [r["trace_id"] for r in results if r.get("trace_id")]
    trace_dup = len(trace_ids) - len(set(trace_ids))

    run_ids   = [r["decision_run_id"] for r in concluidos if r.get("decision_run_id")]
    run_dup   = len(run_ids) - len(set(run_ids))

    sem_run   = [r for r in concluidos if not r.get("decision_run_id")]

    divergencias = []
    if trace_dup > 0:
        divergencias.append(f"trace_id duplicado: {trace_dup} ocorrências")
    if run_dup > 0:
        divergencias.append(f"decision_run_id duplicado: {run_dup} ocorrências")
    if sem_run:
        divergencias.append(f"concluídos sem decision_run_id: {len(sem_run)}")

    return {
        "total_recebidos":         total,
        "total_concluido":         len(concluidos),
        "total_erro":              len(erros),
        "total_skipped":           len(skipped),
        "total_locks_recuperados": len(locks_rec),
        "trace_ids_unicos":        len(set(trace_ids)),
        "trace_ids_duplicados":    trace_dup,
        "run_ids_unicos":          len(set(run_ids)),
        "run_ids_duplicados":      run_dup,
        "correlacao_completa":     len(sem_run) == 0,
        "divergencias":            divergencias,
        "integridade":             "OK" if not divergencias else "DIVERGENCIA",
    }


# ── EXECUÇÃO DE LOTE ──────────────────────────────────────────────────────────

def run_batch(requests: list, stop_on_error: bool = False) -> dict:
    results = []
    for req in requests:
        r = run_episode(req)
        results.append(r)
        if stop_on_error and r["status"] == "erro":
            logger.warning(f"[runner] BATCH stop_on_error — parando em {req.episodio_id}")
            break
    report = batch_integrity_report(results)
    logger.info(
        f"[runner] BATCH_DONE total={report['total_recebidos']} "
        f"concluido={report['total_concluido']} erro={report['total_erro']} "
        f"integridade={report['integridade']}"
    )
    return {"results": results, "report": report}
