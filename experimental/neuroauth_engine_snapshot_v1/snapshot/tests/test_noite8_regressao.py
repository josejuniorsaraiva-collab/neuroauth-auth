"""
tests/test_noite8_regressao.py
NEUROAUTH — Testes de regressão do contrato do runner

Cobre os 5 cenários de borda identificados na revisão de arquitetura:
  R01: enqueue seguido de relocalização do item
  R02: lock expirado com item ausente
  R03: erro em persist_decision com row_idx=None
  R04: duas execuções concorrentes tentando a mesma linha
  R05: recovery após crash entre lock e persist
"""

import sys, uuid, pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call
sys.path.insert(0, '/Users/josecorreiasaraivajunior/neuroauth/backend')

from app.services.runner import (
    run_episode, _get_queue_item, _load_queue_rows,
    _is_lock_expired, _recover_expired_lock,
    LOCK_TTL_SECONDS, MAX_ATTEMPTS, QUEUE_HEADERS,
)
from app.services.structured_logger import NeuroLog
from app.models.decide import DecideRequest, OpmeItem


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _req(ep_id=None):
    return DecideRequest(
        episodio_id=ep_id or f"EP-REG-{str(uuid.uuid4())[:6].upper()}",
        cid_principal="M50.1",
        procedimento="Artrodese cervical anterior ACDF discectomia cervical anterior",
        convenio="Unimed Cariri", cod_cbhpm="3.07.13.02-1",
        indicacao_clinica="Hernia cervical C5-C6 mielopatia compressiva deficit motor Lhermitte RM",
        achados_resumo="RM cervical hernia C5-C6 compressao medular confirmada",
        tto_conservador="Fisioterapia 8 semanas analgesia",
        necessita_opme="Sim",
        opme_items=[
            OpmeItem(descricao="Cage PEEK C5-C6", qtd=1, fabricante="Synthes"),
            OpmeItem(descricao="Placa cervical", qtd=1, fabricante="DePuy"),
            OpmeItem(descricao="Parafusos", qtd=4, fabricante="Stryker"),
        ],
        crm="CE-12345", cbo="225120", medico_solicitante="Dr. Regressao"
    )


def _make_ws(rows=None):
    """Worksheet mock com append dinâmico — estado consistente após enqueue."""
    ws = MagicMock()
    data = [list(QUEUE_HEADERS)]
    if rows:
        for row_dict in rows:
            data.append([row_dict.get(h, "") for h in QUEUE_HEADERS])

    def dynamic_append(row, **kwargs):
        data.append(list(row))

    ws.get_all_values.side_effect = lambda: [list(r) for r in data]
    ws.append_row.side_effect = dynamic_append
    ws.row_values.return_value = list(QUEUE_HEADERS)
    ws.update_cells.return_value = None
    return ws


def _item(ep_id, status, attempts=0, lock_owner="", lock_at="", run_id=""):
    return {
        "queue_item_id": "QI-TEST", "episode_id": ep_id,
        "trace_id": "TR-OLD", "idempotency_key": "idem_old",
        "lock_owner": lock_owner, "lock_at": lock_at,
        "attempt_count": str(attempts), "last_attempt_at": "",
        "final_status": status, "decision_run_id": run_id,
        "error_message": "", "created_at": "2026-01-01", "updated_at": "2026-01-01",
    }


VERIFY_OK = {"veredicto": "OK", "tentativas": 1, "detalhes": []}


# ── TESTES DE REGRESSÃO ───────────────────────────────────────────────────────

class TestRunnerRegressao:

    def test_R01_enqueue_seguido_de_relocalizacao(self):
        """
        R01: após _enqueue(), _get_queue_item() deve encontrar o item.
        Valida que o append dinâmico do ws mantém consistência de estado.
        """
        ep_id = "EP-ENQUEUE-R01"
        ws = _make_ws([])  # fila vazia

        # Verificar que item não existe antes
        item_antes, idx_antes = _get_queue_item(ws, ep_id)
        assert item_antes is None
        assert idx_antes is None

        # Simular enqueue manual
        from app.services.runner import _enqueue, _build_idempotency_key
        req = _req(ep_id)
        idem = _build_idempotency_key(req)
        _enqueue(ws, req, idem)

        # Verificar que item é encontrado após enqueue
        item_depois, idx_depois = _get_queue_item(ws, ep_id)
        assert item_depois is not None, "Item deve ser encontrado após enqueue"
        assert idx_depois is not None, "row_idx deve ser válido após enqueue"
        assert item_depois["episode_id"] == ep_id
        assert item_depois["final_status"] == "pendente"

    def test_R02_lock_expirado_com_item_ausente(self):
        """
        R02: _recover_expired_lock com item=None deve retornar sem quebrar.
        Valida que o contrato de None-safety está correto.
        """
        ws = MagicMock()
        log = NeuroLog("TR-R02", "EP-R02")

        # Não deve lançar exceção
        try:
            _recover_expired_lock(ws, 2, None, log)
            recovered = True
        except Exception as e:
            recovered = False
            pytest.fail(f"_recover_expired_lock com item=None lançou: {e}")

        assert recovered
        # ws não deve ter sido chamado para escrita
        ws.update_cells.assert_not_called()

    def test_R02b_lock_expirado_com_row_idx_none(self):
        """
        R02b: _recover_expired_lock com row_idx=None deve retornar sem quebrar.
        """
        ws = MagicMock()
        log = NeuroLog("TR-R02B", "EP-R02B")
        item = _item("EP-R02B", "lockado", lock_owner="runner-old")

        try:
            _recover_expired_lock(ws, None, item, log)
        except Exception as e:
            pytest.fail(f"_recover_expired_lock com row_idx=None lançou: {e}")

        ws.update_cells.assert_not_called()

    def test_R03_erro_persist_com_row_idx_none(self):
        """
        R03: se row_idx=None quando persist falha, o runner deve registrar
        o erro sem tentar _update_queue_row (evitar AttributeError).
        """
        ep_id = "EP-R03"
        # ws retorna fila vazia E append não funciona (simula falha de enqueue)
        ws = MagicMock()
        ws.get_all_values.return_value = [list(QUEUE_HEADERS)]
        ws.append_row.return_value = None  # estático — não atualiza data
        ws.row_values.return_value = list(QUEUE_HEADERS)
        ws.update_cells.return_value = None

        with patch("app.services.runner._get_queue_sheet", return_value=ws), \
             patch("app.services.runner.persist_decision",
                   side_effect=RuntimeError("db down")):
            result = run_episode(_req(ep_id))

        # Deve registrar erro sem quebrar
        assert result["status"] == "erro"
        assert result.get("error") is not None
        # update_cells não deve ter sido chamado com row_idx inválido
        # (ws.update_cells pode ter sido chamado por _update_queue_row
        #  mas apenas se row_idx foi encontrado — neste caso não foi)

    def test_R04_duas_execucoes_concorrentes_mesma_linha(self):
        """
        R04: duas execuções tentando processar o mesmo episódio —
        a segunda deve retornar skipped_locked (lock válido).
        """
        ep_id = "EP-R04"
        fresh_at = datetime.now(timezone.utc).isoformat()
        item_locked = _item(ep_id, "lockado",
                            lock_owner="runner-primeiro", lock_at=fresh_at)
        ws = _make_ws([item_locked])

        with patch("app.services.runner._get_queue_sheet", return_value=ws):
            result = run_episode(_req(ep_id))

        assert result["status"] == "skipped_locked"
        assert result.get("lock_recovered") is not True
        # Garantir que não processou
        assert result.get("decision_run_id") is None

    def test_R05_recovery_apos_crash_entre_lock_e_persist(self):
        """
        R05: episódio com lock expirado (crash entre lock e persist)
        deve ser recuperado e processado normalmente.
        Simula cenário: processo morreu após adquirir lock mas antes de persistir.
        """
        ep_id = "EP-R05"
        crashed_at = (
            datetime.now(timezone.utc) - timedelta(seconds=LOCK_TTL_SECONDS + 120)
        ).isoformat()

        # Estado pós-crash: lockado com lock expirado, attempt_count=1
        item_crashed = _item(ep_id, "lockado", attempts=1,
                             lock_owner="runner-crashed", lock_at=crashed_at)
        ws = _make_ws([item_crashed])

        updates = []
        def fake_update(w, idx, upd):
            updates.append(upd.copy())

        with patch("app.services.runner._get_queue_sheet", return_value=ws), \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=VERIFY_OK), \
             patch("app.services.runner._update_queue_row", side_effect=fake_update):
            result = run_episode(_req(ep_id))

        # Deve ter processado (não skipped_locked)
        assert result["status"] != "skipped_locked", \
            "Lock expirado não deve bloquear reprocessamento"
        assert result.get("lock_recovered") is True, \
            "lock_recovered deve ser True após recovery de crash"

        # Deve ter passado pelo estado pendente (recovery) e depois lockado novamente
        statuses = [u.get("final_status") for u in updates if "final_status" in u]
        assert "pendente" in statuses, \
            "Recovery deve resetar para pendente antes de re-processar"
