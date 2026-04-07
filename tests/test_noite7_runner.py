"""
tests/test_noite7_runner.py
NEUROAUTH — Testes de Noite 7: runner idempotente

Valida:
- episódio concluído não reprocessa
- episódio lockado não entra
- falha antes de persistência não marca concluído
- mesmo idempotency_key não duplica
- 1 caso = 1 trace_id = 1 decisão
- lock é liberado em erro controlado
"""

import sys, pytest, uuid
sys.path.insert(0, '/Users/josecorreiasaraivajunior/neuroauth/backend')

from unittest.mock import patch, MagicMock
from app.services.runner import (
    _build_idempotency_key, _get_queue_item, run_episode, ESTADOS_FINAIS
)
from app.models.decide import DecideRequest, OpmeItem


def _req(ep_id: str = None) -> DecideRequest:
    return DecideRequest(
        episodio_id=ep_id or f"EP-TEST-{str(uuid.uuid4())[:6].upper()}",
        cid_principal="M50.1",
        procedimento="Artrodese cervical anterior ACDF discectomia cervical anterior",
        convenio="Unimed Cariri",
        cod_cbhpm="3.07.13.02-1",
        indicacao_clinica="Hernia cervical C5-C6 mielopatia compressiva deficit motor Lhermitte RM compressao medular",
        achados_resumo="RM cervical hernia C5-C6 compressao medular confirmada",
        tto_conservador="Fisioterapia 8 semanas analgesia",
        necessita_opme="Sim",
        opme_items=[OpmeItem(descricao="Cage PEEK C5-C6", qtd=1, fabricante="Synthes")],
        crm="CE-12345", cbo="225120",
        medico_solicitante="Dr. Teste Noite7"
    )


class TestN7Idempotencia:

    def test_T701_idempotency_key_deterministica(self):
        """T7-01: mesma request → mesma idempotency_key."""
        req = _req("EP-FIXED")
        k1 = _build_idempotency_key(req)
        k2 = _build_idempotency_key(req)
        assert k1 == k2, "idempotency_key deve ser determinística"

    def test_T702_keys_diferentes_para_requests_diferentes(self):
        """T7-02: requests diferentes → keys diferentes."""
        req1 = _req("EP-A")
        req2 = _req("EP-B")
        assert _build_idempotency_key(req1) != _build_idempotency_key(req2)

    def test_T703_chave_contem_engine_version(self):
        """T7-03: idempotency_key contém engine_version."""
        from app.services.runner import ENGINE_VERSION
        k = _build_idempotency_key(_req())
        assert ENGINE_VERSION in k


class TestN7ControleDeEstado:

    def _mock_ws(self, item_status: str = None, item_idem: str = None):
        """Cria mock de worksheet com item existente."""
        ws = MagicMock()
        if item_status is None:
            ws.get_all_values.return_value = [
                ["queue_item_id","episode_id","trace_id","idempotency_key",
                 "lock_owner","lock_at","attempt_count","last_attempt_at",
                 "final_status","decision_run_id","error_message","created_at","updated_at"]
            ]
        else:
            ws.get_all_values.return_value = [
                ["queue_item_id","episode_id","trace_id","idempotency_key",
                 "lock_owner","lock_at","attempt_count","last_attempt_at",
                 "final_status","decision_run_id","error_message","created_at","updated_at"],
                ["QI-001","EP-TARGET","TR-OLD",item_idem or "k1",
                 "runner-old","2026-01-01","1","2026-01-01",
                 item_status,"DR-OLD","","2026-01-01","2026-01-01"],
            ]
        ws.col_values.return_value = (
            ["episode_id","EP-TARGET"] if item_status else ["episode_id"]
        )
        return ws

    def test_T704_concluido_nao_reprocessa(self):
        """T7-04: episódio concluído retorna skipped_already_done."""
        req = _req("EP-TARGET")
        with patch("app.services.runner._get_queue_sheet") as mock_qs:
            mock_qs.return_value = self._mock_ws("concluido")
            result = run_episode(req)
        assert result["status"] == "skipped_already_done"
        assert result["decision_run_id"] == "DR-OLD"

    def test_T705_lockado_nao_entra(self):
        """T7-05: episódio lockado por outro ciclo retorna skipped_locked."""
        req = _req("EP-TARGET")
        with patch("app.services.runner._get_queue_sheet") as mock_qs:
            mock_qs.return_value = self._mock_ws("lockado")
            result = run_episode(req)
        assert result["status"] == "skipped_locked"
        assert result.get("error") is not None

    def test_T706_processando_nao_entra(self):
        """T7-06: episódio em 'processando' retorna skipped_locked."""
        req = _req("EP-TARGET")
        with patch("app.services.runner._get_queue_sheet") as mock_qs:
            mock_qs.return_value = self._mock_ws("processando")
            result = run_episode(req)
        assert result["status"] == "skipped_locked"


class TestN7FluxoCompleto:

    def _mock_completo(self, ep_id: str):
        """Mock de worksheet vazia (novo episódio)."""
        ws = MagicMock()
        ws.get_all_values.return_value = [
            ["queue_item_id","episode_id","trace_id","idempotency_key",
             "lock_owner","lock_at","attempt_count","last_attempt_at",
             "final_status","decision_run_id","error_message","created_at","updated_at"]
        ]
        # col_values para episode_id: retorna lista sem o ep_id (não existe ainda)
        ws.col_values.return_value = ["episode_id"]
        ws.append_row.return_value = None
        ws.row_values.return_value = [
            "queue_item_id","episode_id","trace_id","idempotency_key",
            "lock_owner","lock_at","attempt_count","last_attempt_at",
            "final_status","decision_run_id","error_message","created_at","updated_at"
        ]
        ws.update_cells.return_value = None
        # Simular find_row_index retornando linha 2 após enqueue
        return ws

    def test_T707_falha_persist_nao_marca_concluido(self):
        """T7-07: falha antes de persistência → status 'erro', não 'concluido'."""
        req = _req()
        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", return_value=False), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._mock_completo(req.episodio_id)
            result = run_episode(req)

        assert result["status"] == "erro"
        assert result["status"] not in ESTADOS_FINAIS - {"erro"}

    def test_T708_falha_verify_nao_marca_concluido(self):
        """T7-08: falha de verify_persistence → status 'erro', não 'concluido'."""
        req = _req()
        verify_fail = {"veredicto": "BLOQUEADO", "tentativas": 3, "detalhes": ["falhou"]}
        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=verify_fail), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._mock_completo(req.episodio_id)
            result = run_episode(req)

        assert result["status"] == "erro"

    def test_T709_um_caso_um_trace_id(self):
        """T7-09: cada chamada gera trace_id único."""
        req1 = _req()
        req2 = _req()
        verify_ok = {"veredicto": "OK", "tentativas": 1, "detalhes": []}

        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=verify_ok), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._mock_completo(req1.episodio_id)
            r1 = run_episode(req1)

        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=verify_ok), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._mock_completo(req2.episodio_id)
            r2 = run_episode(req2)

        assert r1["trace_id"] != r2["trace_id"], "trace_ids devem ser únicos por execução"

    def test_T710_lock_liberado_em_erro(self):
        """T7-10: lock deve ser liberado mesmo quando ocorre erro controlado."""
        req = _req()
        updates_registrados = []

        def fake_update(ws, row_idx, updates):
            updates_registrados.append(updates.copy())

        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", side_effect=RuntimeError("db down")), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None), \
             patch("app.services.runner._update_queue_row", side_effect=fake_update):
            mock_qs.return_value = self._mock_completo(req.episodio_id)
            result = run_episode(req)

        assert result["status"] == "erro"
        # Verificar que houve update com lock_owner="" (liberação do lock)
        liberacoes = [u for u in updates_registrados if u.get("lock_owner") == ""]
        assert len(liberacoes) >= 1, "Lock deve ser liberado em erro controlado"
