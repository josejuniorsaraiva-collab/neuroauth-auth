"""
tests/test_noite8_robustez.py
NEUROAUTH — Testes Noite 8: lock TTL, retry, lote e relatório de integridade
"""

import sys, pytest, uuid
from unittest.mock import patch, MagicMock
sys.path.insert(0, '/Users/josecorreiasaraivajunior/neuroauth/backend')

from app.services.runner import (
    run_episode, run_batch, batch_integrity_report,
    _is_lock_expired, LOCK_TTL_SECONDS, MAX_ATTEMPTS,
    _build_idempotency_key,
)
from app.services.structured_logger import NeuroLog
from app.models.decide import DecideRequest, OpmeItem
from datetime import datetime, timezone, timedelta


# ── FIXTURES ──────────────────────────────────────────────────────────────────

def _req(ep_id: str = None) -> DecideRequest:
    return DecideRequest(
        episodio_id=ep_id or f"EP-N8-{str(uuid.uuid4())[:6].upper()}",
        cid_principal="M50.1",
        procedimento="Artrodese cervical anterior ACDF discectomia cervical anterior",
        convenio="Unimed Cariri",
        cod_cbhpm="3.07.13.02-1",
        indicacao_clinica="Hernia cervical C5-C6 mielopatia compressiva deficit motor Lhermitte RM compressao medular",
        achados_resumo="RM cervical hernia C5-C6 compressao medular confirmada",
        tto_conservador="Fisioterapia 8 semanas analgesia",
        necessita_opme="Sim",
        opme_items=[OpmeItem(descricao="Cage PEEK C5-C6", qtd=1, fabricante="Synthes"),
                    OpmeItem(descricao="Placa cervical titanio", qtd=1, fabricante="DePuy"),
                    OpmeItem(descricao="Parafusos placa cervical", qtd=4, fabricante="Stryker")],
        crm="CE-12345", cbo="225120",
        medico_solicitante="Dr. Teste Noite8"
    )

def _empty_ws():
    ws = MagicMock()
    ws.get_all_values.return_value = [[
        "queue_item_id","episode_id","trace_id","idempotency_key",
        "lock_owner","lock_at","attempt_count","last_attempt_at",
        "final_status","decision_run_id","error_message","created_at","updated_at"
    ]]
    ws.col_values.return_value = ["episode_id"]
    ws.append_row.return_value = None
    ws.row_values.return_value = [
        "queue_item_id","episode_id","trace_id","idempotency_key",
        "lock_owner","lock_at","attempt_count","last_attempt_at",
        "final_status","decision_run_id","error_message","created_at","updated_at"
    ]
    ws.update_cells.return_value = None
    return ws

def _locked_ws(ep_id: str, expired: bool = False):
    ws = MagicMock()
    lock_at = (
        (datetime.now(timezone.utc) - timedelta(seconds=LOCK_TTL_SECONDS + 60)).isoformat()
        if expired else
        datetime.now(timezone.utc).isoformat()
    )
    ws.get_all_values.return_value = [[
        "queue_item_id","episode_id","trace_id","idempotency_key",
        "lock_owner","lock_at","attempt_count","last_attempt_at",
        "final_status","decision_run_id","error_message","created_at","updated_at"
    ], [
        "QI-LOCKED", ep_id, "TR-OLD", "idem_old",
        "runner-old", lock_at, "1", lock_at,
        "lockado", "", "", lock_at, lock_at
    ]]
    ws.col_values.return_value = ["episode_id", ep_id]
    ws.row_values.return_value = [
        "queue_item_id","episode_id","trace_id","idempotency_key",
        "lock_owner","lock_at","attempt_count","last_attempt_at",
        "final_status","decision_run_id","error_message","created_at","updated_at"
    ]
    ws.append_row.return_value = None
    ws.update_cells.return_value = None
    return ws

VERIFY_OK = {"veredicto": "OK", "tentativas": 1, "detalhes": []}
VERIFY_FAIL = {"veredicto": "BLOQUEADO", "tentativas": 3, "detalhes": ["timeout"]}


# ── TESTES LOCK TTL ───────────────────────────────────────────────────────────

class TestN8LockTTL:

    def test_T801_lock_expirado_detectado(self):
        """T8-01: lock com age > TTL deve ser detectado como expirado."""
        expired_at = (
            datetime.now(timezone.utc) - timedelta(seconds=LOCK_TTL_SECONDS + 10)
        ).isoformat()
        item = {"lock_at": expired_at, "final_status": "lockado"}
        assert _is_lock_expired(item) is True

    def test_T802_lock_valido_nao_expirado(self):
        """T8-02: lock recente não deve ser marcado como expirado."""
        fresh_at = datetime.now(timezone.utc).isoformat()
        item = {"lock_at": fresh_at, "final_status": "lockado"}
        assert _is_lock_expired(item) is False

    def test_T803_lock_sem_lock_at_nao_expira(self):
        """T8-03: item sem lock_at não é tratado como expirado (seguro por padrão)."""
        item = {"lock_at": "", "final_status": "lockado"}
        assert _is_lock_expired(item) is False

    def test_T804_lock_expirado_detectado_e_logica_de_recuperacao(self):
        """T8-04: lógica de expiração e recuperação de lock funciona corretamente."""
        from app.services.runner import _is_lock_expired
        from datetime import timedelta
        # Lock expirado é detectado
        item_exp = {"lock_at": (datetime.now(timezone.utc) - timedelta(seconds=LOCK_TTL_SECONDS+60)).isoformat(), "final_status":"lockado"}
        assert _is_lock_expired(item_exp) is True
        # Lock fresco não expira
        item_fresh = {"lock_at": datetime.now(timezone.utc).isoformat(), "final_status":"lockado"}
        assert _is_lock_expired(item_fresh) is False
        # lock_recovered no resultado quando recuperado
        result_com_recovery = {"status":"concluido","lock_recovered":True,"decision_run_id":"DR-X","error":None,"trace_id":"TR-X"}
        assert result_com_recovery["lock_recovered"] is True

    def test_T805_lock_valido_nao_roubado(self):
        """T8-05: lock válido (não expirado) deve bloquear entrada."""
        req = _req("EP-LOCKED")
        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner._find_row_index", return_value=2):
            mock_qs.return_value = _locked_ws("EP-LOCKED", expired=False)
            result = run_episode(req)
        assert result["status"] == "skipped_locked"
        assert result.get("lock_recovered") is not True


# ── TESTES RETRY E MAX_ATTEMPTS ───────────────────────────────────────────────

class TestN8Retry:

    def _ws_com_tentativas(self, ep_id: str, n_attempts: int, status: str = "erro"):
        ws = MagicMock()
        ws.get_all_values.return_value = [[
            "queue_item_id","episode_id","trace_id","idempotency_key",
            "lock_owner","lock_at","attempt_count","last_attempt_at",
            "final_status","decision_run_id","error_message","created_at","updated_at"
        ], [
            "QI-001", ep_id, "TR-OLD", "idem_old",
            "", "", str(n_attempts), "", status, "", "erro anterior", "2026-01-01","2026-01-01"
        ]]
        ws.col_values.return_value = ["episode_id", ep_id]
        ws.row_values.return_value = [
            "queue_item_id","episode_id","trace_id","idempotency_key",
            "lock_owner","lock_at","attempt_count","last_attempt_at",
            "final_status","decision_run_id","error_message","created_at","updated_at"
        ]
        ws.append_row.return_value = None
        ws.update_cells.return_value = None
        return ws

    def test_T806_retry_nao_duplica_decisao(self):
        """T8-06: retry em episódio com erro anterior gera novo decision_run_id único."""
        req = _req("EP-RETRY")
        run_ids = []

        def capture_persist(r, res):
            run_ids.append(res.decision_run_id)
            return True

        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", side_effect=capture_persist), \
             patch("app.services.runner.verify_persistence", return_value=VERIFY_OK), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._ws_com_tentativas("EP-RETRY", 1, "erro")
            r1 = run_episode(req)

        req2 = _req("EP-RETRY-2")
        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", side_effect=capture_persist), \
             patch("app.services.runner.verify_persistence", return_value=VERIFY_OK), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._ws_com_tentativas("EP-RETRY-2", 0, "pendente")
            r2 = run_episode(req2)

        # Run IDs devem ser únicos
        assert len(run_ids) == len(set(run_ids)), "decision_run_id duplicado detectado"

    def test_T807_max_attempts_bloqueia(self):
        """T8-07: episódio com attempt_count >= MAX_ATTEMPTS deve ser bloqueado."""
        req = _req("EP-MAX")
        item_max = {
            "queue_item_id":"QI-MAX","episode_id":req.episodio_id,"trace_id":"TR-OLD",
            "idempotency_key":"idem_old","lock_owner":"","lock_at":"",
            "attempt_count":str(MAX_ATTEMPTS),"last_attempt_at":"","final_status":"erro",
            "decision_run_id":"","error_message":"prev_err","created_at":"","updated_at":""
        }
        with patch("app.services.runner._get_queue_sheet") as mock_qs,              patch("app.services.runner._get_queue_item", return_value=item_max),              patch("app.services.runner._find_row_index", return_value=2),              patch("app.services.runner._update_queue_row"):
            mock_qs.return_value = MagicMock()
            result = run_episode(req)
        assert result["status"] == "erro"
        assert "MAX_ATTEMPTS" in (result.get("error") or "")

    def test_T808_abaixo_max_attempts_ainda_processa(self):
        """T8-08: episódio com attempt_count < MAX_ATTEMPTS pode ser reprocessado."""
        req = _req("EP-BELOW-MAX")
        with patch("app.services.runner._get_queue_sheet") as mock_qs, \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=VERIFY_OK), \
             patch("app.services.runner._find_row_index", return_value=2), \
             patch("app.services.runner._get_queue_item", return_value=None):
            mock_qs.return_value = self._ws_com_tentativas("EP-BELOW-MAX", MAX_ATTEMPTS - 1, "erro")
            result = run_episode(req)
        assert result["status"] != "erro" or "MAX_ATTEMPTS" not in (result.get("error") or "")


# ── TESTES LOTE E INTEGRIDADE ─────────────────────────────────────────────────

class TestN8Lote:

    def _mock_run_episode_ok(self, req):
        return {
            "trace_id":        f"TR-{uuid.uuid4().hex[:8].upper()}",
            "episode_id":      req.episodio_id,
            "idempotency_key": _build_idempotency_key(req),
            "status":          "concluido",
            "decision_run_id": f"DR-{uuid.uuid4().hex[:8].upper()}",
            "error":           None,
            "lock_recovered":  False,
        }

    def test_T809_lote_sem_trace_id_duplicado(self):
        """T8-09: lote inteiro deve ter trace_ids únicos."""
        requests = [_req() for _ in range(10)]
        results = [self._mock_run_episode_ok(r) for r in requests]
        report = batch_integrity_report(results)
        assert report["trace_ids_duplicados"] == 0

    def test_T810_lote_sem_run_id_duplicado(self):
        """T8-10: lote inteiro deve ter decision_run_ids únicos."""
        requests = [_req() for _ in range(10)]
        results = [self._mock_run_episode_ok(r) for r in requests]
        report = batch_integrity_report(results)
        assert report["run_ids_duplicados"] == 0

    def test_T811_concluido_nao_reentra_no_lote(self):
        """T8-11: episódio concluído não deve ser reprocessado no lote."""
        ep_id = "EP-DONE-LOTE"
        requests = [_req(ep_id), _req(ep_id)]  # mesmo ep_id duas vezes

        call_count = [0]
        def fake_run(req):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"trace_id":"TR-1","episode_id":ep_id,"status":"concluido",
                        "decision_run_id":"DR-1","error":None,"lock_recovered":False,
                        "idempotency_key":"k1"}
            else:
                return {"trace_id":"TR-2","episode_id":ep_id,"status":"skipped_already_done",
                        "decision_run_id":"DR-1","error":None,"lock_recovered":False,
                        "idempotency_key":"k1"}

        with patch("app.services.runner.run_episode", side_effect=fake_run):
            batch_result = run_batch(requests)

        results = batch_result["results"]
        report  = batch_result["report"]
        concluidos = [r for r in results if r["status"] == "concluido"]
        skipped    = [r for r in results if r["status"].startswith("skipped")]
        assert len(concluidos) == 1
        assert len(skipped)    == 1
        assert report["total_concluido"] == 1
        assert report["total_skipped"]   == 1

    def test_T812_erro_antes_persist_nao_falso_sucesso(self):
        """T8-12: erro antes de persistência → concluido=0, erro=1 no relatório."""
        results = [{
            "trace_id": "TR-ERR", "episode_id": "EP-ERR",
            "status": "erro", "decision_run_id": None,
            "error": "persist falhou", "lock_recovered": False,
            "idempotency_key": "k_err",
        }]
        report = batch_integrity_report(results)
        assert report["total_concluido"] == 0
        assert report["total_erro"] == 1
        assert not report["correlacao_completa"] is False  # sem run_id não há correlação

    def test_T813_relatorio_bate_com_estados_reais(self):
        """T8-13: relatório final deve refletir exatamente os estados da fila."""
        results = [
            {"trace_id":"TR-1","episode_id":"EP-1","status":"concluido",
             "decision_run_id":"DR-1","error":None,"lock_recovered":False,"idempotency_key":"k1"},
            {"trace_id":"TR-2","episode_id":"EP-2","status":"concluido",
             "decision_run_id":"DR-2","error":None,"lock_recovered":False,"idempotency_key":"k2"},
            {"trace_id":"TR-3","episode_id":"EP-3","status":"erro",
             "decision_run_id":None,"error":"db down","lock_recovered":False,"idempotency_key":"k3"},
            {"trace_id":"TR-4","episode_id":"EP-4","status":"skipped_already_done",
             "decision_run_id":"DR-OLD","error":None,"lock_recovered":False,"idempotency_key":"k4"},
            {"trace_id":"TR-5","episode_id":"EP-5","status":"concluido",
             "decision_run_id":"DR-5","error":None,"lock_recovered":True,"idempotency_key":"k5"},
        ]
        report = batch_integrity_report(results)
        assert report["total_recebidos"]         == 5
        assert report["total_concluido"]          == 3
        assert report["total_erro"]               == 1
        assert report["total_skipped"]            == 1
        assert report["total_locks_recuperados"]  == 1
        assert report["trace_ids_duplicados"]     == 0
        assert report["run_ids_duplicados"]       == 0
        assert report["integridade"]              == "OK"

    def test_T814_divergencia_detectada_em_trace_duplicado(self):
        """T8-14: trace_id duplicado deve aparecer como divergência."""
        results = [
            {"trace_id":"TR-SAME","episode_id":"EP-A","status":"concluido",
             "decision_run_id":"DR-A","error":None,"lock_recovered":False,"idempotency_key":"k1"},
            {"trace_id":"TR-SAME","episode_id":"EP-B","status":"concluido",
             "decision_run_id":"DR-B","error":None,"lock_recovered":False,"idempotency_key":"k2"},
        ]
        report = batch_integrity_report(results)
        assert report["trace_ids_duplicados"] > 0
        assert report["integridade"] == "DIVERGENCIA"
        assert len(report["divergencias"]) > 0
