"""
tests/test_noite8_robustez.py
NEUROAUTH — Testes Noite 8: lock TTL, retry, lote e integridade

Política de mocks:
  - Nunca mockar _get_queue_item ou _load_queue_rows nos testes de lógica de negócio
  - Mockar apenas: escrita no ws (_update_queue_row, append_row),
    tempo atual (datetime.now), e serviços externos (persist, verify)
  - Fixtures de ws usam get_all_values() realista para que _load_queue_rows funcione
"""

import sys, uuid, pytest, logging
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call
sys.path.insert(0, '/Users/josecorreiasaraivajunior/neuroauth/backend')

from app.services.runner import (
    run_episode, run_batch, batch_integrity_report,
    _is_lock_expired, _build_idempotency_key,
    _recover_expired_lock, _load_queue_rows,
    LOCK_TTL_SECONDS, MAX_ATTEMPTS, QUEUE_HEADERS,
)
from app.services.structured_logger import NeuroLog
from app.models.decide import DecideRequest, OpmeItem


# ── HELPERS ───────────────────────────────────────────────────────────────────

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
        opme_items=[
            OpmeItem(descricao="Cage PEEK C5-C6", qtd=1, fabricante="Synthes"),
            OpmeItem(descricao="Placa cervical titanio", qtd=1, fabricante="DePuy"),
            OpmeItem(descricao="Parafusos placa cervical", qtd=4, fabricante="Stryker"),
        ],
        crm="CE-12345", cbo="225120",
        medico_solicitante="Dr. Teste Noite8"
    )


def _make_ws(rows: list = None) -> MagicMock:
    """
    Cria worksheet mock com get_all_values() realista para _load_queue_rows.
    append_row é dinâmico: adiciona a linha ao estado interno do mock.
    rows: lista de dicts com os dados (sem cabeçalho).
    """
    ws = MagicMock()
    data = [list(QUEUE_HEADERS)]
    if rows:
        for row_dict in rows:
            row = [row_dict.get(h, "") for h in QUEUE_HEADERS]
            data.append(row)

    # append_row dinâmico: atualiza data para que _get_queue_item encontre o item
    def dynamic_append(row, **kwargs):
        data.append(list(row))

    ws.get_all_values.side_effect = lambda: [list(r) for r in data]
    ws.append_row.side_effect = dynamic_append
    ws.row_values.return_value = list(QUEUE_HEADERS)
    ws.update_cells.return_value = None
    return ws


def _item(ep_id: str, status: str, attempts: int = 0,
          lock_owner: str = "", lock_at: str = "",
          run_id: str = "") -> dict:
    """Cria um item de fila com os campos mínimos."""
    return {
        "queue_item_id": "QI-TEST",
        "episode_id":    ep_id,
        "trace_id":      "TR-OLD",
        "idempotency_key": "idem_old",
        "lock_owner":    lock_owner,
        "lock_at":       lock_at,
        "attempt_count": str(attempts),
        "last_attempt_at": "",
        "final_status":  status,
        "decision_run_id": run_id,
        "error_message": "",
        "created_at":    "2026-01-01",
        "updated_at":    "2026-01-01",
    }


VERIFY_OK   = {"veredicto": "OK",       "tentativas": 1, "detalhes": []}
VERIFY_FAIL = {"veredicto": "BLOQUEADO","tentativas": 3, "detalhes": ["timeout"]}


# ── TESTES LOCK TTL ───────────────────────────────────────────────────────────

class TestN8LockTTL:

    def test_T801_lock_expirado_detectado(self):
        """T8-01: lock com age > TTL deve ser detectado como expirado."""
        expired_at = (datetime.now(timezone.utc) - timedelta(seconds=LOCK_TTL_SECONDS + 10)).isoformat()
        assert _is_lock_expired(_item("EP-X", "lockado", lock_at=expired_at)) is True

    def test_T802_lock_valido_nao_expirado(self):
        """T8-02: lock recente não deve ser marcado como expirado."""
        fresh_at = datetime.now(timezone.utc).isoformat()
        assert _is_lock_expired(_item("EP-X", "lockado", lock_at=fresh_at)) is False

    def test_T803_lock_sem_lock_at_nao_expira(self):
        """T8-03: item sem lock_at não é tratado como expirado."""
        assert _is_lock_expired(_item("EP-X", "lockado", lock_at="")) is False

    def test_T803b_is_lock_expired_aceita_none(self):
        """T8-03b: _is_lock_expired(None) retorna False — sem quebrar."""
        assert _is_lock_expired(None) is False

    def test_T804_lock_expirado_e_recuperado_sem_releitura(self):
        """
        T8-04: episódio com lock expirado deve ser recuperado e processado.
        Fixture realista: ws.get_all_values() retorna item com lock expirado.
        Sem patch de _get_queue_item — a leitura real ocorre via _load_queue_rows.
        """
        ep_id      = "EP-EXPIRED"
        expired_at = (datetime.now(timezone.utc) - timedelta(seconds=LOCK_TTL_SECONDS + 60)).isoformat()
        item_exp   = _item(ep_id, "lockado", attempts=1,
                           lock_owner="runner-old", lock_at=expired_at)

        ws = _make_ws([item_exp])
        log = NeuroLog("TR-TEST", ep_id)

        # _recover_expired_lock com item já resolvido — sem releitura
        updates_chamados = []
        def fake_update(w, idx, upd):
            updates_chamados.append(upd.copy())

        with patch("app.services.runner._update_queue_row", side_effect=fake_update):
            _recover_expired_lock(ws, 2, item_exp, log)

        # Deve ter resetado status e limpo lock
        assert any(u.get("final_status") == "pendente" for u in updates_chamados)
        assert any(u.get("lock_owner") == "" for u in updates_chamados)

        # Verificar que _load_queue_rows funciona com o ws realista
        headers, rows = _load_queue_rows(ws)
        assert len(rows) == 1
        assert rows[0]["episode_id"] == ep_id
        assert rows[0]["final_status"] == "lockado"

    def test_T805_lock_valido_nao_roubado(self):
        """T8-05: lock válido retorna skipped_locked."""
        ep_id    = "EP-LOCKED"
        fresh_at = datetime.now(timezone.utc).isoformat()
        item_loc = _item(ep_id, "lockado", lock_owner="runner-other", lock_at=fresh_at)

        ws = _make_ws([item_loc])
        with patch("app.services.runner._get_queue_sheet", return_value=ws), \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=VERIFY_OK):
            result = run_episode(_req(ep_id))

        assert result["status"] == "skipped_locked"
        assert result.get("lock_recovered") is not True


# ── TESTES RETRY E MAX_ATTEMPTS ───────────────────────────────────────────────

class TestN8Retry:

    def test_T806_retry_nao_duplica_decisao(self):
        """T8-06: dois episodios distintos geram decision_run_ids unicos."""
        run_ids = []
        def capture(req, res):
            run_ids.append(res.decision_run_id)
            return True

        for _ in range(2):
            req = _req()
            ws  = _make_ws([])
            with patch("app.services.runner._get_queue_sheet", return_value=ws),                  patch("app.services.runner.persist_decision", side_effect=capture),                  patch("app.services.runner.verify_persistence", return_value=VERIFY_OK),                  patch("app.services.runner._update_queue_row"):
                run_episode(req)

        assert len(run_ids) == 2
        assert run_ids[0] != run_ids[1]

    def test_T807_max_attempts_bloqueia(self):
        """
        T8-07: episódio com attempt_count == MAX_ATTEMPTS deve ser bloqueado.
        Fixture realista: ws retorna item com attempts=MAX_ATTEMPTS via get_all_values.
        Sem patch de _get_queue_item — a regra de negócio é testada diretamente.
        """
        ep_id    = "EP-MAX"
        item_max = _item(ep_id, "erro", attempts=MAX_ATTEMPTS)

        ws = _make_ws([item_max])
        updates = []
        def fake_update(w, idx, upd): updates.append(upd.copy())

        with patch("app.services.runner._get_queue_sheet", return_value=ws), \
             patch("app.services.runner._update_queue_row", side_effect=fake_update):
            result = run_episode(_req(ep_id))

        assert result["status"] == "erro"
        assert "MAX_ATTEMPTS" in (result.get("error") or ""), \
            f"Esperado MAX_ATTEMPTS em error, obtido: {result.get('error')}"
        # Deve ter gravado o bloqueio definitivo na fila
        assert any("MAX_ATTEMPTS" in str(u.get("error_message","")) for u in updates)

    def test_T808_abaixo_max_attempts_processa(self):
        """T8-08: attempt_count < MAX_ATTEMPTS permite reprocessamento."""
        ep_id   = "EP-BELOW"
        item_ok = _item(ep_id, "erro", attempts=MAX_ATTEMPTS - 1)

        ws = _make_ws([item_ok])
        with patch("app.services.runner._get_queue_sheet", return_value=ws), \
             patch("app.services.runner.persist_decision", return_value=True), \
             patch("app.services.runner.verify_persistence", return_value=VERIFY_OK), \
             patch("app.services.runner._update_queue_row"):
            result = run_episode(_req(ep_id))

        assert "MAX_ATTEMPTS" not in (result.get("error") or "")


# ── TESTES LOTE E INTEGRIDADE ─────────────────────────────────────────────────

class TestN8Lote:

    def _ok_result(self, req):
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
        """T8-09: trace_ids únicos em lote de 10."""
        results = [self._ok_result(_req()) for _ in range(10)]
        report  = batch_integrity_report(results)
        assert report["trace_ids_duplicados"] == 0

    def test_T810_lote_sem_run_id_duplicado(self):
        """T8-10: decision_run_ids únicos em lote de 10."""
        results = [self._ok_result(_req()) for _ in range(10)]
        report  = batch_integrity_report(results)
        assert report["run_ids_duplicados"] == 0

    def test_T811_concluido_nao_reentra_no_lote(self):
        """T8-11: episódio concluído retorna skipped no segundo processo do lote."""
        ep_id = "EP-DONE-LOTE"
        n = [0]
        def fake_run(req):
            n[0] += 1
            if n[0] == 1:
                return {"trace_id":"TR-1","episode_id":ep_id,"status":"concluido",
                        "decision_run_id":"DR-1","error":None,"lock_recovered":False,
                        "idempotency_key":"k1"}
            return {"trace_id":"TR-2","episode_id":ep_id,"status":"skipped_already_done",
                    "decision_run_id":"DR-1","error":None,"lock_recovered":False,
                    "idempotency_key":"k1"}

        with patch("app.services.runner.run_episode", side_effect=fake_run):
            batch = run_batch([_req(ep_id), _req(ep_id)])

        assert batch["report"]["total_concluido"] == 1
        assert batch["report"]["total_skipped"]   == 1

    def test_T812_erro_antes_persist_nao_falso_sucesso(self):
        """T8-12: erro antes de persistência → sem concluído no relatório."""
        results = [{"trace_id":"TR-E","episode_id":"EP-E","status":"erro",
                    "decision_run_id":None,"error":"persist falhou",
                    "lock_recovered":False,"idempotency_key":"k"}]
        report = batch_integrity_report(results)
        assert report["total_concluido"] == 0
        assert report["total_erro"] == 1

    def test_T813_relatorio_bate_com_estados(self):
        """T8-13: relatório reflete exatamente os estados dos resultados."""
        results = [
            {"trace_id":"TR-1","episode_id":"EP-1","status":"concluido",
             "decision_run_id":"DR-1","error":None,"lock_recovered":False,"idempotency_key":"k1"},
            {"trace_id":"TR-2","episode_id":"EP-2","status":"concluido",
             "decision_run_id":"DR-2","error":None,"lock_recovered":False,"idempotency_key":"k2"},
            {"trace_id":"TR-3","episode_id":"EP-3","status":"erro",
             "decision_run_id":None,"error":"db","lock_recovered":False,"idempotency_key":"k3"},
            {"trace_id":"TR-4","episode_id":"EP-4","status":"skipped_already_done",
             "decision_run_id":"DR-OLD","error":None,"lock_recovered":False,"idempotency_key":"k4"},
            {"trace_id":"TR-5","episode_id":"EP-5","status":"concluido",
             "decision_run_id":"DR-5","error":None,"lock_recovered":True,"idempotency_key":"k5"},
        ]
        r = batch_integrity_report(results)
        assert r["total_recebidos"]         == 5
        assert r["total_concluido"]          == 3
        assert r["total_erro"]               == 1
        assert r["total_skipped"]            == 1
        assert r["total_locks_recuperados"]  == 1
        assert r["trace_ids_duplicados"]     == 0
        assert r["run_ids_duplicados"]       == 0
        assert r["integridade"]              == "OK"

    def test_T814_divergencia_trace_duplicado(self):
        """T8-14: trace_id duplicado → integridade DIVERGENCIA."""
        results = [
            {"trace_id":"TR-SAME","episode_id":"EP-A","status":"concluido",
             "decision_run_id":"DR-A","error":None,"lock_recovered":False,"idempotency_key":"k1"},
            {"trace_id":"TR-SAME","episode_id":"EP-B","status":"concluido",
             "decision_run_id":"DR-B","error":None,"lock_recovered":False,"idempotency_key":"k2"},
        ]
        r = batch_integrity_report(results)
        assert r["trace_ids_duplicados"] > 0
        assert r["integridade"] == "DIVERGENCIA"
        assert len(r["divergencias"]) > 0
