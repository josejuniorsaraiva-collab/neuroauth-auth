"""
tests/test_noite6_observabilidade.py
NEUROAUTH — Testes de Noite 6: observabilidade estruturada
"""

import json, logging, sys, pytest
sys.path.insert(0, '/Users/josecorreiasaraivajunior/neuroauth/backend')

from app.services.structured_logger import NeuroLog, VALID_EVENTS

CAMPOS_OBRIGATORIOS = [
    "timestamp_utc","trace_id","episode_id","decision_run_id",
    "event_name","service_name","status","latency_ms","details_json",
]


class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []
    def emit(self, record):
        try:
            self.records.append(json.loads(record.getMessage()))
        except Exception:
            pass
    def get_events(self): return [r["event_name"] for r in self.records if "event_name" in r]
    def get_by(self, ev): return [r for r in self.records if r.get("event_name") == ev]
    def clear(self): self.records.clear()


@pytest.fixture
def cap():
    h = LogCapture()
    h.setLevel(logging.DEBUG)
    lg = logging.getLogger("neuroauth.structured")
    lg.setLevel(logging.DEBUG)
    lg.addHandler(h)
    yield h
    lg.removeHandler(h)
    h.clear()


class TestN6CamposObrigatorios:

    def test_T601_campos_presentes(self, cap):
        """T6-01: 9 campos obrigatórios em todo evento."""
        NeuroLog("TR-T601","EP-T601","DR-T601").emit("decision_started",details={"v":"v2"})
        for campo in CAMPOS_OBRIGATORIOS:
            assert campo in cap.records[0], f"Campo ausente: {campo}"

    def test_T602_trace_id_100pct(self, cap):
        """T6-02: trace_id em 100% dos eventos."""
        log = NeuroLog("TR-FIXED","EP-T602","DR-T602")
        for ev in ["request_received","decision_started","rules_applied",
                   "decision_result","persist_start","persist_success",
                   "verify_success","response_sent"]:
            log.emit(ev, status="ok")
        for r in cap.records:
            assert r.get("trace_id") == "TR-FIXED", f"trace_id ausente em {r.get('event_name')}"

    def test_T603_episode_e_run_id(self, cap):
        """T6-03: episode_id e decision_run_id em todos os eventos."""
        log = NeuroLog("TR-T603","EP-FIXED","DR-FIXED")
        log.emit("rules_applied", details={"rule_count":14})
        log.emit("decision_result", details={"final_decision":"GO"})
        for r in cap.records:
            assert r["episode_id"] == "EP-FIXED"
            assert r["decision_run_id"] == "DR-FIXED"

    def test_T604_latency_e_timestamp(self, cap):
        """T6-04: latency_ms inteiro >= 0, timestamp_utc contém T."""
        NeuroLog("TR-T604","EP-T604").emit("response_sent",details={"http_status":200})
        r = cap.records[0]
        assert isinstance(r["latency_ms"], int) and r["latency_ms"] >= 0
        assert "T" in r["timestamp_utc"]

    def test_T605_status_valido(self, cap):
        """T6-05: status ok e error emitidos corretamente."""
        log = NeuroLog("TR-T605","EP-T605","DR-T605")
        log.emit("decision_started", status="ok")
        log.error("persist_decision","RuntimeError","falha de escrita")
        statuses = [r["status"] for r in cap.records]
        assert "ok" in statuses and "error" in statuses


class TestN6OrdemECorrelacao:

    def test_T606_ordem_cronologica(self, cap):
        """T6-06: eventos em ordem correta."""
        log = NeuroLog("TR-T606","EP-T606","DR-T606")
        ordem = ["request_received","decision_started","rules_applied",
                 "decision_result","persist_start","persist_success",
                 "verify_success","response_sent"]
        for ev in ordem: log.emit(ev, status="ok")
        assert cap.get_events() == ordem

    def test_T607_error_captura_estagio(self, cap):
        """T6-07: error_occurred registra o estágio exato."""
        log = NeuroLog("TR-T607","EP-T607","DR-T607")
        log.error("persist_decision","ConnectionError","timeout sheets")
        erros = cap.get_by("error_occurred")
        assert len(erros) == 1
        assert erros[0]["details_json"]["failed_stage"] == "persist_decision"
        assert erros[0]["status"] == "error"

    def test_T608_persist_antes_verify(self, cap):
        """T6-08: persist_success precede verify_success."""
        log = NeuroLog("TR-T608","EP-T608","DR-T608")
        log.emit("persist_success", details={"persisted_decision_run_id":"DR-T608"})
        log.emit("verify_success", details={"correlation_ok":True})
        evs = cap.get_events()
        assert evs.index("persist_success") < evs.index("verify_success")

    def test_T609_reconstrucao_completa(self, cap):
        """T6-09: reconstrução do caso completo apenas pelos logs."""
        TR, EP, RUN = "TR-T609","EP-T609","DR-T609"
        log = NeuroLog(TR, EP, RUN)
        log.emit("request_received", details={"procedimento":"ACDF","convenio":"Unimed Cariri"})
        log.emit("decision_started", details={"engine_version":"v2.2"})
        log.emit("rules_applied",    details={"pendencias_count":1})
        log.emit("decision_result",  details={"final_decision":"GO","score":100})
        log.emit("persist_success",  details={"persisted_decision_run_id":RUN})
        log.emit("verify_success",   details={"correlation_ok":True})
        log.emit("response_sent",    details={"http_status":200})

        for r in cap.records:
            assert r["trace_id"] == TR
            assert r["episode_id"] == EP
            assert r["decision_run_id"] == RUN

        dr = cap.get_by("decision_result")[0]
        assert dr["details_json"]["final_decision"] == "GO"
        assert dr["details_json"]["score"] == 100
