"""
Regras da aba 09_REGRAS_DECISAO (Google Sheets) avaliadas como GATILHO.
As 10 regras abaixo reproduzem a planilha de produção em 2026-10-22.
Sem rede: o adapter é substituído por estas regras. Dados 100% fictícios.
"""
import pytest

from app.services import rule_library_adapter as rla
from app.services.decision_engine import decide, _req_to_ctx
from app.models.decide import DecideRequest, OpmeItem


def _regra(rule_id, layer, if_json, bloqueia, operador="GLOBAL", prioridade=50):
    return {
        "rule_id": rule_id, "rule_name": rule_id.lower(), "layer": layer, "status": "ACTIVE",
        "priority": prioridade, "severity": "CRITICA" if bloqueia else "MODERADA", "blocking": bloqueia,
        "operator_name": operador, "applies_if_json": if_json, "validation_logic_json": if_json,
        "_trigger": True, "failure_action": "BLOCK" if bloqueia else "WARN",
        "gate_if_fail": "NO_GO" if bloqueia else "GO_COM_RESSALVAS", "score_impact": -30 if bloqueia else -10,
        "user_message": f"mensagem {rule_id}", "source_type": "ANS",
    }


REGRAS_PLANILHA = [
    _regra("REGRA_0001", "ANS", '{"opme_context.numero_marcas":{"lt":3}}', True, prioridade=10),
    _regra("REGRA_0002", "EVIDENCIA", '{"clinical_context.indicacao_len":{"lt":50}}', False, prioridade=20),
    _regra("REGRA_0003", "EVIDENCIA", '{"niveis":{"gte":2},"carater":"eletivo"}', False, "UNIMED_CARIRI", 30),
    _regra("REGRA_0004", "ANS", '{"clinical_context.tto_conservador_semanas":{"lt":6},"convenio_id":"SULAMERICA"}', True, "SULAMERICA", 10),
    _regra("REGRA_0005", "ANS", '{"convenio_id":"HAPVIDA","episodio.numero_guia_operadora":{"exists":false}}', True, "HAPVIDA", 10),
    _regra("REGRA_0006", "EVIDENCIA", '{"profile_id":"ARTROPLASTIA_CERVICAL"}', False, prioridade=15),
    _regra("REGRA_0007", "EVIDENCIA", '{"profile_id":"CLIPAGEM_ANEURISMA"}', False, prioridade=5),
    _regra("REGRA_0008", "OPERADORA", '{"profile_id":"TROMBECTOMIA_MECANICA"}', False, prioridade=1),
    _regra("REGRA_0009", "EVIDENCIA", '{"cbo_executor":{"not_in":"cbos_permitidos_profile"}}', False, prioridade=25),
    _regra("REGRA_0010", "OPERADORA", '{"convenio_id":"BRADESCO_SAUDE"}', False, "BRADESCO_SAUDE", 5),
]

IND = ("Paciente fictício com hérnia discal L4-L5 à esquerda e radiculopatia L5 esquerda, dor ciática "
       "incapacitante EVA 9/10, déficit motor grau 4 em dorsiflexão do hálux esquerdo, sem melhora após "
       "12 semanas de tratamento conservador com fisioterapia, AINE e bloqueio radicular. Sinais e sintomas "
       "correlacionam com o achado de imagem no mesmo nível e lado.")
BASE = dict(cid_principal="M51.1", procedimento="Microdiscectomia lombar L4-L5 (FICTÍCIO)", cod_cbhpm="3.07.14.02-5",
            convenio="UNIMED", crm="00000-CE", cbo="225118", medico_solicitante="Dr. Teste",
            indicacao_clinica=IND, achados_resumo="RM fictícia: hérnia L4-L5 esquerda comprimindo raiz L5.",
            tto_conservador="Sim — 12 semanas de fisioterapia e AINE", necessita_opme="Não")


def _opme(*fabricantes):
    return dict(necessita_opme="Sim", procedimento="Artrodese lombar L4-L5 (FICTÍCIO)",
                opme_items=[OpmeItem(descricao=f"Item {i}", qtd=1, fabricante=f) for i, f in enumerate(fabricantes)],
                justificativas_opme={f"Item {i}": "Fusão intersomática (fictício)." for i in range(len(fabricantes))})


@pytest.fixture(autouse=True)
def regras_da_planilha(monkeypatch):
    monkeypatch.setattr(rla, "load_rules", lambda layer=None: [r for r in REGRAS_PLANILHA if layer in (None, r["layer"])])
    monkeypatch.setattr(rla, "get_rules_by_layer", lambda layer: [r for r in REGRAS_PLANILHA if r["layer"] == layer])
    monkeypatch.setattr(rla, "get_operator_profiles", lambda: {})


def _rodar(**mud):
    d = decide(_req_to_ctx(DecideRequest(**{**BASE, **mud})))
    falhas = {r["rule_id"] for c in ("ans", "evidencia", "operadora")
              for r in d["layer_results"][c].get("results", []) if r.get("passed") is False}
    return d["final_gate"], falhas


# ── Semântica do gatilho (unidade) ─────────────────────────────────────────────
@pytest.mark.parametrize("regra,ctx,aplica", [
    ("REGRA_0001", {"opme_context": {"numero_marcas": 1}}, True),
    ("REGRA_0001", {"opme_context": {"numero_marcas": 3}}, False),
    ("REGRA_0001", {}, False),                                         # sem OPME → não dispara
    ("REGRA_0002", {"clinical_context": {"indicacao_len": 20}}, True),
    ("REGRA_0002", {"clinical_context": {"indicacao_len": 80}}, False),
    ("REGRA_0004", {"convenio_id": "SULAMERICA", "clinical_context": {"tto_conservador_semanas": 2}}, True),
    ("REGRA_0004", {"convenio_id": "SULAMERICA", "clinical_context": {"tto_conservador_semanas": 8}}, False),
    ("REGRA_0004", {"convenio_id": "UNIMED_CARIRI", "clinical_context": {"tto_conservador_semanas": 2}}, False),
    ("REGRA_0005", {"convenio_id": "HAPVIDA", "episodio": {}}, True),
    ("REGRA_0005", {"convenio_id": "HAPVIDA", "episodio": {"numero_guia_operadora": "123"}}, False),
    ("REGRA_0009", {"cbo": "225118"}, False),                          # cbo_executor ausente → não dispara
    ("REGRA_0010", {"convenio_id": "BRADESCO_SAUDE"}, True),
    ("REGRA_0010", {"convenio_id": "UNIMED_CARIRI"}, False),
])
def test_gatilho_por_regra(regra, ctx, aplica):
    r = next(x for x in REGRAS_PLANILHA if x["rule_id"] == regra)
    aplicou, passou = rla.avaliar_regra(r, ctx)
    assert aplicou is aplica and passou is (not aplica)


def test_regra_sem_trigger_mantem_semantica_original():
    r = {"rule_id": "X", "operator_name": "GLOBAL", "applies_if_json": '{"a":{"gte":1}}',
         "validation_logic_json": '{"b":{"gte":5}}'}
    assert rla.avaliar_regra(r, {"a": 1, "b": 7}) == (True, True)
    assert rla.avaliar_regra(r, {"a": 1, "b": 2}) == (True, False)


@pytest.mark.parametrize("nome,esperado", [
    ("UNIMED", "UNIMED_CARIRI"), ("Unimed Cariri", "UNIMED_CARIRI"), ("SULAMÉRICA", "SULAMERICA"),
    ("Sul America", "SULAMERICA"), ("BRADESCO SAUDE", "BRADESCO_SAUDE"), ("Hapvida", "HAPVIDA"),
    ("PARTICULAR", "PARTICULAR"), ("", "GLOBAL"),
])
def test_convenio_id(nome, esperado):
    assert rla.convenio_id_de(nome) == esperado


# ── Ponta a ponta no motor ─────────────────────────────────────────────────────
def test_guia_completa_unimed_nao_dispara_planilha():
    gate, falhas = _rodar()
    assert not {f for f in falhas if f.startswith("REGRA_")}
    assert gate == "GO_COM_RESSALVAS"           # só OP_RELATORIO_001, como antes


def test_particular_completa_continua_go():
    assert _rodar(convenio="PARTICULAR") == ("GO", set())


def test_hapvida_sem_numero_da_guia_bloqueia():
    gate, falhas = _rodar(convenio="HAPVIDA")
    assert gate == "NO_GO" and "REGRA_0005" in falhas


def test_hapvida_com_numero_da_guia_passa():
    gate, falhas = _rodar(convenio="HAPVIDA", numero_guia_operadora="HAP-000123")
    assert "REGRA_0005" not in falhas and gate != "NO_GO"


def test_sulamerica_sem_conservador_bloqueia_pela_regra_0004():
    gate, falhas = _rodar(convenio="SULAMERICA", tto_conservador="")
    assert gate == "NO_GO" and "REGRA_0004" in falhas


def test_opme_uma_marca_bloqueia_e_tres_marcas_nao():
    gate1, f1 = _rodar(**_opme("Marca A"))
    assert gate1 == "NO_GO" and "REGRA_0001" in f1
    gate3, f3 = _rodar(**_opme("Marca A", "Marca B", "Marca C"))
    assert "REGRA_0001" not in f3


def test_indicacao_curta_gera_alerta_0002():
    gate, falhas = _rodar(indicacao_clinica="Hérnia L4-L5. Cirurgia.")
    assert "REGRA_0002" in falhas and gate != "NO_GO"


def test_bradesco_gera_alerta_0010():
    gate, falhas = _rodar(convenio="BRADESCO SAUDE")
    assert "REGRA_0010" in falhas and gate == "GO_COM_RESSALVAS"


def test_regra_de_outra_operadora_nao_vaza():
    _, falhas = _rodar(convenio="UNIMED", tto_conservador="Sim — 2 semanas")
    assert "REGRA_0004" not in falhas and "REGRA_0005" not in falhas and "REGRA_0010" not in falhas
