"""
NEUROAUTH — Testes de Integração da Camada Web
Versão: 2.0.0

Cobre:
  T01 — POST /motor/test com payload completo → GO
  T02 — POST /motor/test sem proc_master_row → SYS001 NO_GO
  T03 — POST /motor/test com COD_TUSS divergente → NO_GO RGL010
  T04 — POST /motor/test sem raw_case → 400
  T05 — POST /decision/run/<id> caso GO real
  T06 — POST /decision/run/<id> caso PENDENCIA (CID ausente)
  T07 — POST /decision/run/<id> episodio inexistente → 404
  T08 — POST /decision/run/<id> sem proc_master_row → SYS001 persistido
  T09 — GET /health → engine_version presente
  T10 — Persistência: run gravado com campos obrigatórios
  T11 — SYS001 persistido como NO_GO (não silenciado)
  T12 — Campos protegidos não aparecem em autopreenchimentos
"""
import sys
import json

sys.path.insert(0, "/home/claude/neuroauth_motor")

from app import create_app
from repositories.decision_repository import get_decision_run, _EPISODIOS

app = create_app()
client = app.test_client()

PASSED = []
FAILED = []

def check(label: str, condition: bool, detail: str = ""):
    if condition:
        PASSED.append(label)
        print(f"  ✓  {label}")
    else:
        FAILED.append(label)
        msg = f"  ✗  {label}"
        if detail: msg += f" — {detail}"
        print(msg)

def post(url, body):
    return client.post(url, json=body, content_type="application/json")

def get(url):
    return client.get(url)

# Dado mestre válido para testes isolados
MASTER = {
    "descricao": "Artrodese cervical anterior",
    "especialidade": "Neurocirurgia",
    "porte": "7C",
    "porte_anestesico": "5",
    "via_acesso": "anterior",
    "codigo_cbhpm": "3.07.15.39-3",
    "regras_json": {
        "cod_tuss_esperado": "40808505",
        "multinivel": True,
        "min_niveis": 1,
        "max_niveis": 3,
        "lateralidade_obrigatoria": False,
        "opme_obrigatoria": True,
        "opme_materiais_permitidos": ["CAGE_PEEK_01"],
        "opme_quantidade_por_niveis": False,
        "carater_obrigatorio": True,
        "aceita_urgencia": True,
        "cids_preferenciais": ["M50.1"],
        "cids_incompativeis": [],
    }
}

RAW_GO = {
    "episodio_id":  "EP_TEST_GO",
    "usuario_id":   "USR_001",
    "profile_id":   "PROF_ACDF_01",
    "codigo_tuss":  "40808505",
    "cid_principal":"M50.1",
    "convenio_id":  "UNIMED_CARIRI",
    "carater":      "eletivo",
    "niveis":       "2",
    "opme_json":    {"materiais": [{"codigo": "CAGE_PEEK_01", "qtd": 1}]},
}

print("\n── /motor/test (rota isolada) ─────────────────────────────────────────")

# T01 — GO completo
print("\nT01  POST /motor/test — caso GO")
r = post("/motor/test", {"raw_case": RAW_GO, "proc_master_row": MASTER, "session_user_id": "USR_001"})
d = r.get_json()
check("T01a  HTTP 200",                  r.status_code == 200)
check("T01b  decision_status = GO",      d.get("decision_status") == "GO")
check("T01c  can_send = True",           d.get("can_send") is True)
check("T01d  engine_version presente",   "engine_version" in d)

# T02 — sem proc_master_row → SYS001
print("\nT02  POST /motor/test — sem proc_master_row")
r = post("/motor/test", {"raw_case": RAW_GO})
d = r.get_json()
check("T02a  HTTP 200",                  r.status_code == 200)
check("T02b  decision_status = NO_GO",   d.get("decision_status") == "NO_GO")
check("T02c  bloqueio SYS001",           any(b["codigo"] == "SYS001" for b in d.get("bloqueios", [])))
check("T02d  can_send = False",          d.get("can_send") is False)

# T03 — COD_TUSS errado → NO_GO RGL010
print("\nT03  POST /motor/test — COD_TUSS diverge do mestre")
raw_tuss_errado = {**RAW_GO, "codigo_tuss": "99999999"}
r = post("/motor/test", {"raw_case": raw_tuss_errado, "proc_master_row": MASTER})
d = r.get_json()
check("T03a  decision_status = NO_GO",   d.get("decision_status") == "NO_GO")
check("T03b  bloqueio RGL010",           any(b["codigo"] == "RGL010" for b in d.get("bloqueios", [])))

# T04 — sem raw_case → 400
print("\nT04  POST /motor/test — sem raw_case")
r = post("/motor/test", {"proc_master_row": MASTER})
check("T04a  HTTP 400",                  r.status_code == 400)
check("T04b  mensagem de erro presente", "erro" in (r.get_json() or {}))

print("\n── /decision/run/<id> (rota real) ─────────────────────────────────────")

# T05 — episódio GO real (EP_2024_001 tem todos os campos)
print("\nT05  POST /decision/run/EP_2024_001 — caso GO")
r = post("/decision/run/EP_2024_001", {})
d = r.get_json()
check("T05a  HTTP 200",                  r.status_code == 200)
check("T05b  decision_status = GO",      d.get("decision_status") == "GO")
check("T05c  can_send = True",           d.get("can_send") is True)
check("T05d  _run_id presente",          "_run_id" in d)

# T06 — EP_2024_002 sem CID → PENDENCIA
print("\nT06  POST /decision/run/EP_2024_002 — CID ausente")
r = post("/decision/run/EP_2024_002", {})
d = r.get_json()
check("T06a  HTTP 200",                  r.status_code == 200)
check("T06b  PENDENCIA_OBRIGATORIA",     d.get("decision_status") == "PENDENCIA_OBRIGATORIA")
check("T06c  can_send = False",          d.get("can_send") is False)
check("T06d  pendencia RGL004",          any(p["codigo"] == "RGL004" for p in d.get("pendencias", [])))

# T07 — episódio inexistente → 404
print("\nT07  POST /decision/run/EP_INEXISTENTE — 404")
r = post("/decision/run/EP_INEXISTENTE", {})
check("T07a  HTTP 404",                  r.status_code == 404)

# T08 — profile_id não existe no mestre → SYS001 persistido
print("\nT08  POST /decision/run — proc_master_row ausente → SYS001 persistido")
_EPISODIOS["EP_SYS001_TEST"] = {
    "episodio_id":  "EP_SYS001_TEST",
    "profile_id":   "PROF_INEXISTENTE",
    "convenio_id":  "UNIMED_CARIRI",
    "usuario_id":   "USR_001",
    "cod_tuss":     "40808505",
    "decision_status": None,
}
r = post("/decision/run/EP_SYS001_TEST", {})
d = r.get_json()
check("T08a  HTTP 200",                  r.status_code == 200)
check("T08b  decision_status = NO_GO",   d.get("decision_status") == "NO_GO")
check("T08c  bloqueio SYS001",           any(b["codigo"] == "SYS001" for b in d.get("bloqueios", [])))
# verificar que foi persistido
ep_after = _EPISODIOS.get("EP_SYS001_TEST", {})
check("T08d  episodio atualizado com NO_GO", ep_after.get("decision_status") == "NO_GO")

print("\n── /health ────────────────────────────────────────────────────────────")

# T09 — health check
print("\nT09  GET /health")
r = get("/health")
d = r.get_json()
check("T09a  HTTP 200",                  r.status_code == 200)
check("T09b  status = ok",               d.get("status") == "ok")
check("T09c  engine_version presente",   "engine_version" in d)

print("\n── Persistência ───────────────────────────────────────────────────────")

# T10 — run gravado com campos obrigatórios
print("\nT10  Run persistido com campos obrigatórios")
run_id = _EPISODIOS.get("EP_2024_001", {}).get("decision_run_id")
run    = get_decision_run(run_id) if run_id else None
check("T10a  run encontrado pelo run_id",          run is not None, f"run_id={run_id}")
if run:
    for campo in ("decision_run_id","episodio_id","decision_status",
                  "confidence_global","bloqueios_json","pendencias_json",
                  "alertas_json","campos_inferidos_json","engine_version","created_at"):
        check(f"T10b  campo '{campo}' presente", campo in run)

# T11 — SYS001 persistido como NO_GO, não silenciado
print("\nT11  SYS001 persistido como NO_GO")
ep_sys = _EPISODIOS.get("EP_SYS001_TEST", {})
check("T11a  decision_status = NO_GO",   ep_sys.get("decision_status") == "NO_GO")
run_id_sys = ep_sys.get("decision_run_id")
run_sys    = get_decision_run(run_id_sys) if run_id_sys else None
check("T11b  run gravado mesmo para SYS001", run_sys is not None)
if run_sys:
    bloq = run_sys.get("bloqueios_json", [])
    check("T11c  bloqueio SYS001 no run",    any(b.get("codigo") == "SYS001" for b in bloq))

# T12 — campos protegidos não em autopreenchimentos
print("\nT12  Campos protegidos nao aparecem em autopreenchimentos")
r = post("/motor/test", {"raw_case": RAW_GO, "proc_master_row": MASTER})
d = r.get_json()
auto_campos = [a["campo"] for a in d.get("autopreenchimentos", [])]
for cp in ["COD_TUSS", "CID_PRINCIPAL", "OPME_JSON", "PROFILE_ID", "CONVENIO_ID", "CARATER", "LATERALIDADE"]:
    check(f"T12  {cp} nao em autopreenchimentos", cp not in auto_campos)

# =============================================================================
total = len(PASSED) + len(FAILED)
print(f"\n{'='*70}")
print(f"  Resultado: {len(PASSED)}/{total} testes passaram")
if FAILED:
    print(f"\n  Falharam ({len(FAILED)}):")
    for f in FAILED:
        print(f"    ✗ {f}")
print(f"{'='*70}\n")

sys.exit(0 if not FAILED else 1)
