"""
tests/test_governanca_producao.py
5 cenários críticos de governança financeira — rode com:
    python tests/test_governanca_producao.py
ou:
    python -m pytest tests/test_governanca_producao.py -v
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import MagicMock, patch


# ──────────────────────────────────────────────────────────────
# Helper: mock do sheet_client (interface _SheetsAdapter)
# ──────────────────────────────────────────────────────────────

def make_client(prod=None, audit=None, periodos=None):
    c = MagicMock()
    store = {
        "PRODUCAO":             list(prod or []),
        "PRODUCAO_AUDIT":       list(audit or []),
        "PERIODOS_COMPETENCIA": list(periodos or []),
    }
    c.get_all_records.side_effect = lambda worksheet=None: store.get(worksheet, [])
    c.append_row_by_header.return_value = None
    return c


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

LINHA_BASE = {
    "caso_id":             "EP_2026_001",
    "cirurgiao_id":        "CIR_001",
    "papel":               "PRINCIPAL",
    "versao":              "v1",
    "status_producao":     "ABERTO",
    "periodo_competencia": "2026-04",
    "valor_calculado":     "2000.00",
    "valor_base_usado":    "2000.00",
    "percentual_aplicado": "1.00",
    "operadora":           "UNIMED",
    "timestamp_calculo":   "2026-04-01T10:00:00Z",
    "substituida_em":      "",
    "substituida_por":     "",
}

PERIODO_FECHADO = [{"periodo": "2026-04", "status": "FECHADO"}]


# ──────────────────────────────────────────────────────────────
# TESTE 1 — ABERTO → FECHADO
# ──────────────────────────────────────────────────────────────

def test_1_fechar_periodo():
    from app.services.surgeon_producao import fechar_periodo

    prod = [LINHA_BASE]
    c = make_client(prod=prod)

    r = fechar_periodo(c, "2026-04", "admin")

    assert r["status"]      == "FECHADO",  f"status esperado FECHADO, recebeu {r['status']}"
    assert r["total_casos"] == 1,          f"total_casos esperado 1, recebeu {r['total_casos']}"
    assert r["valor_total"] == 2000.0,     f"valor_total esperado 2000.0, recebeu {r['valor_total']}"

    c.append_row_by_header.assert_called_once()
    call_kw = c.append_row_by_header.call_args[1]
    assert call_kw["worksheet"] == "PERIODOS_COMPETENCIA"

    print("✅ test_1_fechar_periodo PASS")


# ──────────────────────────────────────────────────────────────
# TESTE 2 — Tentativa de gravação em período fechado → PermissionError
# gravar_producao usa API interna → mockamos _get_status_periodo_internal
# ──────────────────────────────────────────────────────────────

def test_2_gravacao_periodo_fechado():
    from app.services.surgeon_producao import gravar_producao

    linhas_novas = [{**LINHA_BASE, "caso_id": "EP_2026_002"}]

    raised = False
    with patch("app.services.surgeon_producao._get_status_periodo_internal", return_value="FECHADO"):
        try:
            gravar_producao(linhas_novas)
        except PermissionError as e:
            raised = True
            assert "FECHADO" in str(e), f"Mensagem inesperada: {e}"

    assert raised, "PermissionError não foi levantado para período FECHADO"
    print("✅ test_2_gravacao_periodo_fechado PASS")


# ──────────────────────────────────────────────────────────────
# TESTE 3 — Retificação normal (período ABERTO)
# ──────────────────────────────────────────────────────────────

def test_3_retificacao_normal():
    from app.services.surgeon_producao import retificar_producao

    c = make_client(prod=[LINHA_BASE])

    r = retificar_producao(
        caso_id="EP_2026_001",
        sheet_client=c,
        motivo="Correção de honorário base",
        usuario="jose.jr",
        novos_dados={"valor_calculado": "2200.00", "valor_base_usado": "2200.00"},
    )

    assert r["versao_nova"] == "v2", f"versao_nova esperada v2, recebeu {r['versao_nova']}"
    assert "audit_id" in r,          "audit_id ausente no retorno"
    assert r["ok"] is True

    # 3 chamadas: SUBSTITUIDA + nova versão + audit log
    assert c.append_row_by_header.call_count == 3, (
        f"Esperado 3 chamadas, recebeu {c.append_row_by_header.call_count}"
    )

    calls = c.append_row_by_header.call_args_list
    sub_call   = calls[0][1]["row_data"]
    nova_call  = calls[1][1]["row_data"]
    audit_call = calls[2][1]["row_data"]

    assert sub_call["status_producao"]  == "SUBSTITUIDA"
    assert sub_call["substituida_por"]  == "v2"
    assert nova_call["versao"]           == "v2"
    assert nova_call["status_producao"]  == "ABERTO"
    assert nova_call["valor_calculado"]  == "2200.00"
    assert audit_call["versao_anterior"] == "v1"
    assert audit_call["versao_nova"]     == "v2"
    assert audit_call["usuario"]         == "jose.jr"

    print("✅ test_3_retificacao_normal PASS")


# ──────────────────────────────────────────────────────────────
# TESTE 4 — Retificação forçada em período FECHADO
# ──────────────────────────────────────────────────────────────

def test_4_retificacao_forcada_periodo_fechado():
    from app.services.surgeon_producao import retificar_producao

    c = make_client(prod=[LINHA_BASE], periodos=PERIODO_FECHADO)

    # Sem forcar=True deve lançar PermissionError
    raised = False
    try:
        retificar_producao("EP_2026_001", c, "Ajuste", "jose.jr", {"valor_calculado": "1800.00"}, forcar=False)
    except PermissionError:
        raised = True
    assert raised, "PermissionError não foi levantado sem forcar=True em período FECHADO"

    # Com forcar=True deve funcionar
    c2 = make_client(prod=[LINHA_BASE], periodos=PERIODO_FECHADO)
    r = retificar_producao(
        "EP_2026_001", c2,
        motivo="Correção emergencial autorizada",
        usuario="jose.jr",
        novos_dados={"valor_calculado": "1800.00"},
        forcar=True,
    )

    assert r["versao_nova"] == "v2"

    audit_call = c2.append_row_by_header.call_args_list[-1][1]["row_data"]
    assert "OVERRIDE_PERIODO_FECHADO" in audit_call["motivo"], (
        f"Flag OVERRIDE_PERIODO_FECHADO ausente: {audit_call['motivo']}"
    )
    assert audit_call["action"] == "RETIFICACAO_FORCADA"

    print("✅ test_4_retificacao_forcada_periodo_fechado PASS")


# ──────────────────────────────────────────────────────────────
# TESTE 5 — Audit log completo em PRODUCAO_AUDIT
# ──────────────────────────────────────────────────────────────

def test_5_audit_log_completo():
    from app.services.surgeon_producao import retificar_producao

    c = make_client(prod=[LINHA_BASE])
    retificar_producao(
        "EP_2026_001", c,
        motivo="Ajuste de percentual acordado com convênio",
        usuario="jose.jr",
        novos_dados={"percentual_aplicado": "0.25", "valor_calculado": "500.00"},
    )

    calls     = c.append_row_by_header.call_args_list
    audit_row = calls[-1][1]["row_data"]

    assert audit_row.get("caso_id")          == "EP_2026_001"
    assert audit_row.get("usuario")          == "jose.jr"
    assert audit_row.get("versao_anterior")  == "v1"
    assert audit_row.get("versao_nova")      == "v2"
    assert "audit_id" in audit_row and audit_row["audit_id"]
    assert "timestamp_audit" in audit_row and audit_row["timestamp_audit"]
    assert "campos_alterados" in audit_row
    assert "percentual_aplicado" in audit_row["campos_alterados"]
    assert audit_row.get("action") == "RETIFICACAO"

    print("✅ test_5_audit_log_completo PASS")


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n=== GOVERNANÇA FINANCEIRA — TESTES DE VALIDAÇÃO ===\n")
    try:
        test_1_fechar_periodo()
        test_2_gravacao_periodo_fechado()
        test_3_retificacao_normal()
        test_4_retificacao_forcada_periodo_fechado()
        test_5_audit_log_completo()
        print("\n✅ TODOS OS 5 TESTES PASSARAM — STATUS: GO\n")
    except AssertionError as e:
        print(f"\n❌ FALHA: {e}\n")
        sys.exit(1)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n❌ ERRO INESPERADO: {e}\n")
        sys.exit(1)
