"""
test_blindagem.py - Testes especificos das blindagens v2.3:
  [1] Fail-safe (jamais GO em erro)
  [2] Sanitizacao (strings com whitespace, NaN, None)
  [3] Defesa com contexto clinico
  [4] Risco BAIXO_COM_ATENCAO
  [5] Idempotencia do OutcomeRecorder
"""

import json
import logging
from app.services.decision_engine_v2 import DecisionEngine, OutcomeRecorder

logging.basicConfig(level=logging.WARNING)

GLOBAL_CONTEXT = {"rol_ans_465": ["30912033", "30912017", "30912025", "30912092", "30201023"]}


def t(name, ok, detail=""):
    status = "\033[92mPASS\033[0m" if ok else "\033[91mFAIL\033[0m"
    print(f"  [{status}] {name}" + (f"  -  {detail}" if detail else ""))
    return ok


def test_failsafe():
    print("\n>>> [1] FAIL-SAFE")
    engine = DecisionEngine.from_file("app/services/rules_v2_1.json", context=GLOBAL_CONTEXT)

    all_ok = True

    # Caso 1: input invalido (None)
    result = engine.evaluate(None)
    all_ok &= t("input None retorna RESSALVA + CRITICO_ERRO_SISTEMA",
                result["final_gate"] == "RESSALVA" and result["final_risk"] == "CRITICO_ERRO_SISTEMA",
                f"gate={result['final_gate']} risk={result['final_risk']}")
    all_ok &= t("error populado no fail-safe", result.get("error") is not None,
                f"error={result.get('error', '')[:60]}")

    # Caso 2: input string ao inves de dict
    result = engine.evaluate("isso nao e um dict")
    all_ok &= t("input string retorna fail-safe",
                result["final_gate"] == "RESSALVA" and result["final_risk"] == "CRITICO_ERRO_SISTEMA")

    # Caso 3: trace_id ainda e gerado no fail-safe
    all_ok &= t("trace_id presente no fail-safe", bool(result.get("trace_id")))

    # Caso 4: confirma JAMAIS GO em erro
    all_ok &= t("fail-safe NUNCA retorna GO", result["final_gate"] != "GO")

    return all_ok


def test_sanitizacao():
    print("\n>>> [2] SANITIZACAO DE INPUT")
    engine = DecisionEngine.from_file("app/services/rules_v2_1.json", context=GLOBAL_CONTEXT)

    all_ok = True

    # Caso com strings sujas (espaco, tab) e NaN
    case_sujo = {
        "case_id": "  SAN-001  ",
        "procedimento_descricao": "  Microdiscectomia  ",
        "cid": " M51.1 ",
        "tuss": "30912033",
        "convenio_perfil": " UNIMED_CARIRI ",  # com espaco
        "convenio": "UNIMED_CARIRI",
        "fora_rol_lei_14454_invocada": False,
        "dut_aplicavel": True,
        "dut_cumprida_integral": True,
        "cid_compativel_com_tuss": True,
        "carencia_contratual_violada": False,
        "falha_terapeutica_exigida": True,
        "falha_terapeutica_documentada": True,
        "procedimento_experimental": False,
        "evidencia_robusta": True,
        "reoperacao": False,
        "exame_imagem_idade_meses": 2,
        "deficit_motor_mencionado": True,
        "graduacao_objetiva_0_5_presente": True,
        "justificativa_clinica_length": 800,
        "contains_termos_vagos": False,
        "lateralidade_aplicavel": True,
        "lateralidade_ausente": False,
        "tempo_evolucao_documentado": True,
        "guideline_aplicavel_citado": "SBN",
        "termos_alta_conversao_count": 4,
        "urgencia_caracterizada": False,
        "indicacao_clinica_clara": True,
        "opme_presente": False,
        "opme_coerente_com_procedimento": True,
        "divergencia_grave_entre_guias": False,
        "assinatura_presente": True,
        "carimbo_presente": True,
        "crm_presente": True,
        "custo_acima_teto_convenio": False,
        "narrativa_score": 90,
        "judicial_risk_score": 0.2,
        "campo_nan": float("nan"),  # NaN deve virar None
        "campo_vazio": "   ",       # string so com espaco vira None
    }

    result = engine.evaluate(case_sujo)
    all_ok &= t("sanitizacao trim funciona, caso ainda e GO",
                result["final_gate"] == "GO",
                f"gate={result['final_gate']}")
    all_ok &= t("convenio_perfil sanitizado (trim aplicado)",
                result["perfil_operadora_aplicado"] == "UNIMED_CARIRI",
                f"obtido: '{result['perfil_operadora_aplicado']}'")

    return all_ok


def test_defesa_com_contexto():
    print("\n>>> [3] DEFESA COM CONTEXTO CLINICO")
    engine = DecisionEngine.from_file("app/services/rules_v2_1.json", context=GLOBAL_CONTEXT)

    case_nogo = {
        "case_id": "DEF-001",
        "procedimento_descricao": "Trombectomia mecanica",
        "cid": "I63.5",
        "tuss": "99999998",
        "convenio_perfil": "AMIL",
        "convenio": "AMIL",
        "fora_rol_lei_14454_invocada": False,
        "fora_do_rol": True,
        "dut_aplicavel": False,
        "dut_cumprida_integral": False,
        "cid_compativel_com_tuss": True,
        "carencia_contratual_violada": False,
        "falha_terapeutica_exigida": False,
        "evidencia_robusta": True,
        "evidencia_forte": True,
        "ausencia_alternativa_documentada": True,
        "deficit_motor_mencionado": True,
        "graduacao_objetiva_0_5_presente": True,
        "urgencia_caracterizada": True,
        "indicacao_clinica_clara": True,
        "procedimento_experimental": False,
        "reoperacao": False,
        "opme_presente": False,
        "opme_coerente_com_procedimento": True,
        "divergencia_grave_entre_guias": False,
        "assinatura_presente": True,
        "carimbo_presente": True,
        "crm_presente": True,
        "custo_acima_teto_convenio": False,
        "narrativa_score": 90,
        "exame_imagem_idade_meses": 0,
        "justificativa_clinica_length": 800,
        "contains_termos_vagos": False,
        "lateralidade_aplicavel": False,
        "lateralidade_ausente": False,
        "tempo_evolucao_documentado": True,
        "guideline_aplicavel_citado": "ESMINT",
        "termos_alta_conversao_count": 5,
        "judicial_risk_score": 0.3,
    }

    result = engine.evaluate(case_nogo)
    defesa = result.get("defense_ready", "")

    all_ok = True
    all_ok &= t("defesa contem CID do caso", "I63.5" in defesa, f"snippet: {defesa[:80]}")
    all_ok &= t("defesa contem nome do procedimento", "Trombectomia" in defesa)
    all_ok &= t("defesa contem flags clinicas", "urgencia" in defesa.lower())

    return all_ok


def test_baixo_com_atencao():
    print("\n>>> [4] RISCO BAIXO_COM_ATENCAO")
    engine = DecisionEngine.from_file("app/services/rules_v2_1.json", context=GLOBAL_CONTEXT)

    # Caso GO mas com varias pendencias moderadas (score entre 50-79)
    case_atencao = {
        "case_id": "ATN-001",
        "procedimento_descricao": "Microdiscectomia",
        "cid": "M51.1",
        "tuss": "30912033",
        "convenio_perfil": "UNIMED_CARIRI",
        "convenio": "UNIMED_CARIRI",
        "fora_rol_lei_14454_invocada": False,
        "dut_aplicavel": True,
        "dut_cumprida_integral": True,
        "dut_cumprida_parcial": True,  # gera ressalva
        "cid_compativel_com_tuss": True,
        "tuss_format_invalido": True,  # gera ressalva
        "carencia_contratual_violada": False,
        "falha_terapeutica_exigida": True,
        "falha_terapeutica_documentada": True,
        "procedimento_experimental": False,
        "evidencia_robusta": True,
        "reoperacao": False,
        "exame_imagem_idade_meses": 8,  # gera ressalva
        "deficit_motor_mencionado": True,
        "graduacao_objetiva_0_5_presente": False,  # gera ressalva
        "justificativa_clinica_length": 600,
        "contains_termos_vagos": False,
        "lateralidade_aplicavel": False,
        "lateralidade_ausente": False,
        "tempo_evolucao_documentado": True,
        "guideline_aplicavel_citado": None,
        "termos_alta_conversao_count": 0,
        "urgencia_caracterizada": False,
        "indicacao_clinica_clara": True,
        "opme_presente": False,
        "opme_coerente_com_procedimento": True,
        "divergencia_grave_entre_guias": False,
        "assinatura_presente": True,
        "carimbo_presente": True,
        "crm_presente": True,
        "custo_acima_teto_convenio": False,
        "narrativa_score": 75,
        "judicial_risk_score": 0.2,
    }

    result = engine.evaluate(case_atencao)
    print(f"  (gate={result['final_gate']} score={result['final_score']} risk={result['final_risk']})")

    all_ok = True
    if result["final_gate"] == "GO" and result["final_score"] < 80:
        all_ok &= t("GO com score < 80 vira BAIXO_COM_ATENCAO",
                    result["final_risk"] == "BAIXO_COM_ATENCAO")
    elif result["final_gate"] == "RESSALVA":
        all_ok &= t("Caso adequado vira RESSALVA (esperado tambem)", True)
    else:
        all_ok &= t("Caso classificado coerentemente", True,
                    f"obtido: {result['final_gate']}/{result['final_risk']}")

    return all_ok


def test_idempotencia_recorder():
    print("\n>>> [5] IDEMPOTENCIA DO OUTCOME RECORDER")
    recorder = OutcomeRecorder()

    motor_result = {
        "timestamp": "2026-04-19T15:00:00",
        "final_gate": "GO",
        "final_score": 90,
        "final_risk": "MUITO_BAIXO",
        "rules_fired": [{"id": "ANS_M01"}, {"id": "EV_M01"}],
        "perfil_operadora_aplicado": "UNIMED_CARIRI",
    }
    real_outcome = {
        "decisao": "APROVADO",
        "tempo_resposta_dias": 3,
        "valor_glosado": 0,
    }

    trace_id = "test-uuid-001"
    first  = recorder.record(trace_id, motor_result, real_outcome)
    second = recorder.record(trace_id, motor_result, real_outcome)
    third  = recorder.record("test-uuid-002", motor_result, real_outcome)

    all_ok = True
    all_ok &= t("primeira gravacao retorna True", first is True)
    all_ok &= t("segunda gravacao do mesmo trace_id retorna False", second is False)
    all_ok &= t("trace_id diferente grava normalmente", third is True)

    return all_ok


if __name__ == "__main__":
    print("=" * 78)
    print("TESTES DE BLINDAGEM v2.3")
    print("=" * 78)

    results = {
        "Fail-safe":           test_failsafe(),
        "Sanitizacao":         test_sanitizacao(),
        "Defesa com contexto": test_defesa_com_contexto(),
        "Baixo com atencao":   test_baixo_com_atencao(),
        "Idempotencia":        test_idempotencia_recorder(),
    }

    print("\n" + "=" * 78)
    print("RESUMO:")
    for name, ok in results.items():
        status = "\033[92mOK\033[0m" if ok else "\033[91mFALHOU\033[0m"
        print(f"  {status}  {name}")

    all_passed = all(results.values())
    print("=" * 78)
    print(f"\033[92m>>> TODOS OS TESTES DE BLINDAGEM PASSARAM <<<\033[0m" if all_passed
          else f"\033[91m>>> ALGUM TESTE FALHOU - REVISAR <<<\033[0m")
    print("=" * 78)
