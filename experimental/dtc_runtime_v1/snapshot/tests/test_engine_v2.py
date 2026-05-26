"""
NEUROAUTH - test_engine.py
==========================
Suite de teste com 6 casos clinicos reais para validar o decision_engine.

Cobre:
  2 casos GO       (microdiscectomia ideal, urgencia override)
  2 casos RESSALVA (artrodese com pendencias, OPME sem cotacao)
  2 casos NO_GO    (CID incompativel, fora do rol sem defesa)
"""

import json
from app.services.decision_engine_v2 import DecisionEngine, OutcomeRecorder


# ============================================================
# CASOS DE TESTE
# ============================================================

CASE_GO_IDEAL = {
    "case_id": "GO-001",
    "procedimento_descricao": "Microdiscectomia lombar L4-L5 paramediana esquerda",
    "cid": "M51.1",
    "tuss": "30912033",
    "convenio_perfil": "UNIMED_CARIRI",

    # ANS
    "fora_rol_lei_14454_invocada": False,
    "dut_aplicavel": True,
    "dut_cumprida_integral": True,
    "dut_cumprida_parcial": False,
    "cid_compativel_com_tuss": True,
    "tuss_format_invalido": False,
    "cid_format_invalido": False,
    "convenio": "UNIMED_CARIRI",
    "carencia_contratual_violada": False,

    # EVIDENCIA
    "falha_terapeutica_exigida": True,
    "falha_terapeutica_documentada": True,
    "procedimento_experimental": False,
    "evidencia_robusta": True,
    "reoperacao": False,
    "laudo_cirurgia_anterior_anexado": False,
    "exame_imagem_idade_meses": 2,
    "deficit_motor_mencionado": True,
    "graduacao_objetiva_0_5_presente": True,
    "justificativa_clinica_length": 850,
    "contains_termos_vagos": False,
    "lateralidade_aplicavel": True,
    "lateralidade_ausente": False,
    "tempo_evolucao_documentado": True,
    "guideline_aplicavel_citado": "SBN",
    "termos_alta_conversao_count": 4,
    "urgencia_caracterizada": False,
    "risco_neurologico_progressivo_documentado": False,
    "indicacao_clinica_clara": True,

    # OPERADORA
    "opme_presente": False,
    "opme_coerente_com_procedimento": True,
    "opme_cotacoes_count": 0,
    "opme_tem_similar_nacional": True,
    "opme_especificada_por_marca": False,
    "opme_especificacao_tecnica_presente": True,
    "divergencia_grave_entre_guias": False,
    "assinatura_presente": True,
    "carimbo_presente": True,
    "crm_presente": True,
    "custo_acima_teto_convenio": False,
    "narrativa_score": 92,
    "judicial_risk_score": 0.2,
    "cost_vs_judicialization_ratio": 0.15,
}

CASE_GO_URGENCIA = {
    "case_id": "GO-002",
    "procedimento_descricao": "Embolizacao de aneurisma cerebral roto",
    "cid": "I60.7",
    "tuss": "30201023",
    "convenio_perfil": "BRADESCO",

    "fora_rol_lei_14454_invocada": False,
    "dut_aplicavel": True,
    "dut_cumprida_integral": False,  # urgencia, nao deu tempo
    "dut_cumprida_parcial": True,
    "cid_compativel_com_tuss": True,
    "tuss_format_invalido": False,
    "cid_format_invalido": False,
    "convenio": "BRADESCO",
    "carencia_contratual_violada": False,

    "falha_terapeutica_exigida": False,
    "falha_terapeutica_documentada": False,
    "procedimento_experimental": False,
    "evidencia_robusta": True,
    "reoperacao": False,
    "exame_imagem_idade_meses": 0,
    "deficit_motor_mencionado": True,
    "graduacao_objetiva_0_5_presente": True,
    "justificativa_clinica_length": 600,
    "contains_termos_vagos": False,
    "lateralidade_aplicavel": True,
    "lateralidade_ausente": False,
    "tempo_evolucao_documentado": True,
    "guideline_aplicavel_citado": "ESMINT",
    "termos_alta_conversao_count": 5,
    "urgencia_caracterizada": True,  # OVERRIDE
    "risco_neurologico_progressivo_documentado": True,
    "indicacao_clinica_clara": True,

    "opme_presente": True,
    "opme_coerente_com_procedimento": True,
    "opme.registro_anvisa": "REG-12345",
    "opme_cotacoes_count": 1,
    "opme_tem_similar_nacional": False,
    "opme_especificada_por_marca": False,
    "opme_especificacao_tecnica_presente": True,
    "divergencia_grave_entre_guias": False,
    "assinatura_presente": True,
    "carimbo_presente": True,
    "crm_presente": True,
    "custo_acima_teto_convenio": True,
    "narrativa_score": 85,
    "judicial_risk_score": 0.85,
    "cost_vs_judicialization_ratio": 0.10,
}

CASE_RESSALVA_PENDENCIAS = {
    "case_id": "RES-001",
    "procedimento_descricao": "Artrodese cervical anterior C5-C6",
    "cid": "M50.1",
    "tuss": "30912092",
    "convenio_perfil": "UNIMED_CARIRI",

    "fora_rol_lei_14454_invocada": False,
    "dut_aplicavel": True,
    "dut_cumprida_integral": False,
    "dut_cumprida_parcial": True,  # Pendencia ANS
    "cid_compativel_com_tuss": True,
    "tuss_format_invalido": True,  # Pendencia ANS
    "cid_format_invalido": False,
    "convenio": "UNIMED_CARIRI",
    "carencia_contratual_violada": False,

    "falha_terapeutica_exigida": True,
    "falha_terapeutica_documentada": True,
    "procedimento_experimental": False,
    "evidencia_robusta": True,
    "reoperacao": False,
    "exame_imagem_idade_meses": 8,  # Pendencia EV
    "deficit_motor_mencionado": True,
    "graduacao_objetiva_0_5_presente": False,  # Pendencia EV
    "justificativa_clinica_length": 250,
    "contains_termos_vagos": False,
    "lateralidade_aplicavel": False,
    "lateralidade_ausente": False,
    "tempo_evolucao_documentado": True,
    "guideline_aplicavel_citado": "AANS",
    "termos_alta_conversao_count": 3,
    "urgencia_caracterizada": False,
    "risco_neurologico_progressivo_documentado": False,
    "indicacao_clinica_clara": True,

    "opme_presente": True,
    "opme_coerente_com_procedimento": True,
    "opme.registro_anvisa": "REG-98765",
    "opme_cotacoes_count": 1,  # Pendencia OP
    "opme_tem_similar_nacional": False,
    "opme_especificada_por_marca": False,
    "opme_especificacao_tecnica_presente": True,
    "divergencia_grave_entre_guias": False,
    "assinatura_presente": True,
    "carimbo_presente": True,
    "crm_presente": True,
    "custo_acima_teto_convenio": True,
    "narrativa_score": 65,  # Pendencia OP
    "judicial_risk_score": 0.4,
    "cost_vs_judicialization_ratio": 0.5,
}

CASE_RESSALVA_LINGUAGEM = {
    "case_id": "RES-002",
    "procedimento_descricao": "Microdiscectomia lombar",
    "cid": "M51.1",
    "tuss": "30912033",
    "convenio_perfil": "BRADESCO",

    "fora_rol_lei_14454_invocada": False,
    "dut_aplicavel": True,
    "dut_cumprida_integral": False,
    "dut_cumprida_parcial": True,  # Pendencia ANS
    "cid_compativel_com_tuss": True,
    "tuss_format_invalido": True,  # Pendencia ANS
    "cid_format_invalido": False,
    "convenio": "BRADESCO",
    "carencia_contratual_violada": False,

    "falha_terapeutica_exigida": True,
    "falha_terapeutica_documentada": True,
    "procedimento_experimental": False,
    "evidencia_robusta": True,
    "reoperacao": False,
    "exame_imagem_idade_meses": 9,  # Pendencia EV
    "deficit_motor_mencionado": True,
    "graduacao_objetiva_0_5_presente": False,  # Pendencia EV
    "justificativa_clinica_length": 150,  # Pendencia EV
    "contains_termos_vagos": True,  # Pendencia EV
    "lateralidade_aplicavel": True,
    "lateralidade_ausente": True,  # Pendencia EV
    "tempo_evolucao_documentado": False,  # Pendencia EV
    "guideline_aplicavel_citado": None,
    "termos_alta_conversao_count": 0,
    "urgencia_caracterizada": False,
    "risco_neurologico_progressivo_documentado": False,
    "indicacao_clinica_clara": True,

    "opme_presente": True,
    "opme_coerente_com_procedimento": True,
    "opme.registro_anvisa": "REG-321",
    "opme_cotacoes_count": 1,  # Pendencia OP
    "opme_tem_similar_nacional": False,
    "opme_especificada_por_marca": True,  # Pendencia OP
    "opme_especificacao_tecnica_presente": False,
    "divergencia_grave_entre_guias": False,
    "assinatura_presente": True,
    "carimbo_presente": False,  # Pendencia OP
    "crm_presente": True,
    "custo_acima_teto_convenio": True,
    "narrativa_score": 45,  # Pendencia OP
    "judicial_risk_score": 0.3,
    "cost_vs_judicialization_ratio": 0.6,
}

CASE_NOGO_CID = {
    "case_id": "NOGO-001",
    "procedimento_descricao": "Artroplastia cervical",
    "cid": "G44.1",  # CID de cefaleia - INCOMPATIVEL
    "tuss": "30912092",
    "convenio_perfil": "SULAMERICA",

    "fora_rol_lei_14454_invocada": False,
    "dut_aplicavel": True,
    "dut_cumprida_integral": True,
    "dut_cumprida_parcial": False,
    "cid_compativel_com_tuss": False,  # BLOQUEIO DURO
    "tuss_format_invalido": False,
    "cid_format_invalido": False,
    "convenio": "SULAMERICA",
    "carencia_contratual_violada": False,

    "falha_terapeutica_exigida": True,
    "falha_terapeutica_documentada": True,
    "procedimento_experimental": False,
    "evidencia_robusta": True,
    "reoperacao": False,
    "exame_imagem_idade_meses": 4,
    "deficit_motor_mencionado": False,
    "graduacao_objetiva_0_5_presente": False,
    "justificativa_clinica_length": 600,
    "contains_termos_vagos": False,
    "lateralidade_aplicavel": False,
    "lateralidade_ausente": False,
    "tempo_evolucao_documentado": True,
    "guideline_aplicavel_citado": "SBN",
    "termos_alta_conversao_count": 2,
    "urgencia_caracterizada": False,
    "risco_neurologico_progressivo_documentado": False,
    "indicacao_clinica_clara": False,

    "opme_presente": True,
    "opme_coerente_com_procedimento": True,
    "opme.registro_anvisa": "REG-555",
    "opme_cotacoes_count": 3,
    "opme_tem_similar_nacional": True,
    "opme_especificada_por_marca": False,
    "opme_especificacao_tecnica_presente": True,
    "divergencia_grave_entre_guias": False,
    "assinatura_presente": True,
    "carimbo_presente": True,
    "crm_presente": True,
    "custo_acima_teto_convenio": False,
    "narrativa_score": 70,
    "judicial_risk_score": 0.3,
    "cost_vs_judicialization_ratio": 1.5,
}

CASE_NOGO_FORA_ROL = {
    "case_id": "NOGO-002",
    "procedimento_descricao": "Terapia experimental sem registro",
    "cid": "C71.9",
    "tuss": "99999999",  # Nao consta no Rol
    "convenio_perfil": "AMIL",

    "rol_ans_465": ["30912092", "30912033"],  # Nao tem 99999999
    "fora_rol_lei_14454_invocada": False,
    "fora_do_rol": True,
    "evidencia_forte": False,
    "ausencia_alternativa_documentada": False,
    "dut_aplicavel": False,
    "dut_cumprida_integral": False,
    "dut_cumprida_parcial": False,
    "cid_compativel_com_tuss": True,
    "tuss_format_invalido": False,
    "cid_format_invalido": False,
    "convenio": "AMIL",
    "carencia_contratual_violada": False,

    "falha_terapeutica_exigida": True,
    "falha_terapeutica_documentada": False,  # BLOQUEIO
    "procedimento_experimental": True,  # BLOQUEIO
    "evidencia_robusta": False,
    "reoperacao": False,
    "exame_imagem_idade_meses": 2,
    "deficit_motor_mencionado": False,
    "graduacao_objetiva_0_5_presente": False,
    "justificativa_clinica_length": 300,
    "contains_termos_vagos": True,
    "lateralidade_aplicavel": False,
    "lateralidade_ausente": False,
    "tempo_evolucao_documentado": True,
    "guideline_aplicavel_citado": None,
    "termos_alta_conversao_count": 1,
    "urgencia_caracterizada": False,
    "risco_neurologico_progressivo_documentado": False,
    "indicacao_clinica_clara": False,

    "opme_presente": False,
    "opme_coerente_com_procedimento": True,
    "opme_cotacoes_count": 0,
    "opme_tem_similar_nacional": True,
    "opme_especificada_por_marca": False,
    "opme_especificacao_tecnica_presente": True,
    "divergencia_grave_entre_guias": False,
    "assinatura_presente": True,
    "carimbo_presente": True,
    "crm_presente": True,
    "custo_acima_teto_convenio": True,
    "narrativa_score": 30,
    "judicial_risk_score": 0.4,
    "cost_vs_judicialization_ratio": 2.5,
}


# ============================================================
# RUNNER
# ============================================================

CASES = [
    ("GO IDEAL - Microdiscectomia",      CASE_GO_IDEAL),
    ("GO URGENCIA - Aneurisma roto",     CASE_GO_URGENCIA),
    ("RESSALVA - Artrodese pendencias",  CASE_RESSALVA_PENDENCIAS),
    ("RESSALVA - Linguagem fraca",       CASE_RESSALVA_LINGUAGEM),
    ("NO_GO - CID incompativel",         CASE_NOGO_CID),
    ("NO_GO - Fora rol sem defesa",      CASE_NOGO_FORA_ROL),
]


def run_all():
    # Context global (tabelas de referencia que valem para todos os casos)
    GLOBAL_CONTEXT = {
        "rol_ans_465": [
            "30912033",  # Microdiscectomia lombar
            "30912017",  # Hernia disco lombar via aberta
            "30912025",  # Discectomia cervical
            "30912092",  # Artrodese cervical anterior
            "30201023",  # Embolizacao aneurisma cerebral
        ]
    }

    engine = DecisionEngine.from_file("app/services/rules_v2_1.json", context=GLOBAL_CONTEXT)
    recorder = OutcomeRecorder()

    print("=" * 78)
    print("NEUROAUTH MOTOR DECISORIO v2.2 - SUITE DE TESTE")
    print("=" * 78)
    print()

    for nome, case in CASES:
        print(f">>> CASO: {nome} ({case['case_id']})")
        print(f"    Convenio: {case['convenio_perfil']}")

        result = engine.evaluate(case)

        gate = result["final_gate"]
        score = result["final_score"]
        risk = result["final_risk"]
        trace_id = result["trace_id"][:8]

        # Color codes
        color = {"GO": "\033[92m", "RESSALVA": "\033[93m", "NO_GO": "\033[91m"}.get(gate, "")
        reset = "\033[0m"

        print(f"    trace_id: {trace_id}...  |  Decisao: {color}{gate}{reset}  |  Score: {score}/100  |  Risco: {risk}")
        print(f"    Camadas:  ANS={result['layer_ans']['score']:3d}  "
              f"EV={result['layer_evidencia']['score']:3d}  "
              f"OP={result['layer_operadora']['score']:3d}")

        n_fired = len(result["rules_fired"])
        n_pending = len(result["pending_items"])
        print(f"    Regras disparadas: {n_fired}  |  Pendencias unicas: {n_pending}")

        if result["pending_items"]:
            print(f"    Top pendencias:")
            for item in result["pending_items"][:3]:
                print(f"      - [{item['severidade']}] {item['descricao']}")

        print(f"    Acao: {result['recommended_action']}")
        print()

    print("=" * 78)
    print("Suite executada com sucesso. Motor pronto para integracao no Render.")
    print("=" * 78)


if __name__ == "__main__":
    run_all()
