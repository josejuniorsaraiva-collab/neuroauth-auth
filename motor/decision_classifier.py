"""
NEUROAUTH - CHIP 4: Decision Classifier v2.0.0
Converte validacao em decisao operacional: GO | GO_COM_RESSALVAS | PENDENCIA_OBRIGATORIA | NO_GO
"""
from __future__ import annotations
from .schema_mapper import normalize_case, CAMPOS_PROTEGIDOS
from .validator_engine import validate_case, ENGINE_VERSION

THRESHOLD_AUTOFILL = 0.85

def _autopreenchimentos(campos_inferidos):
    return [{"campo":i["campo"],"valor":i["valor"],"justificativa":f"Inferido de {i['fonte']} com confianca {i['confianca']:.2f}"}
            for i in campos_inferidos if i["campo"] not in CAMPOS_PROTEGIDOS and i["confianca"] >= THRESHOLD_AUTOFILL]

def _proxima_acao(status, bloqueios, pendencias):
    if status == "NO_GO":
        return "Corrigir bloqueios em: " + ", ".join({b["campo"] for b in bloqueios}) + ". Caso nao pode avancar."
    if status == "PENDENCIA_OBRIGATORIA":
        return "Preencher campos pendentes: " + ", ".join({p["campo"] for p in pendencias}) + ". Rascunho pode ser salvo, envio bloqueado."
    if status == "GO_COM_RESSALVAS":
        return "Revisar alertas antes de enviar. Autorizacao pode ser gerada com ressalvas registradas."
    return "Caso validado. Autorizacao pode ser gerada e enviada."

def classify_case(validation: dict) -> dict:
    """CHIP 4 - Classifica decisao. Stateless. can_send/can_autofill com regras absolutas."""
    b = validation.get("bloqueios",[]); p = validation.get("pendencias",[]); a = validation.get("alertas",[])
    if b:       status, go = "NO_GO",                 "NO_GO"
    elif p:     status, go = "PENDENCIA_OBRIGATORIA", "PENDENCIA"
    elif a:     status, go = "GO_COM_RESSALVAS",      "GO_COM_RESSALVAS"
    else:       status, go = "GO",                    "GO"
    can_send     = status in ("GO","GO_COM_RESSALVAS")
    can_autofill = status in ("GO","GO_COM_RESSALVAS")
    if any(pp["campo"] in CAMPOS_PROTEGIDOS for pp in p): can_autofill = False
    ci = validation.get("campos_inferidos",[])
    partes = ([f"{len(b)} bloqueio(s)"] if b else []) + ([f"{len(p)} pendencia(s)"] if p else []) + ([f"{len(a)} alerta(s)"] if a else []) or ["caso limpo"]
    return {"decision_status":status,"go_class":go,"confidence_global":validation.get("confidence_global",0.0),"can_send":can_send,"can_autofill":can_autofill,"resumo_operacional":f"{status}: {', '.join(partes)}.","bloqueios":b,"pendencias":p,"alertas":a,"campos_ok":validation.get("campos_ok",[]),"campos_inferidos":ci,"autopreenchimentos":_autopreenchimentos(ci),"proxima_acao_sugerida":_proxima_acao(status,b,p),"engine_version":ENGINE_VERSION}

def run_motor(raw_case: dict, proc_master_row: dict|None=None, convenio_row: dict|None=None, session_user_id: str="", extra_rules: list|None=None) -> dict:
    """
    Pipeline completo: CHIP1 -> CHIP2 -> CHIP4.
    Garantias: sem proc_master_row -> NO_GO SYS001; campos protegidos nunca inferidos; stateless; nunca lanca excecao.
    """
    if not proc_master_row:
        return {"decision_status":"NO_GO","go_class":"NO_GO","confidence_global":0.0,"can_send":False,"can_autofill":False,"resumo_operacional":"Dado mestre ausente. Motor nao pode operar sem proc_master_row.","bloqueios":[{"codigo":"SYS001","campo":"proc_master_row","motivo":"Dado mestre do procedimento nao foi injetado na chamada"}],"pendencias":[],"alertas":[],"campos_ok":[],"campos_inferidos":[],"autopreenchimentos":[],"proxima_acao_sugerida":"Injetar proc_master_row antes de reprocessar o caso","engine_version":ENGINE_VERSION}
    try:
        canonical  = normalize_case(raw_case, proc_master_row, convenio_row, session_user_id)
        validation = validate_case(canonical, extra_rules)
        return classify_case(validation)
    except Exception as e:
        return {"decision_status":"NO_GO","go_class":"NO_GO","confidence_global":0.0,"can_send":False,"can_autofill":False,"resumo_operacional":f"Erro interno do motor: {e}","bloqueios":[{"codigo":"SYS002","campo":"motor","motivo":f"Excecao interna: {e}"}],"pendencias":[],"alertas":[],"campos_ok":[],"campos_inferidos":[],"autopreenchimentos":[],"proxima_acao_sugerida":"Reportar erro ao time tecnico com payload completo","engine_version":ENGINE_VERSION}
