"""
NEUROAUTH - CHIP 2: Validator Engine v2.0.0
Percorre regras sobre objeto canonico. Nunca altera dado. Nunca lanca excecao em producao.
"""
from __future__ import annotations
from typing import Any
from .validator_rules import ALL_RULES, RuleOutcome, SEVERIDADE_BLOQUEIO, SEVERIDADE_PENDENCIA, SEVERIDADE_PENDENCIA_ESTRUTURAL, SEVERIDADE_ALERTA

ENGINE_VERSION = "v2.0.0"
CAMPOS_MONITORADOS = ["CASE_ID","USER_ID","PROFILE_ID","PROC_NOME","COD_TUSS","COD_CBHPM","CID_PRINCIPAL","CONVENIO_ID","CARATER","NIVEIS","LATERALIDADE","TIPO_ANESTESIA","VIA_ACESSO","ESPECIALIDADE","PORTE","PORTE_ANESTESICO","FILME","NECESSITA_OPME","OPME_JSON","DADOS_PACIENTE","CONTEXTO_CLINICO","REGRAS"]
CAMPOS_PROTEGIDOS = frozenset({"COD_TUSS","CID_PRINCIPAL","OPME_JSON","PROFILE_ID","CONVENIO_ID","CARATER","LATERALIDADE"})
THRESHOLD_AUTOFILL = 0.85

def _empty(v: Any) -> bool:
    if v is None: return True
    if isinstance(v, str): return v.strip() == ""
    if isinstance(v, (dict, list)): return len(v) == 0
    return False

def _campos_com_problema(outcomes) -> set:
    return {o["campo"] for o in outcomes}

def _campos_ok(canonical, problemas):
    return [f for f in CAMPOS_MONITORADOS if f not in problemas and not _empty(canonical.get(f))]

def _campos_inferidos(canonical):
    sf = canonical.get("STATUS_FONTE",{}); cf = canonical.get("CONFIANCA",{})
    return [{"campo":f,"valor":canonical.get(f),"confianca":cf.get(f,0.0),"fonte":canonical.get("SOURCE_MAP",{}).get(f,"desconhecida")}
            for f,s in sf.items() if s == "inferido" and f not in CAMPOS_PROTEGIDOS and cf.get(f,0.0) >= THRESHOLD_AUTOFILL]

def _global_confidence(canonical):
    chave = ["PROFILE_ID","PROC_NOME","COD_TUSS","CID_PRINCIPAL","CONVENIO_ID"]
    cf = canonical.get("CONFIANCA",{}); scores = [cf.get(f,0.0) for f in chave]
    return round(sum(scores)/len(scores),3) if scores else 0.0

def validate_case(canonical: dict, extra_rules: list | None = None) -> dict:
    """CHIP 2 - Valida objeto canonico. Stateless. Nunca lanca excecao."""
    rules = list(ALL_RULES) + (extra_rules or [])
    bloqueios=[]; pendencias=[]; alertas=[]; erros=[]
    for fn in rules:
        try:
            o: RuleOutcome | None = fn(canonical)
        except Exception as e:
            erros.append({"regra": getattr(fn,"__name__","?"), "erro": str(e)}); continue
        if o is None: continue
        entry = {"codigo":o.codigo,"campo":o.campo,"motivo":o.motivo}
        if o.severidade == SEVERIDADE_BLOQUEIO: bloqueios.append(entry)
        elif o.severidade in (SEVERIDADE_PENDENCIA, SEVERIDADE_PENDENCIA_ESTRUTURAL): pendencias.append(entry)
        elif o.severidade == SEVERIDADE_ALERTA: alertas.append(entry)
    prob = _campos_com_problema(bloqueios+pendencias+alertas)
    if bloqueios: status = "BLOQUEADO"
    elif pendencias: status = "PENDENCIA_OBRIGATORIA"
    elif alertas: status = "ALERTA"
    else: status = "OK"
    return {"status_validacao":status,"bloqueios":bloqueios,"pendencias":pendencias,"alertas":alertas,"campos_ok":_campos_ok(canonical,prob),"campos_inferidos":_campos_inferidos(canonical),"confidence_global":_global_confidence(canonical),"erros_internos":erros,"engine_version":ENGINE_VERSION}
