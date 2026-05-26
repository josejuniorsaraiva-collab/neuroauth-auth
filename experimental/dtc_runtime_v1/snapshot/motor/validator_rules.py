"""
NEUROAUTH - CHIP 2: Validator Rules v2.0.0
Catalogo de regras deterministicas. Nunca altera dado. Apenas julga.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable

SEVERIDADE_BLOQUEIO             = "BLOQUEIO"
SEVERIDADE_PENDENCIA            = "PENDENCIA_OBRIGATORIA"
SEVERIDADE_PENDENCIA_ESTRUTURAL = "PENDENCIA_ESTRUTURAL"
SEVERIDADE_ALERTA               = "ALERTA"

@dataclass
class RuleOutcome:
    codigo: str
    campo: str
    motivo: str
    severidade: str

RuleFn = Callable[[dict], "RuleOutcome | None"]

def _v(val) -> bool:
    if val is None: return True
    if isinstance(val, str): return val.strip() == ""
    if isinstance(val, (dict, list)): return len(val) == 0
    return False

def rgl001(c):
    if _v(c.get("PROFILE_ID")): return RuleOutcome("RGL001","PROFILE_ID","PROFILE_ID ausente - sem ancora no dado mestre",SEVERIDADE_BLOQUEIO)
def rgl002(c):
    if _v(c.get("PROC_NOME")): return RuleOutcome("RGL002","PROC_NOME","Nome do procedimento ausente",SEVERIDADE_BLOQUEIO)
def rgl003(c):
    if _v(c.get("CONVENIO_ID")): return RuleOutcome("RGL003","CONVENIO_ID","Convenio/operadora ausente",SEVERIDADE_BLOQUEIO)
def rgl004(c):
    if _v(c.get("CID_PRINCIPAL")): return RuleOutcome("RGL004","CID_PRINCIPAL","CID principal ausente - obrigatorio para envio",SEVERIDADE_PENDENCIA)
def rgl005(c):
    if _v(c.get("COD_TUSS")): return RuleOutcome("RGL005","COD_TUSS","Codigo TUSS ausente - campo regulatorio duro",SEVERIDADE_BLOQUEIO)
def rgl010(c):
    t = (c.get("COD_TUSS") or "").strip()
    r = c.get("REGRAS") or {}
    e = str(r.get("cod_tuss_esperado","")).strip()
    if t and e and t != e: return RuleOutcome("RGL010","COD_TUSS",f"COD_TUSS '{t}' diverge do mestre (esperado: '{e}')",SEVERIDADE_BLOQUEIO)
def rgl011(c):
    b = (c.get("COD_CBHPM") or "").strip()
    r = c.get("REGRAS") or {}
    e = str(r.get("cod_cbhpm_esperado","")).strip()
    if b and e and b != e: return RuleOutcome("RGL011","COD_CBHPM",f"COD_CBHPM '{b}' incompativel com mestre (esperado: '{e}')",SEVERIDADE_ALERTA)
def rgl012(c):
    r = c.get("REGRAS") or {}
    if r.get("exige_regras_especificas") and _v(r.get("regras_definidas")): return RuleOutcome("RGL012","REGRAS","Procedimento exige regras especificas mas regras_json esta vazio",SEVERIDADE_PENDENCIA_ESTRUTURAL)
def rgl020(c):
    r = c.get("REGRAS") or {}
    if r.get("multinivel") and c.get("NIVEIS") is None: return RuleOutcome("RGL020","NIVEIS","Procedimento multinivel sem numero de niveis informado",SEVERIDADE_BLOQUEIO)
def rgl021(c):
    n = c.get("NIVEIS"); r = c.get("REGRAS") or {}; m = r.get("min_niveis")
    if n is not None and m is not None:
        try:
            if int(n) < int(m): return RuleOutcome("RGL021","NIVEIS",f"NIVEIS ({n}) abaixo do minimo ({m})",SEVERIDADE_BLOQUEIO)
        except: pass
def rgl022(c):
    n = c.get("NIVEIS"); r = c.get("REGRAS") or {}; m = r.get("max_niveis")
    if n is not None and m is not None:
        try:
            if int(n) > int(m): return RuleOutcome("RGL022","NIVEIS",f"NIVEIS ({n}) acima do maximo ({m})",SEVERIDADE_ALERTA)
        except: pass
def rgl030(c):
    r = c.get("REGRAS") or {}
    if r.get("lateralidade_obrigatoria") and _v(c.get("LATERALIDADE")): return RuleOutcome("RGL030","LATERALIDADE","Procedimento exige lateralidade e campo esta ausente",SEVERIDADE_PENDENCIA)
def rgl040(c):
    r = c.get("REGRAS") or {}
    ob = r.get("opme_obrigatoria", c.get("NECESSITA_OPME", False))
    if ob and _v(c.get("OPME_JSON")): return RuleOutcome("RGL040","OPME_JSON","Procedimento exige OPME e nenhum material foi informado",SEVERIDADE_PENDENCIA)
def rgl041(c):
    o = c.get("OPME_JSON") or {}; r = c.get("REGRAS") or {}; p = r.get("opme_materiais_permitidos",[])
    if o and p:
        for item in o.get("materiais",[]):
            cod = item.get("codigo","")
            if cod and cod not in p: return RuleOutcome("RGL041","OPME_JSON",f"Material '{cod}' incompativel com procedimento",SEVERIDADE_BLOQUEIO)
def rgl042(c):
    o = c.get("OPME_JSON") or {}; r = c.get("REGRAS") or {}
    if o and r.get("opme_quantidade_por_niveis") and c.get("NIVEIS") is None: return RuleOutcome("RGL042","OPME_JSON","Quantidade de OPME depende de NIVEIS mas NIVEIS esta ausente",SEVERIDADE_BLOQUEIO)
def rgl050(c):
    cid = c.get("CID_PRINCIPAL",""); r = c.get("REGRAS") or {}; pref = r.get("cids_preferenciais",[])
    if cid and pref and cid not in pref: return RuleOutcome("RGL050","CID_PRINCIPAL",f"CID '{cid}' fora do conjunto preferencial {pref} - risco de glosa",SEVERIDADE_ALERTA)
def rgl051(c):
    cid = c.get("CID_PRINCIPAL",""); r = c.get("REGRAS") or {}; inc = r.get("cids_incompativeis",[])
    if cid and inc and cid in inc: return RuleOutcome("RGL051","CID_PRINCIPAL",f"CID '{cid}' incompativel com familia do procedimento",SEVERIDADE_BLOQUEIO)
def rgl060(c):
    r = c.get("REGRAS") or {}
    if r.get("carater_obrigatorio") and _v(c.get("CARATER")): return RuleOutcome("RGL060","CARATER","Procedimento exige carater e campo esta ausente",SEVERIDADE_PENDENCIA)
def rgl061(c):
    car = (c.get("CARATER") or "").lower(); r = c.get("REGRAS") or {}
    if car in {"urgencia","urgência","emergencia","emergência"} and not r.get("aceita_urgencia",True):
        return RuleOutcome("RGL061","CARATER","Procedimento nao aceita carater urgencia pela regra da operadora",SEVERIDADE_ALERTA)

ALL_RULES: list[RuleFn] = [
    rgl001,rgl002,rgl003,rgl004,rgl005,
    rgl010,rgl011,rgl012,
    rgl020,rgl021,rgl022,
    rgl030,
    rgl040,rgl041,rgl042,
    rgl050,rgl051,
    rgl060,rgl061,
]
