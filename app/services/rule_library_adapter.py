"""
app/services/rule_library_adapter.py
NEUROAUTH — Rule Library Adapter v1.0.0

Adapter único entre os validators e a fonte externa de regras.
Suporta duas fontes com fallback seguro:

  FONTE A: Google Sheets (planilha neuroauth)
    - 09_REGRAS_DECISAO  → regras com if_json/then_json
    - 11_REGRAS_BLOQUEIO → bloqueios duros
    - 10_REGRAS_ALERTA   → alertas
    - 03_CONVENIOS       → perfis de operadora
    - 20_PESOS           → pesos de score

  FONTE B: RULE_LIBRARY (aba futura ou xlsx)
    - schema novo: rule_id, layer, validation_logic_json, etc.

  FALLBACK: regras embutidas nos validators (sempre disponível)

Interface pública:
  load_rules(layer=None)         → list[dict]
  get_rules_by_layer(layer)      → list[dict]
  get_operator_profiles()        → dict[str, dict]
  get_rule_values()              → dict[str, list]
  get_test_cases()               → list[dict]
  evaluate_condition(logic, ctx) → bool   # parser seguro de validation_logic
  reload()                       → None   # força recarga do cache
"""
from __future__ import annotations

import json
import logging
import time
import threading
from typing import Any

logger = logging.getLogger("neuroauth.rule_library_adapter")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

# TTL do cache em segundos (5 minutos — evita chamadas repetidas ao Sheets)
_CACHE_TTL = 300

# Abas da planilha neuroauth (fonte A)
TAB_REGRAS_DECISAO  = "09_REGRAS_DECISAO"
TAB_REGRAS_ALERTA   = "10_REGRAS_ALERTA"
TAB_REGRAS_BLOQUEIO = "11_REGRAS_BLOQUEIO"
TAB_CONVENIOS       = "03_CONVENIOS"
TAB_PESOS           = "20_PESOS"
TAB_RULE_LIBRARY    = "RULE_LIBRARY"   # aba futura (schema novo)

# Linha de cabeçalho (1-indexed) nas abas com 3 linhas de título
_HEAD = 3

# ══════════════════════════════════════════════════════════════════════════════
# CACHE THREAD-SAFE
# ══════════════════════════════════════════════════════════════════════════════

_cache: dict[str, Any]   = {}
_cache_ts: dict[str, float] = {}
_cache_lock = threading.Lock()


def _cache_get(key: str) -> Any | None:
    with _cache_lock:
        ts = _cache_ts.get(key, 0)
        if time.time() - ts > _CACHE_TTL:
            return None
        return _cache.get(key)


def _cache_set(key: str, value: Any) -> None:
    with _cache_lock:
        _cache[key]    = value
        _cache_ts[key] = time.time()


def reload() -> None:
    """Força limpeza do cache — próxima chamada recarrega do Sheets."""
    with _cache_lock:
        _cache.clear()
        _cache_ts.clear()
    logger.info("rule_library_adapter: cache limpo — próxima leitura buscará do Sheets")


# ══════════════════════════════════════════════════════════════════════════════
# ACESSO AO SHEETS
# ══════════════════════════════════════════════════════════════════════════════

def _get_sheets_client():
    """Retorna cliente gspread. Lança exceção se não configurado."""
    import gspread
    from google.oauth2.service_account import Credentials
    from app.core.config import settings

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    return gspread.authorize(creds)


def _read_tab_as_dicts(ss, tab_name: str, head: int = _HEAD) -> list[dict]:
    """Lê aba do Sheets e retorna lista de dicts usando a linha `head` como cabeçalho."""
    try:
        ws   = ss.worksheet(tab_name)
        vals = ws.get_all_values()
        if len(vals) <= head:
            return []
        headers = [str(h).strip() for h in vals[head - 1]]
        rows    = vals[head:]
        result  = []
        for row in rows:
            if not any(str(c).strip() for c in row):
                continue
            d = {headers[i]: (row[i] if i < len(row) else "")
                 for i in range(len(headers)) if headers[i]}
            result.append(d)
        return result
    except Exception as e:
        logger.warning("rule_library_adapter: falha ao ler aba %s: %s", tab_name, e)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# NORMALIZAÇÃO: FONTE A → schema Rule Library
# Converte regras do formato if_json/then_json para o schema canônico
# ══════════════════════════════════════════════════════════════════════════════

def _tipo_to_layer(tipo: str) -> str:
    mapa = {"bloqueio": "ANS", "alerta": "EVIDENCIA", "info": "OPERADORA"}
    return mapa.get((tipo or "").lower(), "EVIDENCIA")


def _tipo_to_actions(tipo: str) -> tuple[str, str]:
    """Retorna (failure_action, severity) para cada tipo_regra."""
    mapa = {
        "bloqueio": ("BLOCK",       "CRITICA"),
        "alerta":   ("WARN",        "MODERADA"),
        "info":     ("WARN",        "BAIXA"),
    }
    return mapa.get((tipo or "").lower(), ("WARN", "BAIXA"))


def _normalize_fonte_a(raw: dict) -> dict:
    """Normaliza regra do formato 09_REGRAS_DECISAO para schema canônico."""
    tipo    = raw.get("tipo_regra", "alerta").lower()
    layer   = _tipo_to_layer(tipo)
    action, severity = _tipo_to_actions(tipo)
    blocking = tipo == "bloqueio"

    # Extrai mensagem do then_json
    msg = ""
    then_raw = raw.get("then_json", "")
    try:
        then = json.loads(then_raw) if then_raw else {}
        blocks = then.get("add_blocks", [])
        alerts = then.get("add_alerts", [])
        msg    = (blocks + alerts)[0][:150] if (blocks or alerts) else ""
    except Exception:
        msg = then_raw[:150] if then_raw else ""

    return {
        "rule_id":          raw.get("regra_id", ""),
        "rule_name":        raw.get("nome_regra", ""),
        "layer":            layer,
        "status":           "ACTIVE" if raw.get("ativo","").upper() == "TRUE" else "INACTIVE",
        "priority":         int(raw.get("prioridade_execucao", 50) or 50),
        "severity":         severity,
        "blocking":         blocking,
        "rule_type":        tipo.upper(),
        "specialty":        "",
        "procedure_family": raw.get("profile_id", ""),
        "operator_name":    raw.get("convenio_id", "GLOBAL"),
        "applies_if_json":  raw.get("if_json", ""),
        "validation_logic_json": raw.get("if_json", ""),
        "failure_action":   action,
        "gate_if_fail":     "NO_GO" if blocking else "GO_COM_RESSALVAS",
        "score_impact":     -30 if blocking else -10,
        "user_message":     msg,
        "source_type":      "OPERADORA" if raw.get("convenio_id","") not in ("GLOBAL","") else "ANS",
        "version":          raw.get("versao_regra", "1.0.0"),
        "_source":          "fonte_a",
    }


def _normalize_fonte_b(raw: dict) -> dict:
    """Regra já no schema RULE_LIBRARY — só garante campos mínimos."""
    return {
        "rule_id":          raw.get("rule_id", ""),
        "rule_name":        raw.get("rule_name", ""),
        "layer":            raw.get("layer", "ANS"),
        "status":           raw.get("status", "ACTIVE"),
        "priority":         int(raw.get("priority", 50) or 50),
        "severity":         raw.get("severity", "MODERADA"),
        "blocking":         str(raw.get("blocking","")).upper() == "TRUE",
        "rule_type":        raw.get("rule_type", "BINARIA"),
        "specialty":        raw.get("specialty", ""),
        "procedure_family": raw.get("procedure_name", raw.get("procedure_family", "")),
        "operator_name":    raw.get("operator_name", "GLOBAL"),
        "applies_if_json":  raw.get("applies_if_json", ""),
        "excludes_if_json": raw.get("excludes_if_json", ""),
        "validation_logic_json": raw.get("validation_logic_json", ""),
        "failure_action":   raw.get("failure_action", "WARN"),
        "gate_if_fail":     raw.get("gate_if_fail", "GO_COM_RESSALVAS"),
        "score_impact":     int(raw.get("score_impact", 0) or 0),
        "user_message":     raw.get("user_message", ""),
        "source_type":      raw.get("source_type", "ANS"),
        "version":          raw.get("version", "1.0.0"),
        "_source":          "fonte_b",
    }


# ══════════════════════════════════════════════════════════════════════════════
# CARREGAMENTO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def _load_from_sheets() -> dict[str, Any]:
    """
    Carrega todas as abas relevantes do Sheets.
    Retorna dict com chaves: rules, operator_profiles, pesos.
    Lança exceção se Sheets não estiver acessível.
    """
    from app.core.config import settings
    if not settings.GOOGLE_APPLICATION_CREDENTIALS or not settings.SPREADSHEET_ID:
        raise RuntimeError("Credenciais Sheets não configuradas")

    gc = _get_sheets_client()
    ss = gc.open_by_key(settings.SPREADSHEET_ID)

    # Fonte A — abas existentes
    raw_decisao  = _read_tab_as_dicts(ss, TAB_REGRAS_DECISAO)
    raw_alerta   = _read_tab_as_dicts(ss, TAB_REGRAS_ALERTA)
    raw_bloqueio = _read_tab_as_dicts(ss, TAB_REGRAS_BLOQUEIO)

    rules_a = [_normalize_fonte_a(r) for r in raw_decisao + raw_alerta + raw_bloqueio
               if r.get("regra_id") and r.get("ativo","").upper() == "TRUE"]

    # Fonte B — aba RULE_LIBRARY (pode não existir ainda)
    raw_lib = _read_tab_as_dicts(ss, TAB_RULE_LIBRARY)
    rules_b = [_normalize_fonte_b(r) for r in raw_lib
               if r.get("rule_id") and r.get("status","").upper() == "ACTIVE"]

    # Merge: Fonte B tem prioridade sobre A se mesmo rule_id
    rules_map: dict[str, dict] = {}
    for r in rules_a:
        rules_map[r["rule_id"]] = r
    for r in rules_b:
        rules_map[r["rule_id"]] = r   # sobrescreve

    # Operadoras
    raw_conv = _read_tab_as_dicts(ss, TAB_CONVENIOS)
    op_profiles: dict[str, dict] = {}
    for row in raw_conv:
        cid = (row.get("convenio_id") or "").strip()
        if cid and row.get("ativo","").upper() == "TRUE":
            op_profiles[cid.lower()] = {
                "convenio_id":    cid,
                "name":           row.get("nome_convenio", cid),
                "grupo":          row.get("operadora_grupo", ""),
                "usa_tiss":       row.get("usa_portal_tiss","").upper() == "TRUE",
                "modelo":         row.get("modelo_autorizacao",""),
                "observacoes":    row.get("observacoes",""),
                "_source":        "sheets",
            }

    # Pesos
    raw_pesos = _read_tab_as_dicts(ss, TAB_PESOS)
    pesos: list[dict] = raw_pesos

    return {
        "rules":            list(rules_map.values()),
        "operator_profiles":op_profiles,
        "pesos":            pesos,
        "loaded_at":        time.time(),
        "source":           "sheets",
        "rules_a_count":    len(rules_a),
        "rules_b_count":    len(rules_b),
    }


# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════

def load_rules(layer: str | None = None) -> list[dict]:
    """
    Carrega regras externas. Usa cache TTL=5min.
    Fallback automático para lista vazia se Sheets falhar.
    """
    cached = _cache_get("data")
    if cached is None:
        try:
            data = _load_from_sheets()
            _cache_set("data", data)
            logger.info(
                "rule_library_adapter: %d regras carregadas do Sheets "
                "(fonteA=%d fonteB=%d operadoras=%d)",
                len(data["rules"]),
                data["rules_a_count"],
                data["rules_b_count"],
                len(data["operator_profiles"]),
            )
        except Exception as exc:
            logger.warning(
                "rule_library_adapter: falha ao carregar Sheets (%s) — "
                "validators usarão fallback embutido", exc
            )
            _cache_set("data", {"rules": [], "operator_profiles": {}, "pesos": []})
        cached = _cache_get("data")

    rules = cached.get("rules", []) if cached else []
    if layer:
        return [r for r in rules if r.get("layer", "").upper() == layer.upper()
                and r.get("status", "ACTIVE").upper() == "ACTIVE"]
    return [r for r in rules if r.get("status", "ACTIVE").upper() == "ACTIVE"]


def get_rules_by_layer(layer: str) -> list[dict]:
    """Alias semântico de load_rules(layer=...)."""
    return load_rules(layer=layer)


def get_operator_profiles() -> dict[str, dict]:
    """Retorna perfis de operadora carregados do Sheets."""
    cached = _cache_get("data")
    if cached is None:
        load_rules()  # força carregamento
        cached = _cache_get("data")
    return (cached or {}).get("operator_profiles", {})


def get_rule_values() -> dict[str, list]:
    """Valores de referência para enums (fixos por enquanto)."""
    return {
        "layer":          ["ANS", "EVIDENCIA", "OPERADORA"],
        "rule_type":      ["BINARIA","SCORE","ALERTA","RESSALVA","CHECKLIST","JUNTA"],
        "failure_action": ["BLOCK","WARN","SCORE_DOWN","SEND_TO_JUNTA",
                           "REQUEST_DOC","FLAG_OPME","FLAG_GLOSA"],
        "gate":           ["GO","GO_COM_RESSALVAS","NO_GO","JUNTA"],
        "severity":       ["BAIXA","MODERADA","ALTA","CRITICA"],
        "status":         ["ACTIVE","INACTIVE","TEST"],
    }


def get_test_cases() -> list[dict]:
    """Retorna casos de teste da aba RULE_TEST_CASES se existir."""
    cached = _cache_get("data")
    if not cached:
        return []
    # Placeholder — aba de test cases não existe ainda no Sheets
    return []


# ══════════════════════════════════════════════════════════════════════════════
# PARSER SEGURO DE CONDIÇÕES (sem eval)
# ══════════════════════════════════════════════════════════════════════════════

_OP_MAP = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">":  lambda a, b: a >  b,
    "<":  lambda a, b: a <  b,
    "=":  lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "in": lambda a, b: a in (b if isinstance(b, list) else [b]),
    "not_in": lambda a, b: a not in (b if isinstance(b, list) else [b]),
    "contains": lambda a, b: str(b).lower() in str(a).lower(),
    "exists":   lambda a, b: (a not in (None, "", [])) == b,
    "lt":  lambda a, b: a <  b,
    "lte": lambda a, b: a <= b,
    "gt":  lambda a, b: a >  b,
    "gte": lambda a, b: a >= b,
    "eq":  lambda a, b: a == b,
    "neq": lambda a, b: a != b,
}


def _get_nested(ctx: dict, field: str) -> Any:
    """
    Acessa campo no contexto por caminho pontilhado.
    "clinical_context.indicacao_len" → ctx["clinical_context"]["indicacao_len"]
    Fallback para campo flat se não encontrar nested.
    """
    parts = field.split(".")
    val   = ctx
    for p in parts:
        if isinstance(val, dict):
            val = val.get(p)
        else:
            val = None
            break
    # fallback: tenta campo flat com nome original
    if val is None and "." in field:
        val = ctx.get(field)
    return val


def _eval_condition(cond: dict, ctx: dict) -> bool:
    """
    Avalia uma condição simples: {"field": "...", "op": "...", "value": ...}
    ou formato compacto: {"campo": {"op_str": value}}
    """
    # Formato canônico: {"field": "x", "op": ">=", "value": 6}
    if "field" in cond and "op" in cond:
        field = cond["field"]
        op    = cond["op"]
        value = cond["value"]
        ctx_val = _get_nested(ctx, field)
        fn = _OP_MAP.get(op)
        if fn is None:
            logger.warning("evaluate_condition: operador desconhecido '%s'", op)
            return True  # passa no doubt
        try:
            # Coerce tipos numéricos
            if isinstance(value, (int, float)) and ctx_val is not None:
                ctx_val = type(value)(ctx_val)
            return bool(fn(ctx_val, value))
        except Exception as e:
            logger.debug("evaluate_condition: erro em %s %s %s: %s", field, op, value, e)
            return True  # passa no doubt

    # Formato compacto do Sheets: {"campo": {"lt": 3}}
    for field, ops_dict in cond.items():
        if isinstance(ops_dict, dict):
            ctx_val = _get_nested(ctx, field)
            for op_str, val in ops_dict.items():
                fn = _OP_MAP.get(op_str)
                if fn:
                    try:
                        if isinstance(val, (int,float)) and ctx_val is not None:
                            ctx_val = type(val)(ctx_val)
                        if not fn(ctx_val, val):
                            return False
                    except Exception:
                        pass
        elif isinstance(ops_dict, (str, int, float, bool)):
            # {"campo": "valor"} — igualdade direta
            ctx_val = _get_nested(ctx, field)
            if ctx_val != ops_dict:
                return False
    return True


def evaluate_condition(logic: str | dict, ctx: dict) -> bool:
    """
    Avalia validation_logic_json ou applies_if_json contra o contexto.

    Suporta:
      {"operator": "AND", "conditions": [...]}
      {"operator": "OR",  "conditions": [...]}
      {"campo": {"op": value}}  (formato compacto Sheets)
      ""  →  True (sem condição = sempre aplica)
    """
    if not logic:
        return True

    try:
        parsed = json.loads(logic) if isinstance(logic, str) else logic
    except (json.JSONDecodeError, TypeError):
        return True  # lógica inválida → não bloqueia

    if not parsed:
        return True

    # Formato com operator explícito
    if "operator" in parsed and "conditions" in parsed:
        conds = parsed["conditions"]
        op    = parsed["operator"].upper()
        if op == "AND":
            return all(_eval_condition(c, ctx) for c in conds)
        if op == "OR":
            return any(_eval_condition(c, ctx) for c in conds)
        return True

    # Formato compacto: dict de campo → {op: val}
    return _eval_condition(parsed, ctx)
