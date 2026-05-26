"""
NEUROAUTH - CHIP 4: Decision Classifier v2.1.0
Stage-by-stage hardened run_motor().
Principios:
  1. Nunca explodir para fora â qualquer excecao retorna JSON valido
  2. Sempre retornar JSON valido â sem excecoes nao tratadas
  3. Degradar com seguranca â stage com erro usa fallback e continua
  4. Logar exatamente onde falhou â error_code + stage no payload
"""
from __future__ import annotations
import uuid
import traceback
import logging
from datetime import datetime, timezone

from .schema_mapper import normalize_case, CAMPOS_PROTEGIDOS
from .validator_engine import validate_case, ENGINE_VERSION

logger = logging.getLogger("neuroauth.motor")

THRESHOLD_AUTOFILL = 0.85

# Campos minimos obrigatorios para o motor operar
_CONTRATO_MINIMO = ["proc_nome", "procedimento", "cid", "cid_principal", "convenio", "convenio_id"]


# âââââââââââââââââââââââââââââââââââââââââââââ
# HELPERS
# âââââââââââââââââââââââââââââââââââââââââââââ

def _build_error_response(
    error_code: str,
    message: str,
    stage: str,
    request_id: str,
    payload_summary: dict | None = None,
    extra: dict | None = None,
) -> dict:
    """Retorna resposta de erro estruturada e sempre valida."""
    return {
        "decision_status": "NO_GO",
        "go_class": "NO_GO",
        "confidence_global": 0.0,
        "can_send": False,
        "can_autofill": False,
        "resumo_operacional": f"[{stage}] {message}",
        "bloqueios": [{"codigo": error_code, "campo": stage, "motivo": message}],
        "pendencias": [],
        "alertas": [],
        "campos_ok": [],
        "campos_inferidos": [],
        "autopreenchimentos": [],
        "proxima_acao_sugerida": "Reportar erro ao time tecnico com request_id e payload.",
        "engine_version": ENGINE_VERSION,
        "_error": {
            "request_id": request_id,
            "error_code": error_code,
            "stage": stage,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload_summary": payload_summary or {},
            **(extra or {}),
        },
    }


def _summarize_payload(raw_case: dict | None) -> dict:
    """Extrai campos-chave para log â nunca falha."""
    try:
        if not isinstance(raw_case, dict):
            return {"tipo": str(type(raw_case)), "raw": str(raw_case)[:100]}
        return {
            "proc": raw_case.get("procedimento") or raw_case.get("proc_nome") or raw_case.get("PROC_NOME", "?"),
            "cid": raw_case.get("cid") or raw_case.get("cid_principal") or raw_case.get("CID_PRINCIPAL", "?"),
            "convenio": raw_case.get("convenio") or raw_case.get("convenio_id") or raw_case.get("CONVENIO_ID", "?"),
            "keys": list(raw_case.keys())[:10],
        }
    except Exception:
        return {"erro": "summarize_falhou"}


def _normalize_payload(raw_case) -> tuple[dict, str | None]:
    """
    Stage 1: garante que o payload e um dict nao-nulo.
    Retorna (dict_limpo, mensagem_erro | None).
    """
    if raw_case is None:
        return {}, "Payload nulo recebido pelo motor"
    if not isinstance(raw_case, dict):
        try:
            import json
            parsed = json.loads(raw_case)
            if isinstance(parsed, dict):
                return parsed, None
        except Exception:
            pass
        return {}, f"Payload deve ser dict, recebido: {type(raw_case).__name__}"
    # Garantir que chaves sao strings (defensivo)
    try:
        return {str(k): v for k, v in raw_case.items()}, None
    except Exception as e:
        return {}, f"Falha ao normalizar chaves do payload: {e}"


def _validate_min_contract(raw_case: dict) -> str | None:
    """
    Stage 2: verifica contrato minimo (pelo menos um campo de procedimento
    E pelo menos um campo de CID).
    Retorna mensagem de erro ou None se OK.
    """
    tem_proc = any(raw_case.get(k) for k in ["proc_nome", "procedimento", "PROC_NOME", "PROFILE_ID", "profile_id"])
    tem_cid = any(raw_case.get(k) for k in ["cid", "cid_principal", "CID_PRINCIPAL", "diagnostico_cid"])
    if not tem_proc and not tem_cid:
        return "Payload nao contÃ©m nem procedimento nem CID â contrato minimo nao atendido"
    if not tem_proc:
        return "Campo de procedimento ausente no payload (proc_nome / procedimento / profile_id)"
    if not tem_cid:
        return "Campo de CID ausente no payload (cid / cid_principal)"
    return None


# âââââââââââââââââââââââââââââââââââââââââââââ
# CLASSIFICADOR (stateless)
# âââââââââââââââââââââââââââââââââââââââââââââ

def _autopreenchimentos(campos_inferidos):
    return [
        {
            "campo": i["campo"],
            "valor": i["valor"],
            "justificativa": f"Inferido de {i['fonte']} com confianca {i['confianca']:.2f}",
        }
        for i in campos_inferidos
        if i["campo"] not in CAMPOS_PROTEGIDOS and i["confianca"] >= THRESHOLD_AUTOFILL
    ]


def _proxima_acao(status, bloqueios, pendencias):
    if status == "NO_GO":
        return "Corrigir bloqueios em: " + ", ".join({b["campo"] for b in bloqueios}) + ". Caso nao pode avancar."
    if status == "PENDENCIA_OBRIGATORIA":
        return (
            "Preencher campos pendentes: "
            + ", ".join({p["campo"] for p in pendencias})
            + ". Rascunho pode ser salvo, envio bloqueado."
        )
    if status == "GO_COM_RESSALVAS":
        return "Revisar alertas antes de enviar. Autorizacao pode ser gerada com ressalvas registradas."
    return "Caso validado. Autorizacao pode ser gerada e enviada."


def classify_case(validation: dict) -> dict:
    """CHIP 4 â Classifica decisao. Stateless."""
    b = validation.get("bloqueios", [])
    p = validation.get("pendencias", [])
    a = validation.get("alertas", [])
    if b:
        status, go = "NO_GO", "NO_GO"
    elif p:
        status, go = "PENDENCIA_OBRIGATORIA", "PENDENCIA"
    elif a:
        status, go = "GO_COM_RESSALVAS", "GO_COM_RESSALVAS"
    else:
        status, go = "GO", "GO"
    can_send = status in ("GO", "GO_COM_RESSALVAS")
    can_autofill = status in ("GO", "GO_COM_RESSALVAS")
    if any(pp["campo"] in CAMPOS_PROTEGIDOS for pp in p):
        can_autofill = False
    ci = validation.get("campos_inferidos", [])
    partes = (
        ([f"{len(b)} bloqueio(s)"] if b else [])
        + ([f"{len(p)} pendencia(s)"] if p else [])
        + ([f"{len(a)} alerta(s)"] if a else [])
    ) or ["caso limpo"]
    return {
        "decision_status": status,
        "go_class": go,
        "confidence_global": validation.get("confidence_global", 0.0),
        "can_send": can_send,
        "can_autofill": can_autofill,
        "resumo_operacional": f"{status}: {', '.join(partes)}.",
        "bloqueios": b,
        "pendencias": p,
        "alertas": a,
        "campos_ok": validation.get("campos_ok", []),
        "campos_inferidos": ci,
        "autopreenchimentos": _autopreenchimentos(ci),
        "proxima_acao_sugerida": _proxima_acao(status, b, p),
        "engine_version": ENGINE_VERSION,
    }


# âââââââââââââââââââââââââââââââââââââââââââââ
# PIPELINE PRINCIPAL
# âââââââââââââââââââââââââââââââââââââââââââââ

def run_motor(
    raw_case: dict,
    proc_master_row: dict | None = None,
    convenio_row: dict | None = None,
    session_user_id: str = "",
    extra_rules: list | None = None,
) -> dict:
    """
    Pipeline completo com isolamento por stage.
    Stage 1: normalize_payload
    Stage 2: validate_min_contract
    Stage 3: schema_mapper  (normalize_case)
    Stage 4: validator_engine (validate_case)
    Stage 5: classify_case
    Global: SYS_GLOBAL_ERROR se tudo falhar
    """
    request_id = str(uuid.uuid4())

    # ââ Stage 1: Normalizar payload ââââââââââââââââââââââââââ
    try:
        clean_case, norm_err = _normalize_payload(raw_case)
        if norm_err:
            logger.error("SYS_INPUT_NORM_FAIL request_id=%s err=%s", request_id, norm_err)
            return _build_error_response(
                "SYS_INPUT_NORM_FAIL", norm_err, "stage1_normalize",
                request_id, _summarize_payload(raw_case),
            )
    except Exception as _e:
        tb = traceback.format_exc()
        logger.error("SYS_INPUT_NORM_FAIL request_id=%s\n%s", request_id, tb)
        return _build_error_response(
            "SYS_INPUT_NORM_FAIL", f"{type(_e).__name__}: {_e}", "stage1_normalize",
            request_id, {}, {"traceback": tb[-500:]},
        )

    # ââ Stage 2: Contrato minimo âââââââââââââââââââââââââââââ
    try:
        contract_err = _validate_min_contract(clean_case)
        if contract_err:
            logger.warning("SYS_INPUT_CONTRACT_FAIL request_id=%s err=%s", request_id, contract_err)
            return _build_error_response(
                "SYS_INPUT_CONTRACT_FAIL", contract_err, "stage2_contract",
                request_id, _summarize_payload(clean_case),
            )
    except Exception as _e:
        tb = traceback.format_exc()
        logger.error("SYS_INPUT_CONTRACT_FAIL request_id=%s\n%s", request_id, tb)
        return _build_error_response(
            "SYS_INPUT_CONTRACT_FAIL", f"{type(_e).__name__}: {_e}", "stage2_contract",
            request_id, _summarize_payload(clean_case), {"traceback": tb[-500:]},
        )

    # ââ SYS001: proc_master_row obrigatorio ââââââââââââââââââ
    if not proc_master_row:
        logger.warning("SYS001 request_id=%s proc_master_row ausente", request_id)
        return {
            "decision_status": "NO_GO",
            "go_class": "NO_GO",
            "confidence_global": 0.0,
            "can_send": False,
            "can_autofill": False,
            "resumo_operacional": "Dado mestre ausente. Motor nao pode operar sem proc_master_row.",
            "bloqueios": [{"codigo": "SYS001", "campo": "proc_master_row",
                           "motivo": "Dado mestre do procedimento nao foi injetado na chamada"}],
            "pendencias": [], "alertas": [], "campos_ok": [],
            "campos_inferidos": [], "autopreenchimentos": [],
            "proxima_acao_sugerida": "Injetar proc_master_row antes de reprocessar o caso",
            "engine_version": ENGINE_VERSION,
            "_error": {"request_id": request_id, "error_code": "SYS001", "stage": "pre_stage3"},
        }

    # ââ Stage 3: Schema mapper âââââââââââââââââââââââââââââââ
    canonical = None
    try:
        canonical = normalize_case(clean_case, proc_master_row, convenio_row, session_user_id)
    except Exception as _e:
        tb = traceback.format_exc()
        logger.error("SYS_SCHEMA_FAIL request_id=%s\n%s", request_id, tb)
        # Fallback: usar clean_case como canonical com campos minimos
        canonical = {
            "CASE_ID": clean_case.get("case_id", ""),
            "PROC_NOME": clean_case.get("procedimento") or clean_case.get("proc_nome", ""),
            "CID_PRINCIPAL": clean_case.get("cid") or clean_case.get("cid_principal", ""),
            "CONVENIO_ID": clean_case.get("convenio") or clean_case.get("convenio_id", ""),
            "STATUS_FONTE": {}, "CONFIANCA": {}, "SOURCE_MAP": {},
            "_schema_fallback": True,
            "_schema_error": f"{type(_e).__name__}: {_e}",
        }
        logger.warning("SYS_SCHEMA_FAIL usando fallback canonical para request_id=%s", request_id)

    # ââ Stage 4: Validator engine ââââââââââââââââââââââââââââ
    validation = None
    try:
        validation = validate_case(canonical, extra_rules)
    except Exception as _e:
        tb = traceback.format_exc()
        logger.error("SYS_VALIDATOR_FAIL request_id=%s\n%s", request_id, tb)
        return _build_error_response(
            "SYS_VALIDATOR_FAIL", f"{type(_e).__name__}: {_e}", "stage4_validator",
            request_id, _summarize_payload(clean_case), {"traceback": tb[-500:]},
        )

    if not isinstance(validation, dict):
        logger.error("SYS_VALIDATOR_BAD_OUTPUT request_id=%s tipo=%s", request_id, type(validation))
        return _build_error_response(
            "SYS_VALIDATOR_BAD_OUTPUT",
            f"validate_case retornou {type(validation).__name__}, esperado dict",
            "stage4_validator", request_id, _summarize_payload(clean_case),
        )

    # ââ Stage 5: Classificar decisao ââââââââââââââââââââââââ
    try:
        result = classify_case(validation)
        # Anexar request_id ao resultado final (nao-blocante)
        result["_request_id"] = request_id
        return result
    except Exception as _e:
        tb = traceback.format_exc()
        logger.error("SYS_CLASSIFY_FAIL request_id=%s\n%s", request_id, tb)
        return _build_error_response(
            "SYS_CLASSIFY_FAIL", f"{type(_e).__name__}: {_e}", "stage5_classify",
            request_id, _summarize_payload(clean_case), {"traceback": tb[-500:]},
        )
