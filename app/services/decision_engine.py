"""
NEUROAUTH v3.0.0 — Decision engine service (thin wrapper).

Delega ao motor/ existente (CHIP 1-4) e aos repositories/ para persistencia.
Nenhuma logica de negocio aqui — apenas orquestracao.
"""
from __future__ import annotations

import json
import logging
import uuid
import threading
from datetime import datetime, timezone
from typing import Any

from motor import run_motor
from repositories import (
    create_episodio,
    get_episodio,
    get_proc_master_row,
    get_convenio_row,
    save_decision_run,
    save_decision_result,
    update_episodio_status,
    log_case_result,
    suggest_gap_candidates,
    create_or_update_surgery_event,
    log_feedback,
    log_precheck_block,
    refresh_insights_sheet,
    run_precheck,
)

logger = logging.getLogger("neuroauth.services.decision_engine")


# ── Post-decision tasks (async, never blocks response) ───────────────────────

def _launch_post_decision_tasks(
    episodio_id: str, run_id: str, case_body: dict, result: dict,
) -> None:
    def _run() -> None:
        tasks = [
            ("log_case_result",        lambda: log_case_result(episodio_id, run_id, case_body, result)),
            ("suggest_gap_candidates", lambda: suggest_gap_candidates(episodio_id, run_id, result)),
            ("log_feedback",           lambda: log_feedback(episodio_id, run_id, case_body, result)),
            ("refresh_insights_sheet", lambda: refresh_insights_sheet()),
        ]
        for task_name, task_fn in tasks:
            try:
                task_fn()
            except Exception as exc:
                logger.error(
                    "POST_DECISION_TASK_FAIL episodio_id=%s task=%s error=%s",
                    episodio_id, task_name, exc,
                )
    threading.Thread(target=_run, daemon=True).start()


# ── Public API ───────────────────────────────────────────────────────────────

def run_decision(body: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """
    Fluxo completo de decisao:
      1. Gerar episodio_id
      2. Criar episodio em 22_EPISODIOS
      3. Precheck
      4. Executar motor
      5. Persistir run + resultado
      6. Post-processing assincrono
      7. Calendar hook (se aplicavel)

    Retorna (result_dict, http_status_code).
    """
    episodio_id = f"EP_{uuid.uuid4().hex[:10].upper()}"
    now = datetime.now(timezone.utc).isoformat()

    profile_id = body.get("profile_id", "")
    convenio_id = body.get("convenio_id", body.get("convenio", ""))

    # 1. Gravar episodio
    episodio_data = {
        "episodio_id":          episodio_id,
        "paciente_id":          body.get("nome_paciente", ""),
        "profile_id":           profile_id,
        "convenio_id":          convenio_id,
        "hospital_id":          body.get("hospital", ""),
        "carater":              body.get("carater_cod", body.get("carater", "")),
        "niveis":               body.get("qtd_niveis", body.get("niveis", "")),
        "cid_principal":        body.get("cid_principal", body.get("cid", "")),
        "cid_secundarios_json": json.dumps(
            [body.get("cid2")] if body.get("cid2") else []
        ),
        "cbo_executor":         body.get("cbo", body.get("cbo_executor", "")),
        "opme_context_json":    json.dumps(
            body.get("opmes_selecionados", body.get("opme_items", []))
        ),
        "clinical_context_json": json.dumps({
            "indicacao_clinica":  body.get("indicacao_clinica", ""),
            "justificativa_opme": body.get("justificativa_opme", ""),
            "procedimento":       body.get("procedimento", ""),
            "medico_solicitante": body.get("medico_solicitante", ""),
            "form_version":       body.get("form_version", ""),
            "source":             body.get("source", ""),
        }),
        "status_operacional":   "NOVO",
        "created_at":           now,
    }
    create_episodio(episodio_data)

    # 2. Raw case
    opmes_payload = (
        body.get("opme_context_json")
        or body.get("opmes_selecionados")
        or body.get("opme_items")
        or []
    )
    raw_case = {**body, "episodio_id": episodio_id, "opme_context_json": opmes_payload}

    # 3. Dados mestres
    proc_master_row = get_proc_master_row(profile_id) if profile_id else None
    convenio_row = get_convenio_row(convenio_id) if convenio_id else None

    if proc_master_row is None:
        logger.warning("decide '%s': proc_master_row ausente para '%s'", episodio_id, profile_id)

    payload_persist = {
        "raw_case":        raw_case,
        "proc_master_row": proc_master_row,
        "convenio_row":    convenio_row,
        "session_user_id": body.get("medico_solicitante", ""),
    }

    # 4. Precheck
    precheck = run_precheck(raw_case, master=proc_master_row)
    if precheck.warnings or precheck.blocking_issues:
        logger.warning(
            "PRECHECK '%s': rigor=%s warnings=%s blocking=%s",
            episodio_id, precheck.rigor_level, precheck.warnings, precheck.blocking_issues,
        )

    _BLOQUEIOS_ATIVOS = {
        "CARATER_AUSENTE",
        "LATERALIDADE_OBRIGATORIA",
        "TUSS_AUSENTE",
        "OPME_OBRIGATORIA_AUSENTE",
    }
    active_blocks = [
        b for b in precheck.blocking_issues
        if any(tag in b for tag in _BLOQUEIOS_ATIVOS)
    ]
    if active_blocks:
        logger.warning("PRECHECK_BLOCKED: episodio_id=%s motivos=%s", episodio_id, active_blocks)
        threading.Thread(
            target=log_precheck_block,
            args=(episodio_id, raw_case, active_blocks),
            daemon=True,
        ).start()
        return {
            "decision_status": "PENDENCIA_OBRIGATORIA",
            "precheck":        precheck.to_dict(),
            "motivos":         active_blocks,
            "mensagem":        "Corrija os campos obrigatorios antes de enviar",
            "can_send":        False,
            "episodio_id":     episodio_id,
        }, 200

    # 5. Motor
    result = run_motor(
        raw_case=raw_case,
        proc_master_row=proc_master_row,
        convenio_row=convenio_row,
        session_user_id=body.get("medico_solicitante", ""),
    )

    # 6. Persistir
    run_id = save_decision_run(episodio_id, payload_persist, result)
    result["_run_id"] = run_id
    result["episodio_id"] = episodio_id

    save_decision_result(episodio_id, result)
    update_episodio_status(episodio_id, run_id, result)

    # 7. Post-processing
    _launch_post_decision_tasks(episodio_id, run_id, body, result)

    # 8. Calendar hook
    _status_ag = body.get("status_agendamento", "")
    _dec_status = result.get("decision_status", "")
    if _status_ag and _dec_status in ("GO", "GO_COM_RESSALVAS"):
        try:
            _proc_nome = result.get("proc_nome", "")
            if not _proc_nome:
                for _ci in result.get("campos_inferidos", []):
                    if _ci.get("campo") == "PROC_NOME":
                        _proc_nome = _ci.get("valor", "")
                        break
            if not _proc_nome:
                _proc_nome = body.get("profile_id", "")
            _regras = (proc_master_row or {}).get("regras_json", {})
            _existing_eid = body.get("google_event_id", "")
            _calendar_id = body.get("google_calendar_id", "primary")
            _episode_ctx = {
                **body,
                "episodio_id":     episodio_id,
                "decision_status": _dec_status,
                "proc_nome":       _proc_nome,
                "_run_id":         result.get("_run_id", run_id),
            }
            create_or_update_surgery_event(
                episodio_id=episodio_id,
                episode=_episode_ctx,
                proc_nome=_proc_nome,
                regras=_regras,
                calendar_id=_calendar_id,
                existing_event_id=_existing_eid,
            )
        except Exception as _cal_exc:
            logger.warning("decide: calendar hook falhou (nao critico) — %s", _cal_exc)

    logger.info(
        "decide '%s': status=%s run_id=%s confianca=%.3f",
        episodio_id, result.get("decision_status"), run_id, result.get("confidence_global", 0.0),
    )

    result["precheck"] = precheck.to_dict()
    return result, 200


def run_decision_for_episode(episodio_id: str) -> tuple[dict[str, Any], int]:
    """
    Executa motor para episodio ja existente em 22_EPISODIOS.
    Retorna (result_dict, http_status_code).
    """
    episodio = get_episodio(episodio_id)
    if episodio is None:
        return {"erro": f"episodio '{episodio_id}' nao encontrado"}, 404

    profile_id = episodio.get("profile_id", "")
    convenio_id = episodio.get("convenio_id", "")
    session_user_id = episodio.get("usuario_id", "")

    proc_master_row = get_proc_master_row(profile_id) if profile_id else None
    convenio_row = get_convenio_row(convenio_id) if convenio_id else None

    raw_case = {k: v for k, v in episodio.items() if k not in (
        "decision_status", "score_confianca", "decision_run_id",
        "sugestao_principal", "alternativas_json", "updated_at",
    )}

    payload = {
        "raw_case":        raw_case,
        "proc_master_row": proc_master_row,
        "convenio_row":    convenio_row,
        "session_user_id": session_user_id,
    }

    result = run_motor(
        raw_case=raw_case,
        proc_master_row=proc_master_row,
        convenio_row=convenio_row,
        session_user_id=session_user_id,
    )

    run_id = save_decision_run(episodio_id, payload, result)
    result["_run_id"] = run_id

    save_decision_result(episodio_id, result)
    update_episodio_status(episodio_id, run_id, result)
    _launch_post_decision_tasks(episodio_id, run_id, raw_case, result)

    logger.info(
        "decision_run '%s': status=%s run_id=%s",
        episodio_id, result.get("decision_status"), run_id,
    )

    return result, 200
