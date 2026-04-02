"""
NEUROAUTH — Rotas: POST /decision/run/<episodio_id> + POST /decision/submit
Versão: 2.1.0

Responsabilidade: orquestrar o fluxo completo de decisão.
NÃO contém lógica de decisão — delega tudo ao motor.
NÃO acessa Sheets diretamente — delega aos repositórios.

Rotas:
  POST /decision/run/<episodio_id>   — episódio já existente
  POST /decision/submit              — cria episódio + executa motor (frontend direto)
  OPTIONS /decision/*                — CORS preflight

SYS001 e SYS002 são persistidos como NO_GO — nunca silenciados.
"""
from __future__ import annotations

import json
import logging
import uuid
import threading
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request, make_response

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

logger = logging.getLogger("neuroauth.routes.decision")

# ─── Pós-processamento assíncrono ─────────────────────────────────────────────
def _launch_post_decision_tasks(
    episodio_id: str, run_id: str, case_body: dict, result: dict,
) -> None:
    """Dispara tarefas analíticas em thread daemon — nunca bloqueia a resposta.

    Tarefas: log_case_result, suggest_gap_candidates, log_feedback,
    refresh_insights_sheet.
    Falhas capturadas e logadas como POST_DECISION_TASK_FAIL — nunca propagadas.
    """
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
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "POST_DECISION_TASK_FAIL episodio_id=%s task=%s error=%s",
                    episodio_id, task_name, exc,
                )

    threading.Thread(target=_run, daemon=True).start()


decision_bp = Blueprint("decision", __name__, url_prefix="/decision")


# ─── CORS helper ───────────────────────────────────────────────────────────────────────────────

def _cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return response


@decision_bp.route("/<path:dummy>", methods=["OPTIONS"])
def options_handler(dummy=""):
    """CORS preflight handler para todas as sub-rotas de /decision/."""
    return _cors(make_response("", 204))


# ─── GET /decision/config ──────────────────────────────────────────────────────────────────

@decision_bp.get("/config")
def decision_config():
    """
    GET /decision/config

    Fonte única de verdade para o frontend:
      - valid_carater_values: lista ordenada de valores aceitos pelo precheck
      - profiles_requiring_laterality: perfis que exigem campo lateralidade
      - valid_laterality_values: opções de lado aceitas

    CORS habilitado — consumido diretamente pelo browser.
    Sem autenticação: dados de configuração, não sensíveis.
    """
    from repositories.precheck_engine import (
        VALID_CARATER_VALUES,
        _profile_requires_laterality,
    )

    # Perfis com lateralidade obrigatória — espelha _profile_requires_laterality()
    profiles_requiring_laterality = [
        "MICRODISCECTOMIA_LOMBAR",
        "HERNIA_DISCAL_LOMBAR",
        "HERNIA_DISCAL_CERVICAL",
    ]

    return _cors(jsonify({
        "valid_carater_values":          sorted(VALID_CARATER_VALUES),
        "profiles_requiring_laterality": profiles_requiring_laterality,
        "valid_laterality_values":       ["DIREITA", "ESQUERDA", "BILATERAL"],
    }))


# ─── POST /decision/run/<episodio_id> ─────────────────────────────────────────────────────

@decision_bp.post("/run/<episodio_id>")
def decision_run(episodio_id: str):
    """
    POST /decision/run/<episodio_id>

    Executa o motor para um episódio já existente em 22_EPISODIOS.
    Persiste o resultado — inclusive SYS001 e SYS002.
    Retorna exatamente o output do motor.

    Códigos de resposta:
      200 — motor executou (resultado pode ser GO, NO_GO, etc.)
      404 — episódio não encontrado
    """
    # 1. Carregar episódio
    episodio = get_episodio(episodio_id)
    if episodio is None:
        return _cors(jsonify({"erro": f"episodio '{episodio_id}' nao encontrado"})), 404

    # 2. Extrair identificadores
    profile_id      = episodio.get("profile_id", "")
    convenio_id     = episodio.get("convenio_id", "")
    session_user_id = episodio.get("usuario_id", "")

    # 3-4. Buscar dados mestres
    proc_master_row = get_proc_master_row(profile_id) if profile_id else None
    convenio_row    = get_convenio_row(convenio_id)   if convenio_id else None

    if proc_master_row is None:
        logger.warning(
            "decision_run '%s': proc_master_row ausente para profile_id='%s' — motor retornara SYS001",
            episodio_id, profile_id,
        )

    # 5. Montar raw_case
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

    # 6. Chamar motor
    result = run_motor(
        raw_case=raw_case,
        proc_master_row=proc_master_row,
        convenio_row=convenio_row,
        session_user_id=session_user_id,
    )

    # 7-8. Persistir
    run_id = save_decision_run(episodio_id, payload, result)
    result["_run_id"] = run_id

    save_decision_result(episodio_id, result)
    update_episodio_status(episodio_id, run_id, result)

    # 9. Tracker pós-decisão (nunca interrompe a resposta ao frontend)
    # 9. Pós-processamento assíncrono (feedback + insights — não bloqueia resposta)
    _launch_post_decision_tasks(episodio_id, run_id, raw_case, result)

    logger.info(
        "decision_run '%s': status=%s run_id=%s confianca=%.3f",
        episodio_id,
        result.get("decision_status"),
        run_id,
        result.get("confidence_global", 0.0),
    )

    return _cors(jsonify(result)), 200


# ─── POST /decision/submit ────────────────────────────────────────────────────────────────────────────

@decision_bp.post("/submit")
def decision_submit():
    """
    POST /decision/submit

    Fluxo direto do frontend:
      1. Recebe payload do formulário
      2. Cria episódio em 22_EPISODIOS
      3. Executa o motor completo
      4. Persiste run em 21_DECISION_RUNS
      5. Atualiza 22_EPISODIOS com resultado
      6. Retorna resultado + episodio_id + run_id

    Aceita o payload exatamente como enviado pelo frontend (collect() + profile_id).
    O ALIAS_MAP do motor normaliza os campos automaticamente.

    CORS habilitado para uso direto do browser.
    """
    body = request.get_json(silent=True)
    if not body:
        return _cors(jsonify({"erro": "payload JSON obrigatorio"})), 400

    try:
        # ── 1. Gerar episodio_id único ─────────────────────────────────────────────────────────────────────
        episodio_id = f"EP_{uuid.uuid4().hex[:10].upper()}"
        now = datetime.now(timezone.utc).isoformat()

        # ── 2. Extrair campos de controle ─────────────────────────────────────────────────────────────────
        profile_id  = body.get("profile_id", "")
        convenio_id = body.get("convenio_id", body.get("convenio", ""))

        # ── 3. Gravar episódio em 22_EPISODIOS ────────────────────────────────────────────────────────
        episodio_data = {
            "episodio_id":         episodio_id,
            "paciente_id":         body.get("nome_paciente", ""),
            "profile_id":          profile_id,
            "convenio_id":         convenio_id,
            "hospital_id":         body.get("hospital", ""),
            "carater":             body.get("carater_cod", body.get("carater", "")),
            "niveis":              body.get("qtd_niveis", body.get("niveis", "")),
            "cid_principal":       body.get("cid_principal", body.get("cid", "")),
            "cid_secundarios_json": json.dumps(
                [body.get("cid2")] if body.get("cid2") else []
            ),
            "cbo_executor":        body.get("cbo", body.get("cbo_executor", "")),
            "opme_context_json":   json.dumps(
                body.get("opmes_selecionados", body.get("opme_items", []))
            ),
            "clinical_context_json": json.dumps({
                "indicacao_clinica":   body.get("indicacao_clinica", ""),
                "justificativa_opme":  body.get("justificativa_opme", ""),
                "procedimento":        body.get("procedimento", ""),
                "medico_solicitante":  body.get("medico_solicitante", ""),
            }),
            "status_operacional":  "NOVO",
            "created_at":          now,
        }

        create_episodio(episodio_data)

        # ── 4. Montar raw_case (payload completo + episodio_id) ───────────────────────────────────────
        # O ALIAS_MAP do motor normaliza automaticamente convenio→CONVENIO_ID,
        # cod_tuss→COD_TUSS, cid_principal→CID_PRINCIPAL, profile_id→PROFILE_ID, etc.
        #
        # Garantia de OPME_JSON: o frontend pode enviar OPME via 'opmes_selecionados'
        # (campo nomeado pelo formulário) ou via 'opme_context_json' (campo canônico do
        # ALIAS_MAP). Aqui normalizamos para 'opme_context_json' antes de passar ao motor,
        # preservando qualquer valor já presente no body.
        opmes_payload = (
            body.get("opme_context_json")
            or body.get("opmes_selecionados")
            or body.get("opme_items")
            or []
        )
        raw_case = {**body, "episodio_id": episodio_id, "opme_context_json": opmes_payload}

        # ── 5. Buscar dados mestres ───────────────────────────────────────────────────────────────────
        proc_master_row = get_proc_master_row(profile_id) if profile_id else None
        convenio_row    = get_convenio_row(convenio_id)   if convenio_id else None

        if proc_master_row is None:
            logger.warning(
                "decision_submit '%s': proc_master_row ausente para profile_id='%s' — SYS001",
                episodio_id, profile_id,
            )

        payload_persist = {
            "raw_case":        raw_case,
            "proc_master_row": proc_master_row,
            "convenio_row":    convenio_row,
            "session_user_id": body.get("medico_solicitante", ""),
        }

        # ── 6. Bloco 3 — Precheck (bloqueio: CARATER_AUSENTE, LATERALIDADE_OBRIGATORIA, TUSS_AUSENTE) ──
        # Etapa 3: passa proc_master_row para habilitar Regra 7 (OPME_OBRIGATORIA_AUSENTE)
        # master é Optional — se None, Regra 7 fica dormente sem efeito
        precheck = run_precheck(raw_case, master=proc_master_row)
        if precheck.warnings or precheck.blocking_issues:
            logger.warning(
                "PRECHECK '%s': rigor=%s warnings=%s blocking=%s",
                episodio_id,
                precheck.rigor_level,
                precheck.warnings,
                precheck.blocking_issues,
            )

        # Bloqueio parcial — regras validadas em shadow com FP=0
        # TUSS_AUSENTE:             shadow 5/5 RGL005 antecipados, FP=0
        # OPME_OBRIGATORIA_AUSENTE: shadow 3/4 RGL040 antecipados, FP=0
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
            logger.warning(
                "PRECHECK_BLOCKED: episodio_id=%s motivos=%s",
                episodio_id, active_blocks,
            )
            # FASE 4 — log persistente no 23_FEEDBACK_LOOP (thread daemon)
            import threading as _threading
            _threading.Thread(
                target=log_precheck_block,
                args=(episodio_id, raw_case, active_blocks),
                daemon=True,
            ).start()
            return _cors(jsonify({
                "decision_status": "PENDENCIA_OBRIGATORIA",
                "precheck": precheck.to_dict(),
                "motivos": active_blocks,
                "mensagem": "Corrija os campos obrigatórios antes de enviar",
                "can_send": False,
            })), 200
        # Demais blocking_issues permanecem em shadow mode (só log, não bloqueiam)
        # Para ativar bloqueio total: substituir active_blocks por precheck.blocking_issues

        # ── 7. Executar motor ───────────────────────────────────────────────────────────────────────────────
        result = run_motor(
            raw_case=raw_case,
            proc_master_row=proc_master_row,
            convenio_row=convenio_row,
            session_user_id=body.get("medico_solicitante", ""),
        )

        # ── 7. Persistir run + atualizar episódio ───────────────────────────────────────────────
        run_id = save_decision_run(episodio_id, payload_persist, result)
        result["_run_id"]      = run_id
        result["episodio_id"]  = episodio_id

        save_decision_result(episodio_id, result)
        update_episodio_status(episodio_id, run_id, result)

        # ── 8. Tracker pós-decisão (nunca interrompe a resposta ao frontend) ──
        # ── 8. Pós-processamento assíncrono (feedback + insights — não bloqueia resposta) ──
        _launch_post_decision_tasks(episodio_id, run_id, body, result)

        # ── 9. Google Calendar — cria/atualiza evento se agendado ────────────────────────────────
        # Só dispara se status_agendamento estiver definido E decisão for GO/GO_COM_RESSALVAS
        _status_ag = body.get("status_agendamento", "")
        _dec_status = result.get("decision_status", "")
        if _status_ag and _dec_status in ("GO", "GO_COM_RESSALVAS"):
            try:
                # Extrai proc_nome: tenta resultado do motor, depois campos_inferidos, fallback profile_id
                _proc_nome = result.get("proc_nome", "")
                if not _proc_nome:
                    for _ci in result.get("campos_inferidos", []):
                        if _ci.get("campo") == "PROC_NOME":
                            _proc_nome = _ci.get("valor", "")
                            break
                if not _proc_nome:
                    _proc_nome = body.get("profile_id", "")
                _regras      = (proc_master_row or {}).get("regras_json", {})
                _existing_eid = body.get("google_event_id", "")
                _calendar_id  = body.get("google_calendar_id", "primary")
                _episode_ctx  = {
                    **body,
                    "episodio_id":     episodio_id,
                    "decision_status": _dec_status,
                    "proc_nome":       _proc_nome,
                    "_run_id":         result.get("_run_id", run_id),
                }
                create_or_update_surgery_event(
                    episodio_id   = episodio_id,
                    episode       = _episode_ctx,
                    proc_nome     = _proc_nome,
                    regras        = _regras,
                    calendar_id   = _calendar_id,
                    existing_event_id = _existing_eid,
                )
            except Exception as _cal_exc:
                logger.warning("decision_submit: calendar hook falhou (não crítico) — %s", _cal_exc)

        logger.info(
            "decision_submit '%s': status=%s run_id=%s confianca=%.3f",
            episodio_id,
            result.get("decision_status"),
            run_id,
            result.get("confidence_global", 0.0),
        )

        # Bloco 3 — expõe precheck no payload de resposta (shadow mode)
        result["precheck"] = precheck.to_dict()

        return _cors(jsonify(result)), 200

    except Exception as exc:  # noqa: BLE001
        logger.exception("decision_submit: erro interno — %s", exc)
        err_body = {
            "decision_status": "ERRO_INTERNO",
            "erro":            str(exc),
            "erro_tipo":       type(exc).__name__,
        }
        return _cors(jsonify(err_body)), 500
