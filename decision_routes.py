"""
NEUROAUTH — Rota Real: POST /decision/run/<episodio_id>
Versão: 2.0.0

Responsabilidade: orquestrar o fluxo completo de decisão.
NÃO contém lógica de decisão — delega tudo ao motor.
NÃO acessa Sheets diretamente — delega aos repositórios.

Fluxo obrigatório:
  1. carregar episodio pelo episodio_id
  2. extrair profile_id e convenio_id
  3. buscar proc_master_row via repositório
  4. buscar convenio_row via repositório
  5. montar raw_case
  6. chamar run_motor()
  7. persistir run (save_decision_run)
  8. persistir resultado no episódio (save_decision_result)
  9. retornar output do motor

SYS001 e SYS002 são persistidos como NO_GO — nunca silenciados.
"""
from __future__ import annotations

import logging
from flask import Blueprint, jsonify, request

from motor import run_motor
from repositories import (
    get_episodio,
    get_proc_master_row,
    get_convenio_row,
    save_decision_run,
    save_decision_result,
    update_episodio_status,
)

logger = logging.getLogger("neuroauth.routes.decision")

decision_bp = Blueprint("decision", __name__, url_prefix="/decision")


@decision_bp.post("/run/<episodio_id>")
def decision_run(episodio_id: str):
    """
    POST /decision/run/<episodio_id>

    Executa o motor para um episódio já existente.
    Persiste o resultado — inclusive SYS001 e SYS002.
    Retorna exatamente o output do motor.

    Códigos de resposta:
      200 — motor executou (resultado pode ser GO, NO_GO, etc.)
      404 — episódio não encontrado
      400 — payload mal formado
    """
    # -------------------------------------------------------------------------
    # 1. Carregar episódio
    # -------------------------------------------------------------------------
    episodio = get_episodio(episodio_id)
    if episodio is None:
        return jsonify({"erro": f"episodio '{episodio_id}' nao encontrado"}), 404

    # -------------------------------------------------------------------------
    # 2. Extrair identificadores
    # -------------------------------------------------------------------------
    profile_id  = episodio.get("profile_id", "")
    convenio_id = episodio.get("convenio_id", "")
    session_user_id = episodio.get("usuario_id", "")

    # -------------------------------------------------------------------------
    # 3 e 4. Buscar dados mestres via repositório
    # -------------------------------------------------------------------------
    proc_master_row = get_proc_master_row(profile_id) if profile_id else None
    convenio_row    = get_convenio_row(convenio_id)   if convenio_id else None

    # Logar ausência de dado mestre — motor vai retornar SYS001
    if proc_master_row is None:
        logger.warning(
            "decision_run '%s': proc_master_row ausente para profile_id='%s' — motor retornara SYS001",
            episodio_id, profile_id,
        )

    # -------------------------------------------------------------------------
    # 5. Montar raw_case a partir do episódio
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # 6. Chamar motor — interface única, sem lógica de decisão aqui
    # -------------------------------------------------------------------------
    result = run_motor(
        raw_case=raw_case,
        proc_master_row=proc_master_row,
        convenio_row=convenio_row,
        session_user_id=session_user_id,
    )

    # -------------------------------------------------------------------------
    # 7 e 8. Persistir run e resultado — incluindo SYS001/SYS002
    # -------------------------------------------------------------------------
    run_id = save_decision_run(episodio_id, payload, result)
    result["_run_id"] = run_id  # injetar run_id antes de salvar no episódio

    save_decision_result(episodio_id, result)
    update_episodio_status(episodio_id, run_id, result)

    # -------------------------------------------------------------------------
    # 9. Retornar output do motor
    # -------------------------------------------------------------------------
    logger.info(
        "decision_run '%s': status=%s run_id=%s confianca=%.3f",
        episodio_id,
        result.get("decision_status"),
        run_id,
        result.get("confidence_global", 0.0),
    )

    return jsonify(result), 200
