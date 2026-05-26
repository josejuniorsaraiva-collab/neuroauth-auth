"""
NEUROAUTH — Rota de Teste Isolada: POST /motor/test
Versão: 2.0.0

Propósito: testar o motor sem banco, sem Sheets, sem persistência.
Recebe proc_master_row diretamente no payload.
Retorna exatamente o output de run_motor() — sem modificação.

Uso: desenvolvimento, diagnóstico, testes de QA.
"""
from __future__ import annotations

import logging
from flask import Blueprint, request, jsonify

from motor import run_motor

logger = logging.getLogger("neuroauth.routes.motor_test")

motor_bp = Blueprint("motor", __name__, url_prefix="/motor")


@motor_bp.post("/test")
def motor_test():
    """
    POST /motor/test

    Entrada esperada:
    {
      "raw_case": {...},
      "proc_master_row": {...},
      "convenio_row": {...},        (opcional)
      "session_user_id": "..."      (opcional)
    }

    Comportamento:
    - chama run_motor() diretamente
    - NÃO acessa banco ou Sheets
    - NÃO persiste nada
    - retorna exatamente o output do motor

    Erros de payload retornam 400 com mensagem clara.
    Nunca retorna 500 silencioso — motor já captura exceções internamente.
    """
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"erro": "payload JSON obrigatorio"}), 400

    raw_case        = body.get("raw_case")
    proc_master_row = body.get("proc_master_row")
    convenio_row    = body.get("convenio_row")
    session_user_id = body.get("session_user_id", "")

    if raw_case is None:
        return jsonify({"erro": "campo 'raw_case' obrigatorio no payload"}), 400

    # proc_master_row ausente → motor retorna SYS001 (não interceptar aqui)
    result = run_motor(
        raw_case=raw_case,
        proc_master_row=proc_master_row,
        convenio_row=convenio_row,
        session_user_id=session_user_id,
    )

    logger.info(
        "/motor/test — status=%s confianca=%.3f",
        result.get("decision_status"),
        result.get("confidence_global", 0.0),
    )

    return jsonify(result), 200
