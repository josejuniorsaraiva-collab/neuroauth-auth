"""
NEUROAUTH v3.0.0 — Router: /decide

Endpoints:
  POST /decide           — cria episodio + executa motor (fluxo principal)
  POST /decide/{ep_id}   — re-executa motor para episodio existente
  GET  /decide/config    — configuracao para o frontend
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request

from app.services.decision_engine import run_decision, run_decision_for_episode

logger = logging.getLogger("neuroauth.routers.decide")

router = APIRouter(tags=["decide"])


@router.post("/decide")
async def decide(request: Request):
    """
    POST /decide

    Fluxo principal: recebe payload do frontend, cria episodio,
    executa motor, persiste resultado.
    Aceita qualquer JSON — campos extras sao passados ao motor.
    """
    body = await request.json()
    if not body:
        return {"erro": "payload JSON obrigatorio"}

    try:
        result, status = run_decision(body)
        return result
    except Exception as exc:
        logger.exception("decide: erro interno — %s", exc)
        return {
            "decision_status": "ERRO_INTERNO",
            "erro": str(exc),
            "erro_tipo": type(exc).__name__,
        }


@router.post("/decide/{episodio_id}")
async def decide_episode(episodio_id: str):
    """
    POST /decide/{episodio_id}

    Re-executa motor para episodio ja existente em 22_EPISODIOS.
    """
    try:
        result, status = run_decision_for_episode(episodio_id)
        if status == 404:
            from fastapi.responses import JSONResponse
            return JSONResponse(content=result, status_code=404)
        return result
    except Exception as exc:
        logger.exception("decide/%s: erro interno — %s", episodio_id, exc)
        return {
            "decision_status": "ERRO_INTERNO",
            "erro": str(exc),
            "erro_tipo": type(exc).__name__,
        }


@router.get("/decide/config")
async def decide_config():
    """
    GET /decide/config

    Configuracao para o frontend: valores validos, perfis com lateralidade, etc.
    """
    try:
        from repositories.precheck_engine import VALID_CARATER_VALUES
        from repositories.proc_master_repository import get_profiles_requiring_opme

        profiles_requiring_laterality = [
            "MICRODISCECTOMIA_LOMBAR",
            "HERNIA_DISCAL_LOMBAR",
            "HERNIA_DISCAL_CERVICAL",
        ]

        return {
            "valid_carater_values": sorted(VALID_CARATER_VALUES),
            "profiles_requiring_laterality": profiles_requiring_laterality,
            "valid_laterality_values": ["DIREITA", "ESQUERDA", "BILATERAL"],
            "profiles_requiring_opme": get_profiles_requiring_opme(),
        }
    except Exception as exc:
        logger.error("decide/config: erro — %s", exc)
        return {"erro": str(exc)}
