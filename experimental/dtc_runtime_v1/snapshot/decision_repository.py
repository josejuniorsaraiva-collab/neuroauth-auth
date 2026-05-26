"""
NEUROAUTH — Repositório: Decisão
Versão: 2.0.0

Responsabilidade: persistir resultado da decisão e run auditável.
Sem lógica de decisão. Sem acesso ao motor.

Contratos garantidos:
- save_decision_result(): atualiza o episódio com o resultado final
- save_decision_run(): grava o run completo para auditoria
- get_episodio(): carrega dados do episódio para montar o raw_case
- update_episodio_status(): atualiza campos de status no episódio

SYS001 e SYS002 são persistidos como NO_GO — nunca silenciados.
"""
from __future__ import annotations

import uuid
import logging
from datetime import datetime, timezone

logger = logging.getLogger("neuroauth.repo.decision")

# ---------------------------------------------------------------------------
# Storage em memória — substituir por Sheets/banco em produção
# ---------------------------------------------------------------------------
_EPISODIOS: dict[str, dict] = {
    "EP_2024_001": {
        "episodio_id":  "EP_2024_001",
        "profile_id":   "PROF_ACDF_01",
        "convenio_id":  "UNIMED_CARIRI",
        "usuario_id":   "USR_JC_001",
        "cod_tuss":     "40808505",
        "cid_principal":"M50.1",
        "carater":      "eletivo",
        "niveis":       2,
        "opme_json":    {"materiais": [{"codigo": "CAGE_PEEK_01", "qtd": 1}]},
        "decision_status":  None,
        "score_confianca":  None,
        "decision_run_id":  None,
        "updated_at":       None,
    },
    "EP_2024_002": {
        "episodio_id":  "EP_2024_002",
        "profile_id":   "PROF_ACDF_01",
        "convenio_id":  "UNIMED_CARIRI",
        "usuario_id":   "USR_JC_001",
        "cod_tuss":     "40808505",
        # cid_principal ausente — deve gerar PENDENCIA
        "carater":      "eletivo",
        "niveis":       1,
        "opme_json":    {"materiais": [{"codigo": "CAGE_PEEK_01", "qtd": 1}]},
        "decision_status":  None,
        "score_confianca":  None,
        "decision_run_id":  None,
        "updated_at":       None,
    },
}

_DECISION_RUNS: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Interface pública
# ---------------------------------------------------------------------------

def get_episodio(episodio_id: str) -> dict | None:
    """
    Retorna os dados do episódio para montar o raw_case.
    Retorna None se não encontrado.
    """
    episodio = _EPISODIOS.get(episodio_id)
    if episodio is None:
        logger.warning("episodio nao encontrado: '%s'", episodio_id)
    return episodio


def save_decision_result(episodio_id: str, result: dict) -> None:
    """
    Atualiza o episódio com o resultado da decisão.

    Campos atualizados no mínimo:
    - decision_status
    - score_confianca
    - sugestao_principal
    - alternativas_json
    - decision_run_id
    - updated_at

    SYS001 e SYS002 são persistidos como NO_GO — nunca silenciados.
    """
    if episodio_id not in _EPISODIOS:
        logger.error("save_decision_result: episodio '%s' nao encontrado — resultado NAO persistido", episodio_id)
        return

    now = datetime.now(timezone.utc).isoformat()
    ep = _EPISODIOS[episodio_id]

    ep["decision_status"]  = result.get("decision_status", "NO_GO")
    ep["score_confianca"]  = result.get("confidence_global", 0.0)
    ep["sugestao_principal"] = result.get("resumo_operacional", "")
    ep["alternativas_json"]  = {
        "alertas":    result.get("alertas", []),
        "campos_inferidos": result.get("campos_inferidos", []),
    }
    ep["decision_run_id"] = result.get("_run_id")
    ep["updated_at"]      = now

    logger.info(
        "episodio '%s' atualizado: status=%s confianca=%.3f run_id=%s",
        episodio_id,
        ep["decision_status"],
        ep["score_confianca"],
        ep["decision_run_id"],
    )


def save_decision_run(episodio_id: str, payload: dict, result: dict) -> str:
    """
    Grava o run completo para auditoria.

    Campos gravados no mínimo:
    - decision_run_id
    - episodio_id
    - profile_id
    - decision_status
    - confidence_global
    - bloqueios_json
    - pendencias_json
    - alertas_json
    - campos_inferidos_json
    - autopreenchimentos_json
    - engine_version
    - created_at

    Retorna decision_run_id gerado.
    """
    run_id = f"RUN_{uuid.uuid4().hex[:12].upper()}"
    now    = datetime.now(timezone.utc).isoformat()

    run = {
        "decision_run_id":      run_id,
        "episodio_id":          episodio_id,
        "profile_id":           payload.get("raw_case", {}).get("profile_id", ""),
        "decision_status":      result.get("decision_status", "NO_GO"),
        "confidence_global":    result.get("confidence_global", 0.0),
        "bloqueios_json":       result.get("bloqueios", []),
        "pendencias_json":      result.get("pendencias", []),
        "alertas_json":         result.get("alertas", []),
        "campos_inferidos_json": result.get("campos_inferidos", []),
        "autopreenchimentos_json": result.get("autopreenchimentos", []),
        "engine_version":       result.get("engine_version", ""),
        "created_at":           now,
    }

    _DECISION_RUNS[run_id] = run

    logger.info(
        "decision_run gravado: run_id=%s episodio=%s status=%s",
        run_id, episodio_id, run["decision_status"],
    )
    return run_id


def update_episodio_status(episodio_id: str, run_id: str, result: dict) -> None:
    """
    Atualiza campos de status no episódio após persistência do run.
    Chamado pela rota após save_decision_run().
    """
    if episodio_id not in _EPISODIOS:
        return
    _EPISODIOS[episodio_id]["decision_run_id"] = run_id
    _EPISODIOS[episodio_id]["updated_at"] = datetime.now(timezone.utc).isoformat()


def get_decision_run(run_id: str) -> dict | None:
    """Retorna um run pelo ID. Usado em auditoria e testes."""
    return _DECISION_RUNS.get(run_id)
