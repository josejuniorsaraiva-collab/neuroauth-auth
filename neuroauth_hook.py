"""
neuroauth_hook.py — emissor pós-persistência para neuro_ingest

Plug-and-play. Sem dependências externas (apenas stdlib).
Atômico. Idempotente. Nunca levanta exceção para o NEUROAUTH.

USO em FastAPI:

    from fastapi import BackgroundTasks
    from neuroauth_hook import emit_to_neuro_ingest

    @app.post("/decide")
    async def decide(payload: CasoIn, bg: BackgroundTasks):
        caso = process(payload)         # validação + decisão
        save_to_sheets(caso)            # persistência (fonte de verdade)
        bg.add_task(emit_to_neuro_ingest, caso)   # +1 linha — hook
        return resposta
"""

from __future__ import annotations

import os
import json
import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Any

# Caminho default — sobrescritível por env var sem mexer no código
INBOX = Path(os.environ.get(
    "NEURO_INGEST_INBOX",
    str(Path.home() / "neuro_ingest" / "neuroauth_inbox"),
))

log = logging.getLogger("neuroauth_hook")


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------
def _adapt_neuroauth_backend_payload(d: dict[str, Any]) -> dict[str, Any]:
    """
    Detecta schema do DecideRequest do backend NEUROAUTH e adapta para o
    schema do conversor neuro_ingest. Idempotente: se já estiver no
    formato esperado (tem 'paciente' ou 'caso'), retorna sem alterar.
    """
    if not isinstance(d, dict):
        return d
    # já no formato esperado
    if "paciente" in d or "caso" in d or "clinico" in d:
        return d
    # heurística: backend tem 'cid_principal' OU 'indicacao_clinica' no topo
    if "cid_principal" not in d and "indicacao_clinica" not in d:
        return d   # schema desconhecido — passa adiante sem adaptar

    # extrair eventual bloco de decisão anexado
    decisao = d.get("_decision") or {}
    case_id = (
        decisao.get("decision_run_id")
        or decisao.get("episodio_id")
        or d.get("episodio_id")
        or d.get("trace_id")
    )

    adapted: dict[str, Any] = {
        "caso": {
            "id":           case_id,
            "data":         decisao.get("timestamp") or decisao.get("ts"),
            "profissional": d.get("medico_solicitante"),
            "crm":          d.get("crm"),
        },
        "paciente": {},   # backend não envia dados demográficos -> PENDÊNCIA
        "clinico": {
            "cid":         d.get("cid_principal"),
            "anamnese":    d.get("indicacao_clinica"),
            "exame_fisico": d.get("achados_resumo"),
        },
        "procedimento": {
            "nome":          d.get("procedimento"),
            "codigo_tuss":   d.get("cod_cbhpm"),   # CBHPM mapeado como TUSS
            "justificativa": decisao.get("justificativa"),
            "urgencia":      "rotina",
        },
        "historico": {
            "tratamentos_previos": d.get("tto_conservador"),
        },
        "conduta": {
            "proposta": d.get("procedimento"),
            "alternativas_consideradas": d.get("tto_conservador"),
        },
        "opme": {
            "necessita":      d.get("necessita_opme"),
            "items":          d.get("opme_items"),
            "justificativas": d.get("justificativas_opme"),
        },
        "decisao_motor": {
            "decision_run_id": decisao.get("decision_run_id"),
            "episodio_id":     decisao.get("episodio_id"),
            "classification":  decisao.get("classification"),
            "decision_status": decisao.get("decision_status"),
            "score":           decisao.get("score"),
            "risco_glosa":     decisao.get("risco_glosa"),
            "pendencias":      decisao.get("pendencias"),
            "bloqueios":       decisao.get("bloqueios"),
            "motor_version":   decisao.get("motor_version"),
        },
        "convenio":    d.get("convenio"),
        "observacoes": d.get("indicacao_clinica"),
    }
    # remove chaves None/vazias dentro de cada subdict para não poluir
    cleaned: dict[str, Any] = {}
    for k, v in adapted.items():
        if isinstance(v, dict):
            sub = {kk: vv for kk, vv in v.items() if vv not in (None, "", [], {})}
            if sub:
                cleaned[k] = sub
        elif v not in (None, "", [], {}):
            cleaned[k] = v
    return cleaned


def _safe_id(caso: dict[str, Any]) -> str:
    """ID determinístico para nome de arquivo. Garante idempotência."""
    raw = (
        (caso.get("caso") or {}).get("id")
        or caso.get("id")
        or caso.get("case_id")
        or caso.get("decision_run_id")
        or caso.get("episodio_id")
        or caso.get("trace_id")
    )
    if raw:
        cleaned = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(raw))
        return cleaned[:80] or "caso_sem_id"
    # fallback: hash determinístico do conteúdo
    blob = json.dumps(caso, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return "auto_" + hashlib.sha1(blob).hexdigest()[:12]


def _atomic_write(target: Path, payload: bytes) -> None:
    """Escrita atômica: tmp + rename no mesmo FS."""
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        dir=str(target.parent), prefix=".tmp_neuroauth_", suffix=".json"
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
        os.replace(tmp, target)   # atômico no mesmo FS
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ------------------------------------------------------------------
# API pública
# ------------------------------------------------------------------
def emit_to_neuro_ingest(caso: dict[str, Any]) -> Path | None:
    """
    Drop assíncrono do caso no inbox do neuro_ingest.

    - Idempotente: re-POST do mesmo caso (mesmo id, mesmo conteúdo) é no-op
    - Atômico: writer não vê arquivo parcial
    - Nunca levanta: erros são logados, NEUROAUTH segue intocado
    """
    try:
        if not isinstance(caso, dict):
            log.warning("hook: payload não é dict (tipo=%s) — ignorado", type(caso).__name__)
            return None

        # adapta automaticamente o schema do DecideRequest do backend NEUROAUTH
        caso = _adapt_neuroauth_backend_payload(caso)

        cid = _safe_id(caso)
        target = INBOX / f"{cid}.json"
        payload = json.dumps(caso, ensure_ascii=False, indent=2,
                             sort_keys=True).encode("utf-8")

        # idempotência: se já existe e conteúdo é idêntico, no-op
        if target.exists():
            try:
                if target.read_bytes() == payload:
                    log.info("hook: no-op (mesmo conteúdo) %s", cid)
                    return target
                else:
                    log.info("hook: sobrescrevendo %s (conteúdo diferente)", cid)
            except OSError as e:
                log.warning("hook: leitura falhou para idempotência %s: %s", cid, e)

        _atomic_write(target, payload)
        log.info("hook: emitido %s (%d bytes)", target.name, len(payload))
        return target

    except Exception as e:    # nunca propaga
        log.exception("hook: falha ao emitir caso (NEUROAUTH não afetado): %s", e)
        return None
