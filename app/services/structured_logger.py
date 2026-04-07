"""
app/services/structured_logger.py
NEUROAUTH — Logger estruturado central

Regra: todo evento tem timestamp_utc + trace_id + episode_id +
       decision_run_id + event_name + status + details_json.

Eventos obrigatórios no ciclo /decide:
  request_received → decision_started → rules_applied →
  decision_result  → persist_start   → persist_success →
  verify_success   → response_sent   → error_occurred

Uso:
  from app.services.structured_logger import NeuroLog
  log = NeuroLog(trace_id=trace_id, episode_id=ep_id, run_id=run_id)
  log.emit("decision_started", status="ok", details={"engine_version": "v2"})
"""

import logging
import json
from datetime import datetime, timezone
from typing import Optional

_base_logger = logging.getLogger("neuroauth.structured")

VALID_EVENTS = {
    "request_received",
    "decision_started",
    "rules_applied",
    "decision_result",
    "persist_start",
    "persist_success",
    "verify_success",
    "response_sent",
    "error_occurred",
}


class NeuroLog:
    """
    Emite logs estruturados com os 9 campos obrigatórios.
    Instância por requisição — carrega trace_id, episode_id, run_id no contexto.
    """

    def __init__(
        self,
        trace_id: str,
        episode_id: str,
        run_id: Optional[str] = None,
        service_name: str = "neuroauth.decide",
    ):
        self.trace_id    = trace_id
        self.episode_id  = episode_id
        self.run_id      = run_id or ""
        self.service_name = service_name
        self._t0 = datetime.now(timezone.utc)

    def set_run_id(self, run_id: str) -> None:
        """Atualizar run_id após geração pelo motor."""
        self.run_id = run_id

    def emit(
        self,
        event_name: str,
        *,
        status: str = "ok",
        details: Optional[dict] = None,
        latency_ms: Optional[int] = None,
    ) -> None:
        """
        Emite 1 evento estruturado.
        event_name deve ser um dos VALID_EVENTS — se não for, emite mesmo assim
        mas adiciona aviso no campo details.
        """
        if event_name not in VALID_EVENTS:
            _base_logger.warning(
                f"[structured_logger] event_name desconhecido: '{event_name}'. "
                f"Válidos: {sorted(VALID_EVENTS)}"
            )

        now = datetime.now(timezone.utc)
        if latency_ms is None:
            latency_ms = int((now - self._t0).total_seconds() * 1000)

        record = {
            "timestamp_utc":   now.isoformat(),
            "trace_id":        self.trace_id,
            "episode_id":      self.episode_id,
            "decision_run_id": self.run_id,
            "event_name":      event_name,
            "service_name":    self.service_name,
            "status":          status,
            "latency_ms":      latency_ms,
            "details_json":    details or {},
        }

        _base_logger.info(json.dumps(record, ensure_ascii=False))

    def error(
        self,
        failed_stage: str,
        error_type: str,
        error_message: str,
        details: Optional[dict] = None,
    ) -> None:
        """Atalho para error_occurred com campos padronizados."""
        self.emit(
            "error_occurred",
            status="error",
            details={
                "failed_stage":  failed_stage,
                "error_type":    error_type,
                "error_message": error_message[:300],
                **(details or {}),
            },
        )
