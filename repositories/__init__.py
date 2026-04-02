"""repositories — camada de persistência do NEUROAUTH v2.0.0."""
from .proc_master_repository import get_proc_master_row
from .convenio_repository import get_convenio_row
from .decision_repository import (
    create_episodio,
    get_episodio,
    save_decision_result,
    save_decision_run,
    update_episodio_status,
    get_decision_run,
)
from .tracker_repository import log_case_result, suggest_gap_candidates
from .calendar_repository import (
    create_or_update_surgery_event,
    cancel_surgery_event,
    get_service_account_email,
)
from .feedback_repository import log_feedback, log_precheck_block
from .insights_repository import refresh_insights_sheet
from .precheck_engine import run_precheck

__all__ = [
    "get_proc_master_row",
    "get_convenio_row",
    "create_episodio",
    "get_episodio",
    "save_decision_result",
    "save_decision_run",
    "update_episodio_status",
    "get_decision_run",
    "log_case_result",
    "suggest_gap_candidates",
    "create_or_update_surgery_event",
    "cancel_surgery_event",
    "get_service_account_email",
    "log_feedback",
    "log_precheck_block",
    "refresh_insights_sheet",
    "run_precheck",
]
