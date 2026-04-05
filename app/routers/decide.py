"""
app/routers/decide.py
POST /decide — endpoint principal. Requer JWT válido.
Make.com chamado em background task — nunca como entrypoint.
"""

from fastapi import APIRouter, Depends, BackgroundTasks
from app.models.decide import DecideRequest, DecideResponse
from app.services.decision_engine import run_decision
from app.services.sheets_store import persist_decision
from app.core.security import require_authorized
from app.core.config import settings
import httpx
import logging

router = APIRouter()
logger = logging.getLogger("neuroauth.decide")


@router.post("", response_model=DecideResponse)
async def decide(
    req: DecideRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(require_authorized),
):
    logger.info(
        f"[decide] user={user['email']} "
        f"episodio={req.episodio_id} "
        f"proc={req.procedimento} "
        f"cid={req.cid_principal}"
    )

    resultado: DecideResponse = run_decision(req)

    background_tasks.add_task(persist_decision, req, resultado)

    if settings.MAKE_DOC_WEBHOOK:
        background_tasks.add_task(
            _dispatch_make_docs,
            req=req,
            res=resultado,
            user_email=user["email"],
        )

    return resultado


async def _dispatch_make_docs(req: DecideRequest, res: DecideResponse, user_email: str):
    """Periférico: falha aqui não afeta a decisão."""
    try:
        payload = {
            "decision_run_id": res.decision_run_id,
            "episodio_id":     res.episodio_id,
            "classification":  res.classification,
            "decision_status": res.decision_status,
            "justificativa":   res.justificativa,
            "procedimento":    req.procedimento,
            "convenio":        req.convenio,
            "cid_principal":   req.cid_principal,
            "user_email":      user_email,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(settings.MAKE_DOC_WEBHOOK, json=payload)
    except Exception as e:
        logger.warning(f"[decide] Make dispatch falhou: {type(e).__name__}: {str(e)[:80]}")
