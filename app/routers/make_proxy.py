"""
app/routers/make_proxy.py
Proxy seguro Frontend -> Render -> Make.com
GET  /api/make-proxy?email=X          -> forward query to Make webhook (profile lookup)
GET  /api/make-proxy?procedimento=X   -> forward query to Make webhook (procedure lookup)
POST /api/make-proxy                  -> forward JSON payload to Make webhook
Nunca expor webhook URL diretamente no frontend.
"""
import logging
import httpx
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional
from app.core.config import settings

logger = logging.getLogger("neuroauth.make_proxy")
router = APIRouter()


def _resolve_webhook(webhook_type: str) -> str:
    webhook_map = {
        "profile": settings.MAKE_WEBHOOK_PROFILE,
        "general": settings.MAKE_WEBHOOK_GENERAL,
    }
    url = webhook_map.get(webhook_type, "")
    if not url:
        raise HTTPException(
            status_code=400,
            detail=f"webhook_type '{webhook_type}' invalido ou nao configurado.",
        )
    return url


# ── GET: profile / procedure lookup ──────────────────────────
@router.get("/make-proxy")
async def make_proxy_get(
    email: Optional[str] = Query(None),
    procedimento: Optional[str] = Query(None),
    webhook_type: str = Query("profile"),
):
    url = _resolve_webhook(webhook_type)
    params: dict = {}
    if email:
        params["email"] = email
    if procedimento:
        params["procedimento"] = procedimento
    if not params:
        raise HTTPException(status_code=400, detail="Informe email ou procedimento.")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)
        logger.info("Make proxy GET [%s] -> %d", webhook_type, resp.status_code)
        # Forward Make.com response as-is so frontend gets profile JSON directly
        return JSONResponse(
            status_code=resp.status_code,
            content=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text[:500]},
        )
    except httpx.TimeoutException:
        logger.error("Make proxy GET timeout [%s]", webhook_type)
        raise HTTPException(status_code=504, detail="Make.com timeout (30s)")
    except Exception as exc:
        logger.error("Make proxy GET error [%s]: %s", webhook_type, exc)
        raise HTTPException(status_code=502, detail=str(exc))


# ── POST: generic payload forward ────────────────────────────
class MakeProxyRequest(BaseModel):
    webhook_type: str = "profile"
    payload: Dict[str, Any] = {}


class MakeProxyResponse(BaseModel):
    status: str
    make_status_code: int
    detail: str = ""


@router.post("/make-proxy", response_model=MakeProxyResponse)
async def make_proxy_post(body: MakeProxyRequest):
    url = _resolve_webhook(body.webhook_type)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=body.payload)
        logger.info("Make proxy POST [%s] -> %d", body.webhook_type, resp.status_code)
        return MakeProxyResponse(
            status="ok" if resp.status_code == 200 else "error",
            make_status_code=resp.status_code,
            detail=resp.text[:500],
        )
    except httpx.TimeoutException:
        logger.error("Make proxy POST timeout [%s]", body.webhook_type)
        raise HTTPException(status_code=504, detail="Make.com timeout (30s)")
    except Exception as exc:
        logger.error("Make proxy POST error [%s]: %s", body.webhook_type, exc)
        raise HTTPException(status_code=502, detail=str(exc))
