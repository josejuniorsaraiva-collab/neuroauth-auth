"""
app/routers/make_proxy.py
POST /api/make-proxy — proxy seguro Frontend → Render → Make.com
Nunca expor webhook URL diretamente no frontend.
"""

import logging
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from app.core.config import settings

logger = logging.getLogger("neuroauth.make_proxy")

router = APIRouter()


class MakeProxyRequest(BaseModel):
    webhook_type: str = "profile"
    payload: Dict[str, Any] = {}


class MakeProxyResponse(BaseModel):
    status: str
    make_status_code: int
    detail: str = ""


@router.post("/make-proxy", response_model=MakeProxyResponse)
async def make_proxy(body: MakeProxyRequest):
    webhook_map = {
        "profile": settings.MAKE_WEBHOOK_PROFILE,
        "general": settings.MAKE_WEBHOOK_GENERAL,
    }

    url = webhook_map.get(body.webhook_type, "")
    if not url:
        raise HTTPException(
            status_code=400,
            detail=f"webhook_type '{body.webhook_type}' invalido ou nao configurado.",
        )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=body.payload)

        logger.info("Make proxy [%s] -> %d", body.webhook_type, resp.status_code)

        return MakeProxyResponse(
            status="ok" if resp.status_code == 200 else "error",
            make_status_code=resp.status_code,
            detail=resp.text[:500],
        )

    except httpx.TimeoutException:
        logger.error("Make proxy timeout [%s]", body.webhook_type)
        raise HTTPException(status_code=504, detail="Make.com timeout (30s)")
    except Exception as exc:
        logger.error("Make proxy error [%s]: %s", body.webhook_type, exc)
        raise HTTPException(status_code=502, detail=str(exc))
