"""
app/routers/relay.py
Relay endpoints — frontend-facing API that proxies to Make.com webhooks.

Routes:
  GET  /relay/profile?email=X          -> profile lookup via Make webhook
  GET  /relay/profile?procedimento=X   -> procedure lookup via Make webhook
  POST /relay/notify                   -> forward submission payload to Make webhook

These replace the old /api/make-proxy routes with cleaner frontend URLs.
"""
import logging
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional
from app.core.config import settings

logger = logging.getLogger("neuroauth.relay")
router = APIRouter()


def _get_webhook_url(webhook_type: str) -> str:
    """Resolve webhook URL from settings."""
    webhook_map = {
        "profile": settings.MAKE_WEBHOOK_PROFILE,
        "general": settings.MAKE_WEBHOOK_GENERAL,
    }
    url = webhook_map.get(webhook_type, "")
    if not url:
        raise HTTPException(
            status_code=503,
            detail=f"Webhook '{webhook_type}' não configurado no backend.",
        )
    return url


# ── GET /relay/profile ──────────────────────────────────────
@router.get("/profile")
async def relay_profile(
    email: Optional[str] = Query(None),
    procedimento: Optional[str] = Query(None),
):
    """
    Busca perfil do médico ou dados do procedimento via Make.com.
    Inclui fallback alpha para emails autorizados sem perfil no Sheets.
    """
    url = _get_webhook_url("profile")
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
        logger.info("Relay profile GET -> %d", resp.status_code)

        # Fallback alpha: emails autorizados sem perfil no Sheets
        if email:
            from app.core.security import AUTHORIZED_EMAILS
            if email.lower() in AUTHORIZED_EMAILS:
                has_valid_profile = False
                if resp.headers.get("content-type", "").startswith("application/json"):
                    try:
                        body_json = resp.json()
                        has_valid_profile = bool(body_json.get("user_email"))
                    except Exception:
                        has_valid_profile = False

                if not has_valid_profile:
                    logger.info("Relay: fallback alpha para %s", email)
                    fallback_perfil = {
                        "user_email":            email,
                        "medico_nome":           email.split("@")[0],
                        "perfil_tipo":           "medico",
                        "ativo":                 True,
                        "hospital_padrao":       "HSA Barbalha",
                        "convenios_habilitados": "Unimed Cariri",
                        "crm":                   "",
                        "cbo":                   "225120",
                    }
                    return JSONResponse(status_code=200, content=fallback_perfil)

        return JSONResponse(
            status_code=resp.status_code,
            content=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text[:500]},
        )
    except httpx.TimeoutException:
        logger.error("Relay profile timeout")
        raise HTTPException(status_code=504, detail="Make.com timeout (30s)")
    except Exception as exc:
        logger.error("Relay profile error: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc))


# ── POST /relay/notify ──────────────────────────────────────
@router.post("/notify")
async def relay_notify(request: Request):
    """
    Encaminha payload de submissão para Make.com webhook (general).
    Aceita qualquer JSON body do frontend.
    """
    url = _get_webhook_url("general")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body JSON inválido.")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=body)
        logger.info("Relay notify POST -> %d", resp.status_code)

        return JSONResponse(
            status_code=resp.status_code,
            content={
                "status": "ok" if resp.status_code == 200 else "error",
                "make_status_code": resp.status_code,
                "detail": resp.text[:500],
            },
        )
    except httpx.TimeoutException:
        logger.error("Relay notify timeout")
        raise HTTPException(status_code=504, detail="Make.com timeout (30s)")
    except Exception as exc:
        logger.error("Relay notify error: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc))
