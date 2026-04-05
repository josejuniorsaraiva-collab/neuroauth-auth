"""
NEUROAUTH v3.0.0 — Router: /auth

Endpoints:
  POST /auth/google  — valida Google ID token, retorna API key
"""
from __future__ import annotations

import logging

from fastapi import APIRouter

from app.core.config import NEUROAUTH_API_KEY
from app.core.security import verify_google_token
from app.models.decide import AuthGoogleRequest, AuthGoogleResponse

logger = logging.getLogger("neuroauth.routers.auth")

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/google", response_model=AuthGoogleResponse)
async def auth_google(req: AuthGoogleRequest):
    """
    POST /auth/google

    Recebe Google ID token do frontend (sign-in com Google).
    Valida o token com google-auth.
    Retorna email, nome e a API key para chamadas subsequentes.

    Se GOOGLE_CLIENT_ID nao estiver configurado, aceita qualquer token (dev mode).
    """
    payload = verify_google_token(req.id_token)

    logger.info(
        "auth/google: email=%s name=%s",
        payload.get("email", "?"),
        payload.get("name", "?"),
    )

    return AuthGoogleResponse(
        ok=True,
        email=payload.get("email", ""),
        name=payload.get("name", ""),
        sub=payload.get("sub", ""),
        api_key=NEUROAUTH_API_KEY or "dev-key",
    )
