"""
NEUROAUTH v3.0.0 — Security: API key validation + Google token verification.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import Header, HTTPException

from .config import NEUROAUTH_API_KEY, GOOGLE_CLIENT_ID

logger = logging.getLogger("neuroauth.security")


# ── API Key dependency ───────────────────────────────────────────────────────

async def verify_api_key(
    authorization: Optional[str] = Header(None),
) -> str:
    """
    FastAPI dependency: valida header Authorization: Bearer <key>.
    Se NEUROAUTH_API_KEY nao esta configurada, aceita tudo (dev mode).
    """
    if not NEUROAUTH_API_KEY:
        return "dev-mode"

    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header ausente")

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Formato: Bearer <api_key>")

    if parts[1] != NEUROAUTH_API_KEY:
        raise HTTPException(status_code=403, detail="API key invalida")

    return parts[1]


# ── Google ID Token verification ─────────────────────────────────────────────

def verify_google_token(id_token: str) -> dict:
    """
    Verifica Google ID token usando google-auth.
    Retorna payload do token (email, name, sub, etc).
    Levanta HTTPException se invalido.
    """
    if not GOOGLE_CLIENT_ID:
        logger.warning("GOOGLE_CLIENT_ID nao configurado — aceitando token sem validacao")
        return {"email": "dev@neuroauth.local", "name": "Dev Mode", "sub": "dev"}

    try:
        from google.oauth2 import id_token as google_id_token
        from google.auth.transport import requests as google_requests

        payload = google_id_token.verify_oauth2_token(
            id_token,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
        return payload

    except ValueError as exc:
        logger.warning("Google token invalido: %s", exc)
        raise HTTPException(status_code=401, detail=f"Google token invalido: {exc}")
