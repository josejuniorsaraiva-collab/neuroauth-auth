"""
app_nri/auth/supabase_jwt.py
Valida JWT Supabase (ES256) via JWKS público.
Usa PyJWT >= 2.8 com PyJWKClient — suporta cache de chaves.
SEGURANÇA: nunca usa service_role nem secret proprietário — chave pública APENAS.
"""
import logging
import os
from functools import lru_cache

from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt as pyjwt
from jwt import PyJWKClient, ExpiredSignatureError, InvalidTokenError

log = logging.getLogger("nri.auth")

JWKS_URL = os.environ.get(
    "SUPABASE_JWKS_URL",
    "https://kkcojqlzmskuznbaxwqy.supabase.co/auth/v1/.well-known/jwks.json",
)

bearer = HTTPBearer()


@lru_cache(maxsize=1)
def _jwks_client() -> PyJWKClient:
    """Cache do cliente JWKS — chaves raramente mudam."""
    return PyJWKClient(JWKS_URL, cache_keys=True)


def get_current_user(
    creds: HTTPAuthorizationCredentials = Security(bearer),
) -> dict:
    """
    FastAPI Dependency: valida Bearer token Supabase (ES256/JWKS).
    Retorna payload completo do JWT.
    Claims relevantes: sub (user_id), email, app_metadata.role
    """
    token = creds.credentials
    try:
        client = _jwks_client()
        signing_key = client.get_signing_key_from_jwt(token)
        payload = pyjwt.decode(
            token,
            signing_key.key,
            algorithms=["ES256"],
            options={"verify_aud": False},
        )
        log.info(
            "JWT válido sub=%s email=%s",
            payload.get("sub", "?")[:8] + "...",
            payload.get("email", "?"),
        )
        return payload

    except ExpiredSignatureError:
        log.warning("JWT Supabase expirado")
        raise HTTPException(status_code=401, detail="token_expired")

    except InvalidTokenError as e:
        log.warning("JWT Supabase inválido: %s", type(e).__name__)
        raise HTTPException(status_code=401, detail="invalid_token")

    except Exception as e:
        log.error("Erro inesperado ao validar JWT: %s", e)
        raise HTTPException(status_code=401, detail="auth_error")
