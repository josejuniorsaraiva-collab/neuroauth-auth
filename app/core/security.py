"""
app/core/security.py
Gate A: autenticação real server-side.
"""

import hashlib
import logging
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError, JWTClaimsError
from datetime import datetime, timedelta
from app.core.config import settings
import httpx

logger = logging.getLogger("neuroauth.security")
bearer_scheme = HTTPBearer()


def jwt_secret_fingerprint() -> str:
    """First 8 hex chars of SHA256(JWT_SECRET). Safe to log — confirma que issuer e validator usam o mesmo secret em runtime."""
    if not settings.JWT_SECRET:
        return "EMPTY"
    return hashlib.sha256(settings.JWT_SECRET.encode()).hexdigest()[:8]

# Whitelist server-side — fonte de verdade
# Evolução futura: buscar do Sheets (tab MEDICOS_AUTORIZADOS)
AUTHORIZED_EMAILS: set[str] = {
    "josejuniorsaraiva@gmail.com",   # founder — nunca remover
    "josecorreiasaraivajunior@gmail.com",  # dev
    "francelinob571@gmail.com",      # secretária operacional (Bruna)
    "neuroauthautorizacao@gmail.com", # gestora planos de saúde HSA (Karol)
}


def _require_configured():
    """Raise 503 if critical env vars are missing."""
    if not settings.JWT_SECRET:
        raise HTTPException(
            status_code=503,
            detail="JWT_SECRET não configurado. Configure em Render > Environment.",
        )


def create_access_token(email: str, name: str) -> str:
    _require_configured()
    expire = datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    payload = {
        "sub": email,
        "name": name,
        "exp": expire,
        "iat": datetime.utcnow(),
    }
    token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
    logger.info(
        "[JWT_ISSUE] sub=%s fp=%s algo=%s exp_min=%s",
        email, jwt_secret_fingerprint(), settings.JWT_ALGORITHM, settings.JWT_EXPIRE_MINUTES,
    )
    return token


async def verify_google_token(id_token: str) -> dict:
    """Verifica id_token Google via tokeninfo. Retorna {email, name}."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": id_token},
            timeout=10,
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Token Google inválido.")

    data = resp.json()

    # Valida audience — aceita client_id canônico do frontend OU o configurado no env
    token_aud = data.get("aud", "")
    # Client IDs autorizados (fase alpha)
    ALLOWED_AUDIENCES = {  # v3 — whitelist canônica
        "118851719832-qaktum0kj1a6r2a2fp6c75hhag8p2tlf.apps.googleusercontent.com",  # NA_CLIENT_ID do frontend
    }
    # Adicionar o client_id do env se configurado
    if settings.GOOGLE_CLIENT_ID:
        ALLOWED_AUDIENCES.add(settings.GOOGLE_CLIENT_ID)
    if ALLOWED_AUDIENCES and token_aud not in ALLOWED_AUDIENCES:
        raise HTTPException(status_code=401, detail="Audience inválido.")

    email = data.get("email", "").lower()
    if not email:
        raise HTTPException(status_code=401, detail="Email não encontrado no token.")

    return {"email": email, "name": data.get("name", "")}


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    """Dependency: valida JWT NEUROAUTH em cada request protegido.
    Loga o motivo exato do 401 para diagnóstico (assinatura inválida vs expirado vs malformado).
    """
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM]
        )
        email: str = payload.get("sub")
        if not email:
            logger.warning("[JWT_DECODE] no_sub fp=%s", jwt_secret_fingerprint())
            raise HTTPException(status_code=401, detail="Token sem subject.")
        return {"email": email, "name": payload.get("name", "")}
    except ExpiredSignatureError:
        try:
            unv = jwt.get_unverified_claims(token)
        except Exception:
            unv = {}
        logger.warning(
            "[JWT_DECODE] expired fp=%s sub=%s exp=%s iat=%s",
            jwt_secret_fingerprint(), unv.get("sub"), unv.get("exp"), unv.get("iat"),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado. Faça login novamente.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except JWTError as e:
        # Tenta decodificar sem verificar assinatura para logar payload — diagnóstico de divergência de secret.
        try:
            unv = jwt.get_unverified_claims(token)
            logger.warning(
                "[JWT_DECODE] fail reason=%s fp=%s unverified_sub=%s iat=%s exp=%s len=%d",
                f"{type(e).__name__}:{e}", jwt_secret_fingerprint(),
                unv.get("sub"), unv.get("iat"), unv.get("exp"), len(token),
            )
        except Exception as e2:
            logger.warning(
                "[JWT_DECODE] fail reason=%s fp=%s unverified_decode_err=%s len=%d",
                f"{type(e).__name__}:{e}", jwt_secret_fingerprint(), e2, len(token),
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_authorized(user: dict = Depends(get_current_user)) -> dict:
    """Gate binário: email deve estar na whitelist."""
    if user["email"] not in AUTHORIZED_EMAILS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Acesso não autorizado para {user['email']}.",
        )
    return user
