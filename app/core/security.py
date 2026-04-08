"""
app/core/security.py
Gate A: autenticação real server-side.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timedelta
from app.core.config import settings
import httpx

bearer_scheme = HTTPBearer()

# Whitelist server-side — fonte de verdade
# Evolução futura: buscar do Sheets (tab MEDICOS_AUTORIZADOS)
AUTHORIZED_EMAILS: set[str] = {
    "josejuniorsaraiva@gmail.com",   # founder — nunca remover
    # "medico2@email.com",           # adicionar médicos alpha aqui
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
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


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
    """Dependency: valida JWT NEUROAUTH em cada request protegido."""
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM]
        )
        email: str = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Token sem subject.")
        return {"email": email, "name": payload.get("name", "")}
    except JWTError:
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
