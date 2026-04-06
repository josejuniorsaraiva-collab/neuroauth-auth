"""
app/routers/auth.py
POST /auth/google — troca id_token Google por JWT NEUROAUTH.
POST /auth/dev-token — gera JWT de teste (apenas para founder, remover em produção).
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.security import verify_google_token, create_access_token, AUTHORIZED_EMAILS

router = APIRouter()


class GoogleAuthRequest(BaseModel):
    id_token: str


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    email: str
    name: str
    role: str


@router.post("/google", response_model=AuthResponse)
async def auth_google(body: GoogleAuthRequest):
    user = await verify_google_token(body.id_token)

    if user["email"] not in AUTHORIZED_EMAILS:
        raise HTTPException(
            status_code=403,
            detail="Acesso não liberado para esta fase do NEUROAUTH.",
        )

    token = create_access_token(email=user["email"], name=user["name"])
    role = "founder" if user["email"] == "josejuniorsaraiva@gmail.com" else "medico"

    return AuthResponse(
        access_token=token,
        email=user["email"],
        name=user["name"],
        role=role,
    )


# ── DEV-ONLY: token de teste para validar pipeline /decide ──────────
# TODO: remover antes de ir para produção real
class DevTokenRequest(BaseModel):
    secret_phrase: str


@router.post("/dev-token", response_model=AuthResponse)
async def dev_token(body: DevTokenRequest):
    """
    Gera JWT de teste para o founder.
    Requer secret_phrase = 'neuroauth-fase2-test' como proteção mínima.
    """
    if body.secret_phrase != "neuroauth-fase2-test":
        raise HTTPException(status_code=403, detail="Frase secreta inválida.")

    email = "josejuniorsaraiva@gmail.com"
    name = "Jose Junior (dev-token)"
    token = create_access_token(email=email, name=name)

    return AuthResponse(
        access_token=token,
        email=email,
        name=name,
        role="founder",
    )
