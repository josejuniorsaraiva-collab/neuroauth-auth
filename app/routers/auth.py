"""
app/routers/auth.py
POST /auth/google — troca id_token Google por JWT NEUROAUTH.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.security import verify_google_token, create_access_token, AUTHORIZED_EMAILS
from app.core.config import settings

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
