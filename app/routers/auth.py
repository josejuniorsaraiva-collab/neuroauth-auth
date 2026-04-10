"""
app/routers/auth.py
POST /auth/google — troca id_token Google por JWT NEUROAUTH.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from app.core.security import verify_google_token, create_access_token, AUTHORIZED_EMAILS
from app.core.config import settings
import hashlib

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


@router.get("/test-token")
def test_token(key: str = Query(...)):
    """
    TEMPORÁRIO — gera JWT para founder sem OAuth.
    Protegido por HMAC do JWT_SECRET. Remover após FASE 6.
    """
    if not settings.JWT_SECRET:
        raise HTTPException(status_code=503, detail="JWT_SECRET não configurado.")
    expected = hashlib.sha256(settings.JWT_SECRET.encode()).hexdigest()[:16]
    if key != expected:
        raise HTTPException(status_code=403, detail="Chave inválida.")
    email = "josejuniorsaraiva@gmail.com"
    token = create_access_token(email=email, name="Jose Jr (test)")
    return {"access_token": token, "email": email, "note": "TEMPORARY — remove after FASE 6"}
