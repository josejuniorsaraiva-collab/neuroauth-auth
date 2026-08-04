"""
app_nri/routers/health.py
Health check — retorna JSON com Content-Type: application/json.
Render usa este endpoint para verificar que o serviço está vivo.
"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/health", response_class=JSONResponse)
def health():
    """HTTP 200 + JSON sempre. Sem auth. Sem dependências externas."""
    return {"status": "ok"}
