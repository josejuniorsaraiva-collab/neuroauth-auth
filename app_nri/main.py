"""
app_nri/main.py
NRI API — Núcleo de Revisão de Internações
Render service: nri-api-production
Start command: uvicorn app_nri.main:app --host 0.0.0.0 --port $PORT

Auth: JWT Supabase ES256 via JWKS público
DB:   Supabase REST via SUPABASE_SERVICE_ROLE_KEY (env only)
"""
import logging
import sys
from pathlib import Path

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from app_nri.routers import health, nri

logger = logging.getLogger("nri.app")

app = FastAPI(
    title="NRI API",
    version="1.0.0",
    description="Núcleo de Revisão de Internações — API interna FOCS",
    docs_url="/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(nri.router, prefix="/nri")


@app.get("/")
def root():
    """Serve a página de auditoria NRI se existir, senão retorna JSON."""
    html_path = Path(__file__).parent.parent / "frontend" / "nri_auditoria.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html; charset=utf-8")
    return JSONResponse({"status": "NRI API online", "docs": "/docs", "health": "/health"})
