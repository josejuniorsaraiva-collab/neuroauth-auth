"""
app/main.py
Entrypoint FastAPI. Render start command:
 uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""

import logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from app.routers import decide, auth, make_proxy
from app.core.config import settings

logger = logging.getLogger("neuroauth.app")

# Startup validation — warn about missing env vars
_REQUIRED = ["JWT_SECRET", "GOOGLE_CLIENT_ID", "GOOGLE_APPLICATION_CREDENTIALS", "SPREADSHEET_ID"]
_missing = [k for k in _REQUIRED if not getattr(settings, k, "")]
if _missing:
    logger.warning(
        "[NEUROAUTH] ENV VARS AUSENTES: %s — /auth e /decide nao funcionarao. "
        "Configure em Render > Environment.", ", ".join(_missing)
    )

app = FastAPI(
    title="NEUROAUTH API",
    version="1.0.0",
    docs_url=None,  # desabilitar Swagger em producao
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(decide.router, prefix="/decide", tags=["decide"])
app.include_router(make_proxy.router, prefix="/api", tags=["proxy"])


@app.get("/form")
async def serve_form():
    """Serve o formulário de solicitação cirúrgica v2."""
    form_path = Path(__file__).parent.parent / "frontend" / "neuroauth_form_v2.html"
    return FileResponse(form_path, media_type="text/html; charset=utf-8")


@app.get("/health")  # GET — nao POST
def health():
    configured = len(_missing) == 0
    return {
        "status": "ok" if configured else "degraded",
        "version": "1.0.0",
        "configured": configured,
        "missing_env": _missing if not configured else [],
    }
