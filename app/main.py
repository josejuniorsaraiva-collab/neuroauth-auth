"""
app/main.py
Entrypoint FastAPI. Render start command:
  uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import decide, auth
from app.core.config import settings

logger = logging.getLogger("neuroauth.app")

# Startup validation — warn about missing env vars
_REQUIRED = ["JWT_SECRET", "GOOGLE_CLIENT_ID", "GOOGLE_CREDENTIALS_JSON", "SPREADSHEET_ID"]
_missing = [k for k in _REQUIRED if not getattr(settings, k, "")]
if _missing:
    logger.warning(
        "[NEUROAUTH] ENV VARS AUSENTES: %s — /auth e /decide não funcionarão. "
        "Configure em Render > Environment.", ", ".join(_missing)
    )

app = FastAPI(
    title="NEUROAUTH API",
    version="1.0.0",
    docs_url=None,    # desabilitar Swagger em produção
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(auth.router,   prefix="/auth",   tags=["auth"])
app.include_router(decide.router, prefix="/decide", tags=["decide"])


@app.get("/health")   # GET — não POST
def health():
    configured = len(_missing) == 0
    return {
        "status": "ok" if configured else "degraded",
        "version": "1.0.0",
        "configured": configured,
        "missing_env": _missing if not configured else [],
    }
