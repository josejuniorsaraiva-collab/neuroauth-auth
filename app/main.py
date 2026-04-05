"""
app/main.py
Entrypoint FastAPI. Render start command:
  uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import decide, auth
from app.core.config import settings

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
    return {"status": "ok", "version": "1.0.0"}
