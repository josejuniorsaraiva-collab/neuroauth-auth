"""
NEUROAUTH v3.0.0 — FastAPI application package.

Exports:
  application  — FastAPI ASGI app (para gunicorn -k uvicorn.workers.UvicornWorker)
  app          — alias
"""
from app.main import app

application = app

__all__ = ["app", "application"]
