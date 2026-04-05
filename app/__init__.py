"""
NEUROAUTH — FastAPI application package.

Exports:
  application  — FastAPI ASGI app (para gunicorn -k uvicorn.workers.UvicornWorker)
  app          — alias
"""
from .main import app

application = app

__all__ = ["app", "application"]
