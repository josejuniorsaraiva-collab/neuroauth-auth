"""
gunicorn.conf.py — Gunicorn configuration for Render.

Makes `gunicorn app:app` (Render's default Start Command)
work with FastAPI by using Uvicorn ASGI workers.
"""
import os

# Uvicorn worker — bridges gunicorn (WSGI manager) to ASGI app
worker_class = "uvicorn.workers.UvicornWorker"

# Render sets $PORT; default to 10000 (Render's default)
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

# Free tier: single worker is enough
workers = 1

# Timeout — Render kills after 60s anyway
timeout = 120

# Access log to stdout for Render log viewer
accesslog = "-"
errorlog = "-"
loglevel = "info"
