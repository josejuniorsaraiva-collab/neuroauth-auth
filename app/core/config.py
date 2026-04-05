"""
NEUROAUTH v3.0.0 — Configuracao centralizada.

Todas as env vars lidas aqui. Nenhum outro modulo le os.environ diretamente.
"""
from __future__ import annotations

import os


# ── Google Sheets ────────────────────────────────────────────────────────────
SPREADSHEET_ID: str = os.getenv(
    "SPREADSHEET_ID",
    "1tId-AZorbeESHhlvOZei7_UbR0pMj0TMwsH0_lTCGLQ",
)

GOOGLE_SHEETS_CREDS_JSON: str = os.getenv("GOOGLE_SHEETS_CREDS_JSON", "")

# ── API Key ──────────────────────────────────────────────────────────────────
NEUROAUTH_API_KEY: str = os.getenv("NEUROAUTH_API_KEY", "")

# ── Google OAuth (client-side token validation) ──────────────────────────────
GOOGLE_CLIENT_ID: str = os.getenv("GOOGLE_CLIENT_ID", "")

# ── Webhook Make.com (gerador de laudos / documentos) ────────────────────────
MAKE_DOC_WEBHOOK: str = os.getenv("MAKE_DOC_WEBHOOK", "")

# ── Allowed Origins (CORS) ───────────────────────────────────────────────────
ALLOWED_ORIGINS: list[str] = [
    "*",  # TODO: restringir em producao
]
