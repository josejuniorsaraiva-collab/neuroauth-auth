"""
NEUROAUTH v3.0.0 — Google Sheets store (thin wrapper).

Delega ao repositories/ existente. Centraliza a inicializacao de credenciais
a partir de GOOGLE_SHEETS_CREDS_JSON (env var com JSON inline) para Render.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile

from app.core.config import GOOGLE_SHEETS_CREDS_JSON

logger = logging.getLogger("neuroauth.services.sheets_store")

_creds_bootstrapped = False


def bootstrap_credentials() -> None:
    """
    Se GOOGLE_SHEETS_CREDS_JSON estiver definido (JSON inline), grava em arquivo
    temporario e seta GOOGLE_APPLICATION_CREDENTIALS para que o sheets_client
    existente consiga resolver as credenciais.

    Chamado uma vez no startup da app.
    """
    global _creds_bootstrapped
    if _creds_bootstrapped:
        return

    if GOOGLE_SHEETS_CREDS_JSON:
        try:
            # Valida que e JSON valido
            creds_dict = json.loads(GOOGLE_SHEETS_CREDS_JSON)

            # Grava em /tmp para o sheets_client encontrar
            creds_path = "/tmp/gsheets_creds.json"
            with open(creds_path, "w") as f:
                json.dump(creds_dict, f)

            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
            logger.info(
                "Credenciais Google Sheets escritas em %s (project_id=%s)",
                creds_path,
                creds_dict.get("project_id", "?"),
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Falha ao processar GOOGLE_SHEETS_CREDS_JSON: %s", exc)

    _creds_bootstrapped = True
