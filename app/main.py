"""
app/main.py
Entrypoint FastAPI. Render start command:
 uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""

import logging
import sys
from pathlib import Path

# Configurar logging para stdout — necessário para Render capturar INFO/WARNING/ERROR
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from app.routers import decide, auth, make_proxy, metrics, audit, cockpit, hub, relay
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
    docs_url="/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(decide.router, prefix="/decide", tags=["decide"])
app.include_router(make_proxy.router, prefix="/api", tags=["proxy"])
app.include_router(metrics.router, prefix="/metrics", tags=["metrics"])
app.include_router(audit.router, prefix="/audit", tags=["audit"])
app.include_router(cockpit.router, prefix="/cockpit", tags=["cockpit"])
app.include_router(hub.router, prefix="/hub", tags=["hub"])
app.include_router(relay.router, prefix="/relay", tags=["relay"])


@app.get("/form")
async def serve_form():
    """Serve o formulário de solicitação cirúrgica v2."""
    form_path = Path(__file__).parent.parent / "frontend" / "neuroauth_form_v2.html"
    return FileResponse(form_path, media_type="text/html; charset=utf-8")


@app.get("/health")  # GET — nao POST
def health(diag: bool = False):
    from app.services.engine_v3 import ENGINE_VERSION
    configured = len(_missing) == 0
    result = {
        "status": "ok" if configured else "degraded",
        "version": "1.0.0",
        "motor_version": ENGINE_VERSION,
        "configured": configured,
        "missing_env": _missing if not configured else [],
    }
    # ?diag=true → testa conectividade com Sheets (sem JWT)
    if diag and configured:
        try:
            from app.services.sheets_store import _get_client, TAB_DECISION_RUNS
            gc = _get_client()
            ss = gc.open_by_key(settings.SPREADSHEET_ID)
            ws = ss.worksheet(TAB_DECISION_RUNS)
            row_count = len(ws.get_all_values())
            result["sheets"] = {
                "connected": True,
                "tab": TAB_DECISION_RUNS,
                "total_rows": row_count,
            }
        except Exception as e:
            result["sheets"] = {
                "connected": False,
                "error": f"{type(e).__name__}: {str(e)[:200]}",
            }
    return result
