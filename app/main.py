"""
NEUROAUTH v3.0.0 — FastAPI application entry point.

Start command:
  uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""
from __future__ import annotations

import logging
import sys
import os
import traceback

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

# Bootstrap Google Sheets credentials from env var BEFORE any import
# that touches gspread/google-auth.
from app.services.sheets_store import bootstrap_credentials
bootstrap_credentials()

from app.core.config import ALLOWED_ORIGINS
from app.routers import auth, decide

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("neuroauth.app")

# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="NEUROAUTH",
    version="3.0.0",
    description="Motor de decisao para autorizacao cirurgica",
)

# ── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(decide.router)

# ── Legacy Flask routes compatibility ────────────────────────────────────────
# /decision/submit  -> /decide
# /decision/run/X   -> /decide/X
# /decision/config  -> /decide/config
# /motor/test       -> kept as-is below

from motor import run_motor, ENGINE_VERSION


@app.get("/health")
async def health():
    return {"status": "ok", "engine_version": ENGINE_VERSION, "framework": "fastapi"}


@app.post("/motor/test")
async def motor_test(request: Request):
    """POST /motor/test — teste isolado do motor sem persistencia."""
    body = await request.json()
    if not body:
        return JSONResponse({"erro": "payload JSON obrigatorio"}, status_code=400)

    raw_case = body.get("raw_case")
    if raw_case is None:
        return JSONResponse({"erro": "campo 'raw_case' obrigatorio"}, status_code=400)

    result = run_motor(
        raw_case=raw_case,
        proc_master_row=body.get("proc_master_row"),
        convenio_row=body.get("convenio_row"),
        session_user_id=body.get("session_user_id", ""),
    )
    return result


# ── Legacy /decision/* compatibility routes ──────────────────────────────────

@app.post("/decision/submit")
async def legacy_decision_submit(request: Request):
    """Legacy route: redirect to /decide."""
    body = await request.json()
    from app.services.decision_engine import run_decision
    try:
        result, status = run_decision(body)
        return JSONResponse(content=result, status_code=status)
    except Exception as exc:
        logger.exception("legacy /decision/submit: %s", exc)
        return JSONResponse(
            {"decision_status": "ERRO_INTERNO", "erro": str(exc)},
            status_code=500,
        )


@app.post("/decision/run/{episodio_id}")
async def legacy_decision_run(episodio_id: str):
    """Legacy route: redirect to /decide/{episodio_id}."""
    from app.services.decision_engine import run_decision_for_episode
    try:
        result, status = run_decision_for_episode(episodio_id)
        return JSONResponse(content=result, status_code=status)
    except Exception as exc:
        logger.exception("legacy /decision/run/%s: %s", episodio_id, exc)
        return JSONResponse(
            {"decision_status": "ERRO_INTERNO", "erro": str(exc)},
            status_code=500,
        )


@app.get("/decision/config")
async def legacy_decision_config():
    """Legacy route: same as /decide/config."""
    from app.routers.decide import decide_config
    return await decide_config()


# ── Static files: /form ──────────────────────────────────────────────────────
frontend_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.isdir(frontend_dir):
    @app.get("/form")
    async def serve_form():
        from fastapi.responses import FileResponse
        form_path = os.path.join(frontend_dir, "neuroauth_form_v2.html")
        if os.path.isfile(form_path):
            return FileResponse(form_path, media_type="text/html")
        return JSONResponse({"erro": "formulario nao encontrado"}, status_code=404)


# ── Global error handler ────────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception: %s\n%s", exc, traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={
            "decision_status": "ERRO_INTERNO",
            "message": "Erro interno inesperado no servidor.",
            "error_code": "SYS_GLOBAL_ERROR",
        },
    )
