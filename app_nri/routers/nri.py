"""
app_nri/routers/nri.py
Endpoints NRI — fila de internações pendentes de revisão.

SEGURANÇA:
  - JWT Supabase (ES256) exigido em todos os endpoints
  - SUPABASE_SERVICE_ROLE_KEY lida de env var — NUNCA hardcoded
  - service_role bypass RLS para leitura da tabela pelo backend
  - JWT do usuário NÃO é logado
"""
import logging
import os

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from app_nri.auth.supabase_jwt import get_current_user

log = logging.getLogger("nri.router")

router = APIRouter()

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://kkcojqlzmskuznbaxwqy.supabase.co",
)
NRI_TABLE = os.environ.get("NRI_TABLE", "internacoes")


def _service_headers() -> dict:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not key:
        log.error("SUPABASE_SERVICE_ROLE_KEY ausente")
        raise HTTPException(
            status_code=503,
            detail="Serviço não configurado. Contate o administrador.",
        )
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


@router.get("/", response_class=JSONResponse)
def nri_status(user: dict = Depends(get_current_user)):
    """Verifica identidade e role do usuário NRI autenticado."""
    return {
        "ok": True,
        "user_id": user.get("sub"),
        "email": user.get("email"),
        "role": (user.get("app_metadata") or {}).get("role"),
    }


@router.get("/fila", response_class=JSONResponse)
def get_fila(
    status: str = Query(default="pendente", description="Status das internações"),
    limit: int = Query(default=100, ge=1, le=500),
    user: dict = Depends(get_current_user),
):
    """
    Retorna internações pendentes de revisão NRI.
    Requer JWT Supabase válido (nri_viewer ou nri_reviewer).
    """
    headers = _service_headers()
    url = f"{SUPABASE_URL}/rest/v1/{NRI_TABLE}"
    params = {
        "status": f"eq.{status}",
        "select": "*",
        "order": "created_at.asc",
        "limit": str(limit),
    }

    try:
        resp = httpx.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        log.info(
            "fila user=%s status=%s total=%d",
            user.get("email", "?"),
            status,
            len(data),
        )
        return {"fila": data, "total": len(data), "status_filtro": status}

    except httpx.HTTPStatusError as e:
        log.error(
            "Supabase REST %s: %s",
            e.response.status_code,
            e.response.text[:300],
        )
        raise HTTPException(
            status_code=502,
            detail=f"supabase_error: {e.response.status_code}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="supabase_timeout")
    except Exception as e:
        log.error("get_fila error: %s", type(e).__name__)
        raise HTTPException(status_code=503, detail="internal_error")
