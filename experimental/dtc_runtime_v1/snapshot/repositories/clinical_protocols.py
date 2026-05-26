"""
NEUROAUTH â RepositÃ³rio: Protocolos ClÃ­nicos (seed local)
VersÃ£o: 1.0.0
Fonte: repositories/data/clinical_protocols_seed_v1.json
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

_BASE_DIR = Path(__file__).resolve().parent
_DATA_PATH = _BASE_DIR / "data" / "clinical_protocols_seed_v1.json"

_CACHE: List[Dict[str, Any]] | None = None


def _load_protocols() -> List[Dict[str, Any]]:
    global _CACHE
    if _CACHE is None:
        with open(_DATA_PATH, "r", encoding="utf-8") as f:
            _CACHE = json.load(f)
    return _CACHE


def list_protocols() -> List[Dict[str, Any]]:
    """Retorna lista simplificada para o frontend (carrega apenas ativos)."""
    data = _load_protocols()
    return [
        {
            "id": p["id"],
            "label": p["label"],
            "domain": p.get("domain", ""),
            "tuss_hint": p.get("tuss_hint", ""),
            "requires_niveis": p.get("rules", {}).get("require_niveis", False),
            "requires_lateralidade": p.get("rules", {}).get("require_lateralidade", False),
            "requires_opme": p.get("rules", {}).get("require_opme", False),
        }
        for p in data
        if p.get("active", True)
    ]


def get_protocol(protocol_id: str) -> Optional[Dict[str, Any]]:
    """Busca protocolo pelo ID (case-insensitive)."""
    data = _load_protocols()
    pid = (protocol_id or "").strip().upper()
    for p in data:
        if p["id"].upper() == pid:
            return p
    return None


def get_tuss_for_protocol(protocol_id: str) -> str:
    """Retorna o TUSS hint para um protocolo. Retorna '' se nÃ£o encontrado."""
    p = get_protocol(protocol_id)
    if p:
        return p.get("tuss_hint", "")
    return ""
