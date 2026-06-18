"""Leitura e validação dos metadados de um vídeo.

Para um vídeo `meu_video.mp4`, o agente procura um arquivo "sidecar" com os
metadados, na seguinte ordem de preferência:

    meu_video.json   (stdlib, sempre disponível)
    meu_video.yaml / meu_video.yml   (requer PyYAML)
    meu_video.md     (front-matter YAML entre linhas '---', requer PyYAML)

Se nenhum sidecar existir, é usado um conjunto mínimo de padrões, com o título
derivado do nome do arquivo. Assim o sistema nunca trava por falta de metadado,
mas o ideal é sempre fornecer o sidecar.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import CATEGORY_NAME_TO_ID, Config

VALID_PRIVACY = {"public", "private", "unlisted"}


class MetadataError(ValueError):
    """Erro de validação dos metadados do vídeo."""


@dataclass
class VideoMetadata:
    title: str
    description: str = ""
    tags: list[str] = field(default_factory=list)
    category_id: str = "22"
    privacy_status: str = "private"
    publish_at: str | None = None  # ISO 8601 UTC, ex: 2026-07-01T13:00:00Z
    made_for_kids: bool = False
    source_path: Path | None = None  # sidecar de origem (se houver)

    def to_youtube_body(self) -> dict[str, Any]:
        """Monta o corpo da requisição videos.insert."""
        status: dict[str, Any] = {
            "privacyStatus": self.privacy_status,
            "selfDeclaredMadeForKids": self.made_for_kids,
        }
        if self.publish_at:
            # O YouTube só agenda quando o vídeo está como 'private'.
            status["privacyStatus"] = "private"
            status["publishAt"] = self.publish_at
        return {
            "snippet": {
                "title": self.title,
                "description": self.description,
                "tags": self.tags,
                "categoryId": str(self.category_id),
            },
            "status": status,
        }


def _coerce_tags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [t.strip() for t in value.split(",") if t.strip()]
    if isinstance(value, (list, tuple)):
        return [str(t).strip() for t in value if str(t).strip()]
    raise MetadataError(f"'tags' deve ser lista ou string, recebido: {type(value).__name__}")


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "sim", "y"}


def _resolve_category(raw: dict[str, Any], cfg: Config) -> str:
    if "category_id" in raw and raw["category_id"] is not None:
        return str(raw["category_id"])
    if "categoryId" in raw and raw["categoryId"] is not None:
        return str(raw["categoryId"])
    name = raw.get("category")
    if name:
        key = str(name).strip().lower()
        if key in CATEGORY_NAME_TO_ID:
            return CATEGORY_NAME_TO_ID[key]
        raise MetadataError(f"Categoria desconhecida: {name!r}")
    return cfg.default_category_id


def _normalize_publish_at(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    # Aceita 'Z' ou offset; valida que é parseável.
    candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise MetadataError(
            f"'publish_at' inválido: {value!r}. Use ISO 8601, ex: 2026-07-01T13:00:00Z"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if dt <= datetime.now(timezone.utc):
        raise MetadataError(f"'publish_at' está no passado: {value!r}")
    # YouTube espera RFC3339 com 'Z'.
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def find_sidecar(video_path: Path) -> Path | None:
    for ext in (".json", ".yaml", ".yml", ".md"):
        candidate = video_path.with_suffix(ext)
        if candidate.exists():
            return candidate
    return None


def _load_raw(sidecar: Path) -> dict[str, Any]:
    text = sidecar.read_text(encoding="utf-8")
    suffix = sidecar.suffix.lower()
    if suffix == ".json":
        data = json.loads(text)
    elif suffix in {".yaml", ".yml"}:
        data = _load_yaml(text)
    elif suffix == ".md":
        data = _load_yaml(_extract_front_matter(text))
    else:  # pragma: no cover - find_sidecar nunca retorna outra extensão
        raise MetadataError(f"Extensão de metadado não suportada: {suffix}")
    if not isinstance(data, dict):
        raise MetadataError(f"Metadado em {sidecar.name} deve ser um objeto/dicionário")
    return data


def _extract_front_matter(text: str) -> str:
    lines = text.splitlines()
    if lines and lines[0].strip() == "---":
        for i in range(1, len(lines)):
            if lines[i].strip() == "---":
                return "\n".join(lines[1:i])
    raise MetadataError("Arquivo .md sem front-matter YAML delimitado por '---'")


def _load_yaml(text: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise MetadataError(
            "Metadados em YAML/Markdown exigem PyYAML. Instale com 'pip install pyyaml' "
            "ou use um sidecar .json."
        ) from exc
    return yaml.safe_load(text) or {}


def build_metadata(raw: dict[str, Any], cfg: Config, *, fallback_title: str) -> VideoMetadata:
    """Valida um dicionário cru e produz VideoMetadata pronto para upload."""
    title = str(raw.get("title") or fallback_title).strip()
    if not title:
        raise MetadataError("'title' não pode ser vazio")
    if len(title) > 100:
        raise MetadataError(f"'title' excede 100 caracteres (tem {len(title)})")

    privacy = str(raw.get("privacy") or raw.get("privacy_status") or cfg.default_privacy).strip().lower()
    if privacy not in VALID_PRIVACY:
        raise MetadataError(f"'privacy' inválido: {privacy!r}. Use {sorted(VALID_PRIVACY)}")

    return VideoMetadata(
        title=title,
        description=str(raw.get("description") or ""),
        tags=_coerce_tags(raw.get("tags")),
        category_id=_resolve_category(raw, cfg),
        privacy_status=privacy,
        publish_at=_normalize_publish_at(raw.get("publish_at") or raw.get("schedule")),
        made_for_kids=_coerce_bool(raw.get("made_for_kids"), cfg.default_made_for_kids),
    )


def load_metadata(video_path: Path, cfg: Config) -> VideoMetadata:
    """Carrega os metadados de um vídeo a partir do sidecar (ou padrões)."""
    fallback_title = video_path.stem.replace("_", " ").replace("-", " ").strip()
    sidecar = find_sidecar(video_path)
    if sidecar is None:
        meta = build_metadata({}, cfg, fallback_title=fallback_title)
        meta.source_path = None
        return meta
    raw = _load_raw(sidecar)
    meta = build_metadata(raw, cfg, fallback_title=fallback_title)
    meta.source_path = sidecar
    return meta
