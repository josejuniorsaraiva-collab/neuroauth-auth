"""Configuração central do agente, carregada de variáveis de ambiente.

Todos os caminhos são resolvidos em relação à raiz do projeto, de modo que o
agente funciona independentemente do diretório de onde é executado.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Raiz do projeto = pasta que contém este pacote.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _path(env_var: str, default: str) -> Path:
    raw = os.environ.get(env_var, default)
    p = Path(raw)
    return p if p.is_absolute() else (PROJECT_ROOT / p)


@dataclass
class Config:
    # Pastas do fluxo
    ready_dir: Path = field(default_factory=lambda: _path("YPA_READY_DIR", "READY_TO_UPLOAD"))
    uploaded_dir: Path = field(default_factory=lambda: _path("YPA_UPLOADED_DIR", "UPLOADED"))
    failed_dir: Path = field(default_factory=lambda: _path("YPA_FAILED_DIR", "FAILED"))
    log_file: Path = field(default_factory=lambda: _path("YPA_LOG_FILE", "logs/uploads.csv"))

    # Credenciais OAuth do YouTube Data API
    client_secret_file: Path = field(
        default_factory=lambda: _path("YPA_CLIENT_SECRET", "credentials/client_secret.json")
    )
    token_file: Path = field(
        default_factory=lambda: _path("YPA_TOKEN_FILE", "credentials/token.json")
    )

    # Comportamento
    poll_interval: float = float(os.environ.get("YPA_POLL_INTERVAL", "15"))
    # Espera o arquivo parar de crescer antes de subir (evita upload de arquivo
    # ainda sendo escrito/copiado). Em segundos.
    stability_window: float = float(os.environ.get("YPA_STABILITY_WINDOW", "5"))

    # Padrões usados quando o metadado não traz o campo.
    default_privacy: str = os.environ.get("YPA_DEFAULT_PRIVACY", "private")
    default_category_id: str = os.environ.get("YPA_DEFAULT_CATEGORY_ID", "22")  # People & Blogs
    default_made_for_kids: bool = os.environ.get("YPA_DEFAULT_MADE_FOR_KIDS", "false").lower() == "true"

    # Extensões de vídeo aceitas.
    video_extensions: tuple[str, ...] = (".mp4", ".mov", ".mkv", ".avi", ".webm")

    def ensure_dirs(self) -> None:
        for d in (self.ready_dir, self.uploaded_dir, self.failed_dir, self.log_file.parent):
            d.mkdir(parents=True, exist_ok=True)


# Mapa amigável nome -> ID de categoria do YouTube (subconjunto comum).
CATEGORY_NAME_TO_ID = {
    "film & animation": "1",
    "autos & vehicles": "2",
    "music": "10",
    "pets & animals": "15",
    "sports": "17",
    "travel & events": "19",
    "gaming": "20",
    "people & blogs": "22",
    "comedy": "23",
    "entertainment": "24",
    "news & politics": "25",
    "howto & style": "26",
    "education": "27",
    "science & technology": "28",
    "nonprofits & activism": "29",
}
