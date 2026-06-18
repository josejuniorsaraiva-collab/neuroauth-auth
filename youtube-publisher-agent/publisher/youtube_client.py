"""Cliente de upload para o YouTube Data API v3.

As bibliotecas do Google são importadas de forma preguiçosa (lazy) dentro dos
métodos, para que o restante do agente — e os testes — funcionem sem elas
instaladas (ex.: em modo --dry-run).
"""
from __future__ import annotations

from pathlib import Path
from typing import Protocol

from .config import Config
from .metadata import VideoMetadata

YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


class Uploader(Protocol):
    """Interface mínima que o Publisher consome."""

    def upload(self, video_path: Path, meta: VideoMetadata) -> str:
        """Sobe o vídeo e retorna o ID do vídeo no YouTube."""
        ...


def video_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


class DryRunUploader:
    """Uploader falso: não chama a rede. Usado para testes e ensaios.

    Gera um ID determinístico a partir do nome do arquivo.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[Path, VideoMetadata]] = []

    def upload(self, video_path: Path, meta: VideoMetadata) -> str:
        self.calls.append((video_path, meta))
        digest = abs(hash(video_path.name)) % (10**11)
        return f"DRYRUN-{digest:011d}"


class YouTubeUploader:
    """Uploader real via YouTube Data API com upload resumível."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._service = None

    # -- autenticação ----------------------------------------------------
    def _credentials(self):
        from google.auth.transport.requests import Request  # type: ignore
        from google.oauth2.credentials import Credentials  # type: ignore
        from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore

        creds = None
        token_file = self.cfg.token_file
        if token_file.exists():
            creds = Credentials.from_authorized_user_file(str(token_file), YOUTUBE_SCOPES)

        if creds and creds.valid:
            return creds

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not self.cfg.client_secret_file.exists():
                raise FileNotFoundError(
                    f"client_secret não encontrado em {self.cfg.client_secret_file}. "
                    "Baixe as credenciais OAuth (Desktop app) do Google Cloud Console."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(self.cfg.client_secret_file), YOUTUBE_SCOPES
            )
            # Em ambiente sem navegador isso imprime uma URL para autorizar.
            creds = flow.run_local_server(port=0)

        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text(creds.to_json(), encoding="utf-8")
        return creds

    def _build_service(self):
        if self._service is None:
            from googleapiclient.discovery import build  # type: ignore

            self._service = build("youtube", "v3", credentials=self._credentials())
        return self._service

    # -- upload ----------------------------------------------------------
    def upload(self, video_path: Path, meta: VideoMetadata) -> str:
        from googleapiclient.errors import HttpError  # type: ignore
        from googleapiclient.http import MediaFileUpload  # type: ignore

        service = self._build_service()
        media = MediaFileUpload(str(video_path), chunksize=-1, resumable=True)
        request = service.videos().insert(
            part="snippet,status",
            body=meta.to_youtube_body(),
            media_body=media,
        )

        response = None
        try:
            while response is None:
                _status, response = request.next_chunk()
        except HttpError as exc:  # pragma: no cover - depende de rede
            raise RuntimeError(f"Falha no upload (HTTP {exc.resp.status}): {exc}") from exc

        video_id = response.get("id")
        if not video_id:
            raise RuntimeError(f"Resposta da API sem 'id': {response}")
        return video_id
