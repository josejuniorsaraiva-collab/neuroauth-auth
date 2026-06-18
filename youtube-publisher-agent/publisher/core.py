"""Orquestração do agente: detectar, validar, subir, registrar e mover."""
from __future__ import annotations

import logging
import time
from pathlib import Path

from .config import Config
from .metadata import MetadataError, find_sidecar, load_metadata
from .upload_log import UploadRecord, append_record
from .youtube_client import Uploader, video_url

log = logging.getLogger("youtube-publisher-agent")


class Publisher:
    def __init__(self, cfg: Config, uploader: Uploader) -> None:
        self.cfg = cfg
        self.uploader = uploader
        cfg.ensure_dirs()

    # -- descoberta de arquivos -----------------------------------------
    def _is_video(self, path: Path) -> bool:
        return path.is_file() and path.suffix.lower() in self.cfg.video_extensions

    def _is_stable(self, path: Path) -> bool:
        """True se o arquivo não mudou de tamanho na janela de estabilidade.

        Evita subir um arquivo que ainda está sendo escrito/copiado.
        """
        if self.cfg.stability_window <= 0:
            return True
        try:
            first = path.stat().st_size
        except OSError:
            return False
        time.sleep(self.cfg.stability_window)
        try:
            return path.stat().st_size == first
        except OSError:
            return False

    def pending_videos(self) -> list[Path]:
        return sorted(p for p in self.cfg.ready_dir.iterdir() if self._is_video(p))

    # -- movimentação ---------------------------------------------------
    def _move(self, path: Path, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        target = dest_dir / path.name
        # Evita sobrescrever: acrescenta sufixo numérico se já existir.
        counter = 1
        while target.exists():
            target = dest_dir / f"{path.stem}__{counter}{path.suffix}"
            counter += 1
        return path.rename(target)

    def _move_with_sidecar(self, video_path: Path, sidecar: Path | None, dest_dir: Path) -> Path:
        moved = self._move(video_path, dest_dir)
        if sidecar and sidecar.exists():
            try:
                self._move(sidecar, dest_dir)
            except OSError as exc:  # não falha o fluxo por causa do sidecar
                log.warning("Não consegui mover o sidecar %s: %s", sidecar.name, exc)
        return moved

    # -- processamento --------------------------------------------------
    def process_file(self, video_path: Path) -> UploadRecord:
        sidecar = find_sidecar(video_path)
        try:
            meta = load_metadata(video_path, self.cfg)
        except (MetadataError, ValueError) as exc:
            log.error("Metadado inválido para %s: %s", video_path.name, exc)
            record = UploadRecord(filename=video_path.name, status="failed", error=str(exc))
            append_record(self.cfg.log_file, record)
            self._move_with_sidecar(video_path, sidecar, self.cfg.failed_dir)
            return record

        log.info(
            "Subindo %s  (privacidade=%s, agendado=%s)",
            video_path.name,
            meta.privacy_status,
            meta.publish_at or "não",
        )
        try:
            video_id = self.uploader.upload(video_path, meta)
        except Exception as exc:  # noqa: BLE001 - registra qualquer falha de upload
            log.error("Falha no upload de %s: %s", video_path.name, exc)
            record = UploadRecord(filename=video_path.name, status="failed", error=str(exc))
            append_record(self.cfg.log_file, record)
            self._move_with_sidecar(video_path, sidecar, self.cfg.failed_dir)
            return record

        url = video_url(video_id)
        record = UploadRecord(
            filename=video_path.name, status="success", video_id=video_id, url=url
        )
        append_record(self.cfg.log_file, record)
        self._move_with_sidecar(video_path, sidecar, self.cfg.uploaded_dir)
        log.info("OK: %s -> %s", video_path.name, url)
        return record

    def run_once(self) -> list[UploadRecord]:
        records: list[UploadRecord] = []
        for video in self.pending_videos():
            if not self._is_stable(video):
                log.info("Ignorando %s por enquanto (ainda sendo escrito).", video.name)
                continue
            records.append(self.process_file(video))
        return records

    def watch(self) -> None:
        log.info(
            "Monitorando %s (intervalo=%ss). Ctrl+C para parar.",
            self.cfg.ready_dir,
            self.cfg.poll_interval,
        )
        while True:
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001 - loop não pode morrer
                log.exception("Erro inesperado no ciclo de monitoramento: %s", exc)
            time.sleep(self.cfg.poll_interval)
