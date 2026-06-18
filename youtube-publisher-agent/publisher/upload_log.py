"""Registro append-only dos uploads em logs/uploads.csv."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

FIELDNAMES = ["timestamp", "filename", "status", "video_id", "url", "error"]


@dataclass
class UploadRecord:
    filename: str
    status: str  # "success" | "failed"
    video_id: str = ""
    url: str = ""
    error: str = ""
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def as_row(self) -> dict[str, str]:
        return {
            "timestamp": self.timestamp,
            "filename": self.filename,
            "status": self.status,
            "video_id": self.video_id,
            "url": self.url,
            "error": self.error,
        }


def append_record(log_file: Path, record: UploadRecord) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_file.exists() or log_file.stat().st_size == 0
    with log_file.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(record.as_row())
