import csv
import json

import pytest

from publisher.config import Config
from publisher.core import Publisher
from publisher.metadata import VideoMetadata
from publisher.youtube_client import DryRunUploader


@pytest.fixture
def cfg(tmp_path):
    c = Config()
    c.ready_dir = tmp_path / "READY_TO_UPLOAD"
    c.uploaded_dir = tmp_path / "UPLOADED"
    c.failed_dir = tmp_path / "FAILED"
    c.log_file = tmp_path / "logs" / "uploads.csv"
    c.stability_window = 0  # sem espera nos testes
    c.ensure_dirs()
    return c


def _make_video(cfg, name="video1.mp4", meta=None):
    path = cfg.ready_dir / name
    path.write_bytes(b"conteudo de video")
    if meta is not None:
        path.with_suffix(".json").write_text(json.dumps(meta), encoding="utf-8")
    return path


def _read_log(cfg):
    with cfg.log_file.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_upload_sucesso_move_e_loga(cfg):
    _make_video(cfg, meta={"title": "Meu vídeo", "privacy": "unlisted"})
    pub = Publisher(cfg, DryRunUploader())

    records = pub.run_once()

    assert len(records) == 1
    assert records[0].status == "success"
    # arquivo saiu de READY e foi para UPLOADED (vídeo + sidecar)
    assert not (cfg.ready_dir / "video1.mp4").exists()
    assert (cfg.uploaded_dir / "video1.mp4").exists()
    assert (cfg.uploaded_dir / "video1.json").exists()
    # log registrado
    rows = _read_log(cfg)
    assert rows[0]["status"] == "success"
    assert rows[0]["url"].startswith("https://www.youtube.com/watch?v=")
    assert rows[0]["filename"] == "video1.mp4"


def test_metadado_invalido_vai_para_failed(cfg):
    # categoria inexistente => MetadataError
    _make_video(cfg, name="ruim.mp4", meta={"title": "x", "category": "Nope"})
    pub = Publisher(cfg, DryRunUploader())

    records = pub.run_once()

    assert records[0].status == "failed"
    assert (cfg.failed_dir / "ruim.mp4").exists()
    assert not (cfg.uploaded_dir / "ruim.mp4").exists()
    assert _read_log(cfg)[0]["status"] == "failed"


def test_falha_no_upload_vai_para_failed(cfg):
    _make_video(cfg, name="boom.mp4", meta={"title": "ok"})

    class ExplodingUploader:
        def upload(self, video_path, meta: VideoMetadata) -> str:
            raise RuntimeError("rede caiu")

    pub = Publisher(cfg, ExplodingUploader())
    records = pub.run_once()

    assert records[0].status == "failed"
    assert "rede caiu" in records[0].error
    assert (cfg.failed_dir / "boom.mp4").exists()


def test_sem_sidecar_usa_padroes_e_sobe(cfg):
    _make_video(cfg, name="sozinho.mp4")  # sem metadado
    pub = Publisher(cfg, DryRunUploader())

    records = pub.run_once()

    assert records[0].status == "success"
    assert (cfg.uploaded_dir / "sozinho.mp4").exists()


def test_nao_sobrescreve_no_destino(cfg):
    # já existe um arquivo com o mesmo nome em UPLOADED
    (cfg.uploaded_dir / "dup.mp4").write_bytes(b"antigo")
    _make_video(cfg, name="dup.mp4", meta={"title": "novo"})
    pub = Publisher(cfg, DryRunUploader())

    pub.run_once()

    assert (cfg.uploaded_dir / "dup.mp4").exists()
    assert (cfg.uploaded_dir / "dup__1.mp4").exists()


def test_ignora_nao_video(cfg):
    (cfg.ready_dir / "notas.txt").write_text("oi", encoding="utf-8")
    pub = Publisher(cfg, DryRunUploader())
    assert pub.run_once() == []
