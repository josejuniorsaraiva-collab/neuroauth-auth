import json
from datetime import datetime, timedelta, timezone

import pytest

from publisher.config import Config
from publisher.metadata import MetadataError, build_metadata, load_metadata


@pytest.fixture
def cfg(tmp_path):
    c = Config()
    c.ready_dir = tmp_path
    return c


def test_build_metadata_minimo(cfg):
    meta = build_metadata({"title": "Olá"}, cfg, fallback_title="fb")
    assert meta.title == "Olá"
    assert meta.privacy_status == "private"  # default
    assert meta.category_id == "22"
    assert meta.tags == []


def test_titulo_vazio_usa_fallback(cfg):
    meta = build_metadata({}, cfg, fallback_title="meu video legal")
    assert meta.title == "meu video legal"


def test_titulo_longo_demais(cfg):
    with pytest.raises(MetadataError):
        build_metadata({"title": "x" * 101}, cfg, fallback_title="fb")


def test_tags_string_virgula(cfg):
    meta = build_metadata({"title": "t", "tags": "a, b ,c"}, cfg, fallback_title="fb")
    assert meta.tags == ["a", "b", "c"]


def test_categoria_por_nome(cfg):
    meta = build_metadata(
        {"title": "t", "category": "Science & Technology"}, cfg, fallback_title="fb"
    )
    assert meta.category_id == "28"


def test_categoria_invalida(cfg):
    with pytest.raises(MetadataError):
        build_metadata({"title": "t", "category": "Inexistente"}, cfg, fallback_title="fb")


def test_privacy_invalida(cfg):
    with pytest.raises(MetadataError):
        build_metadata({"title": "t", "privacy": "secreto"}, cfg, fallback_title="fb")


def test_publish_at_no_passado(cfg):
    passado = "2000-01-01T00:00:00Z"
    with pytest.raises(MetadataError):
        build_metadata({"title": "t", "publish_at": passado}, cfg, fallback_title="fb")


def test_publish_at_futuro_normaliza(cfg):
    futuro = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = build_metadata({"title": "t", "publish_at": futuro}, cfg, fallback_title="fb")
    assert meta.publish_at.endswith("Z")
    body = meta.to_youtube_body()
    assert body["status"]["privacyStatus"] == "private"  # agendamento força private
    assert body["status"]["publishAt"] == meta.publish_at


def test_load_metadata_json(cfg, tmp_path):
    video = tmp_path / "clipe.mp4"
    video.write_bytes(b"fake")
    (tmp_path / "clipe.json").write_text(
        json.dumps({"title": "Do JSON", "tags": ["x"]}), encoding="utf-8"
    )
    meta = load_metadata(video, cfg)
    assert meta.title == "Do JSON"
    assert meta.tags == ["x"]
    assert meta.source_path.name == "clipe.json"


def test_load_metadata_sem_sidecar_usa_nome(cfg, tmp_path):
    video = tmp_path / "meu_clipe_legal.mp4"
    video.write_bytes(b"fake")
    meta = load_metadata(video, cfg)
    assert meta.title == "meu clipe legal"
    assert meta.source_path is None
