"""
Testes do POST /relay/neurocase — envio de caso ao Apps Script da biblioteca NEUROCASE.
Sem rede: o Apps Script é simulado. Só o JWT é substituído; a whitelist global e a
permissão específica do NEUROCASE são as reais.
"""
import json
import logging

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import settings
from app.core.security import AUTHORIZED_EMAILS, get_current_user
from app.routers import relay

TOKEN_SERVIDOR = "NC2_token_de_teste_que_nunca_pode_vazar_000000"
URL_SERVIDOR = "https://script.example.invalid/macros/s/TESTE/exec"
AUTORIZADO = sorted(AUTHORIZED_EMAILS)[0]
WHITELIST_SEM_NEUROCASE = sorted(AUTHORIZED_EMAILS)[1]
CASO = {"filename": "2026-10-22_caso-ficticio.md", "content": "# Caso fictício\nidade: 50", "categoria": "TESTE_SEGURANCA"}


class _Resp:
    def __init__(self, data=None, raise_json=False):
        self._data, self._raise = data, raise_json

    def json(self):
        if self._raise:
            raise ValueError("não é JSON")
        return self._data


class _Client:
    """Substitui httpx.AsyncClient; guarda a última chamada."""
    ultimo = {}
    resposta = _Resp({"ok": True, "file_id": "F1", "file_url": "https://drive.example/F1",
                      "filename": CASO["filename"], "categoria": "TESTE_SEGURANCA"})
    erro = None

    def __init__(self, *a, **k):
        _Client.ultimo["init"] = k

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, content=None, headers=None):
        _Client.ultimo.update(url=url, body=json.loads(content), headers=headers)
        if _Client.erro:
            raise _Client.erro
        return _Client.resposta


@pytest.fixture()
def cli(monkeypatch):
    monkeypatch.setattr(settings, "NEUROCASE_ENDPOINT", URL_SERVIDOR)
    monkeypatch.setattr(settings, "NEUROCASE_TOKEN", TOKEN_SERVIDOR)
    monkeypatch.setattr(settings, "NEUROCASE_ALLOWED_EMAILS", AUTORIZADO)
    monkeypatch.setattr(relay.httpx, "AsyncClient", _Client)
    _Client.ultimo, _Client.erro = {}, None
    _Client.resposta = _Resp({"ok": True, "file_id": "F1", "file_url": "https://drive.example/F1",
                              "filename": CASO["filename"], "categoria": "TESTE_SEGURANCA"})
    relay.limiter.enabled = False
    yield TestClient(app)
    relay.limiter.enabled = True
    app.dependency_overrides.clear()


def _como(email):
    app.dependency_overrides[get_current_user] = lambda: {"email": email, "name": "Teste"}


def test_anonimo_recusado(cli):
    r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 401
    assert _Client.ultimo == {}


def test_autenticado_fora_da_whitelist_recusado(cli):
    _como("intruso@exemplo.invalid")
    r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 403
    assert _Client.ultimo == {}


def test_whitelist_sem_permissao_neurocase_recusado(cli):
    _como(WHITELIST_SEM_NEUROCASE)
    r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 403
    assert _Client.ultimo == {}


def test_permissao_vazia_nega_todos(cli, monkeypatch):
    monkeypatch.setattr(settings, "NEUROCASE_ALLOWED_EMAILS", "")
    _como(AUTORIZADO)
    assert cli.post("/relay/neurocase", json=CASO).status_code == 403


def test_nao_configurado_503(cli, monkeypatch):
    monkeypatch.setattr(settings, "NEUROCASE_TOKEN", "")
    _como(AUTORIZADO)
    r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 503 and r.json()["error"] == "NEUROCASE_NAO_CONFIGURADO"


@pytest.mark.parametrize("body", [{}, {"filename": "x.md"}, {"content": "y"}, {"filename": " ", "content": "y"}, [1, 2]])
def test_body_invalido_400(cli, body):
    _como(AUTORIZADO)
    assert cli.post("/relay/neurocase", json=body).status_code == 400


def test_conteudo_acima_de_200kb_413(cli):
    _como(AUTORIZADO)
    r = cli.post("/relay/neurocase", json={**CASO, "content": "x" * 200_001})
    assert r.status_code == 413
    assert _Client.ultimo == {}


def test_envio_ok_e_token_so_no_servidor(cli, caplog):
    _como(AUTORIZADO)
    with caplog.at_level(logging.DEBUG):
        r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True and d["file_id"] == "F1" and d["trace_id"].startswith("NC-")
    assert _Client.ultimo["url"] == URL_SERVIDOR
    assert _Client.ultimo["body"]["token"] == TOKEN_SERVIDOR
    assert _Client.ultimo["body"]["categoria"] == "TESTE_SEGURANCA"
    assert TOKEN_SERVIDOR not in r.text
    assert TOKEN_SERVIDOR not in caplog.text


def test_apps_script_recusa_token_502(cli):
    _Client.resposta = _Resp({"ok": False, "error": "TOKEN_INVALIDO"})
    _como(AUTORIZADO)
    r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 502 and r.json()["error"] == "TOKEN_INVALIDO"


@pytest.mark.parametrize("cenario", ["timeout", "nao_json"])
def test_integracao_indisponivel_502(cli, cenario):
    if cenario == "timeout":
        _Client.erro = relay.httpx.TimeoutException("timeout")
    else:
        _Client.resposta = _Resp(raise_json=True)
    _como(AUTORIZADO)
    r = cli.post("/relay/neurocase", json=CASO)
    assert r.status_code == 502 and r.json()["error"] == "INTEGRACAO_INDISPONIVEL"
    assert TOKEN_SERVIDOR not in r.text


def test_limite_de_requisicoes(cli):
    relay.limiter.enabled = True
    relay.limiter.reset()
    _como(AUTORIZADO)
    codigos = [cli.post("/relay/neurocase", json=CASO).status_code for _ in range(11)]
    assert codigos[:10] == [200] * 10 and codigos[10] == 429
