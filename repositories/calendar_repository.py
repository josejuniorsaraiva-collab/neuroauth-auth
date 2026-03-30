"""
NEUROAUTH — Google Calendar Repository
Versão: 1.0.0

Responsabilidade: criar, atualizar e cancelar eventos no Google Calendar
a partir de episódios cirúrgicos. Escreve google_event_id de volta em
22_EPISODIOS via sheets_client.

Autenticação: service account com escopo Calendar + Sheets.
Para funcionar, o usuário deve:
  1. Ativar Google Calendar API no Google Cloud Console (mesmo projeto do Sheets)
  2. Compartilhar o calendário alvo com o e-mail da service account (editor)
     e-mail da service account: ver campo "client_email" em neuroauth-auth-*.json

Graceful degradation: qualquer erro é capturado e logado. Nunca interrompe
a resposta ao frontend.

Mapeamento de status de agendamento:
  agendado_preliminar → cria evento (amarelo)
  confirmado          → atualiza evento (azul)
  autorizado          → atualiza evento + enriquece (verde)
  cancelado           → cancela evento (cinza) + atualiza propriedade
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger("neuroauth.calendar")

# ─── Lazy import do Google Calendar API ──────────────────────────────────────

def _get_calendar_service():
    """Retorna service autenticado do Google Calendar. Lança se credenciais inválidas."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    _CREDS_CANDIDATES = [
        os.path.expanduser("~/Downloads/neuroauth-auth-ae97fe63dec3.json"),
        "/etc/secrets/neuroauth-auth-ae97fe63dec3.json",
        os.environ.get("GOOGLE_SHEETS_CREDS_PATH", ""),
    ]

    creds_path = None
    for p in _CREDS_CANDIDATES:
        if p and os.path.isfile(p):
            creds_path = p
            break

    if not creds_path:
        raise FileNotFoundError(
            "Credenciais não encontradas. "
            "Configure o caminho em GOOGLE_SHEETS_CREDS_PATH ou coloque em ~/Downloads/"
        )

    scopes = [
        "https://www.googleapis.com/auth/calendar",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


# ─── Escrita do google_event_id de volta em 22_EPISODIOS ─────────────────────

def _save_event_id_to_episodio(
    episodio_id: str,
    event_id: str,
    calendar_id: str,
    sync_status: str = "OK",
    sync_error: str = "",
) -> None:
    """Grava google_event_id e sync_status de volta em 22_EPISODIOS."""
    try:
        from .sheets_client import get_worksheet, find_row_by_col, get_header_row, update_row_fields
        import datetime

        ws = get_worksheet("22_EPISODIOS")
        headers = get_header_row(ws, head=3)
        row_idx, _ = find_row_by_col(ws, "episodio_id", episodio_id, head=3)
        if row_idx is None:
            logger.warning("_save_event_id: episodio_id='%s' não encontrado em 22_EPISODIOS", episodio_id)
            return

        updates = {
            "google_event_id":       event_id,
            "google_calendar_id":    calendar_id,
            "sync_status":           sync_status,
            "sync_error":            sync_error,
            "ultima_sincronizacao":  datetime.datetime.utcnow().isoformat() + "Z",
        }
        # Filtra apenas campos que existem nos headers (colunas podem não ter sido criadas ainda)
        updates_filtered = {k: v for k, v in updates.items() if k in headers}
        if updates_filtered:
            update_row_fields(ws, row_idx, headers, updates_filtered)
            logger.info("_save_event_id: gravado event_id=%s → %s", event_id, episodio_id)
        else:
            logger.warning(
                "_save_event_id: nenhum dos campos de sync encontrado em 22_EPISODIOS headers. "
                "Adicione as colunas: google_event_id, google_calendar_id, sync_status, sync_error, ultima_sincronizacao"
            )
    except Exception as exc:
        logger.error("_save_event_id: falha ao gravar event_id — %s", exc)


# ─── Interface pública ────────────────────────────────────────────────────────

def create_or_update_surgery_event(
    episodio_id: str,
    episode: dict,
    proc_nome: str,
    regras: dict,
    calendar_id: str = "primary",
    existing_event_id: str = "",
) -> str:
    """
    Cria ou atualiza evento no Google Calendar para um episódio cirúrgico.

    Retorna o google_event_id (novo ou existente).
    Em caso de erro, retorna "" e nunca lança exceção.

    Lógica:
      - existing_event_id vazio → cria novo evento
      - existing_event_id presente → atualiza evento existente
    """
    try:
        from .calendar_event_builder import build_event_payload

        payload = build_event_payload(episode, proc_nome, regras, calendar_id)
        service = _get_calendar_service()

        if existing_event_id:
            # Atualiza evento existente
            result = (
                service.events()
                .update(calendarId=calendar_id, eventId=existing_event_id, body=payload)
                .execute()
            )
            event_id = result.get("id", existing_event_id)
            logger.info(
                "create_or_update_surgery_event: ATUALIZADO event_id=%s episodio=%s",
                event_id, episodio_id,
            )
        else:
            # Cria novo evento
            result = (
                service.events()
                .insert(calendarId=calendar_id, body=payload)
                .execute()
            )
            event_id = result.get("id", "")
            logger.info(
                "create_or_update_surgery_event: CRIADO event_id=%s episodio=%s",
                event_id, episodio_id,
            )

        # Grava event_id de volta no episódio
        if event_id:
            _save_event_id_to_episodio(episodio_id, event_id, calendar_id, "OK", "")

        return event_id

    except Exception as exc:
        logger.error(
            "create_or_update_surgery_event: falha — episodio=%s erro=%s",
            episodio_id, exc,
        )
        _save_event_id_to_episodio(episodio_id, existing_event_id, calendar_id, "ERRO", str(exc)[:300])
        return existing_event_id


def cancel_surgery_event(
    episodio_id: str,
    event_id: str,
    calendar_id: str = "primary",
) -> bool:
    """
    Cancela evento no Google Calendar (move para cinza / status cancelado).
    Atualiza colorId e adiciona [CANCELADO] ao título.
    Retorna True se cancelado com sucesso.
    """
    if not event_id:
        logger.warning("cancel_surgery_event: event_id vazio para episodio=%s", episodio_id)
        return False

    try:
        service = _get_calendar_service()

        # Busca evento atual para preservar título
        existing = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
        title = existing.get("summary", "")
        if "[CANCELADO]" not in title:
            title = title.replace("[CIRURGIA]", "[CIRURGIA][CANCELADO]", 1)

        patch_body = {
            "summary": title,
            "colorId": "8",  # cinza
        }
        service.events().patch(
            calendarId=calendar_id, eventId=event_id, body=patch_body
        ).execute()

        _save_event_id_to_episodio(episodio_id, event_id, calendar_id, "CANCELADO", "")
        logger.info("cancel_surgery_event: cancelado event_id=%s episodio=%s", event_id, episodio_id)
        return True

    except Exception as exc:
        logger.error("cancel_surgery_event: falha — episodio=%s erro=%s", episodio_id, exc)
        return False


def get_service_account_email() -> str:
    """
    Retorna o e-mail da service account (necessário para compartilhar o calendário).
    Útil para setup inicial.
    """
    import json as _json
    candidates = [
        os.path.expanduser("~/Downloads/neuroauth-auth-ae97fe63dec3.json"),
        "/etc/secrets/neuroauth-auth-ae97fe63dec3.json",
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            with open(p) as f:
                data = _json.load(f)
            return data.get("client_email", "não encontrado")
    return "credenciais não encontradas"
