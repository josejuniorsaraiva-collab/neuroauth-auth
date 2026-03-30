"""
NEUROAUTH — Calendar Event Builder
Versão: 1.0.0

Monta o payload de evento Google Calendar a partir de um episódio cirúrgico.
Nunca decide. Nunca acessa Sheets. Apenas formata.

Esquema de cores (colorId do Google Calendar):
  5 = amarelo  → agendado_preliminar
  9 = azul     → confirmado
  2 = verde    → autorizado
  6 = vermelho → pendencia_critica
 8  = cinza    → cancelado
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

# ─── Mapa de cores por status ─────────────────────────────────────────────────

_COLOR_MAP: dict[str, int] = {
    "agendado_preliminar": 5,   # amarelo
    "confirmado":          9,   # azul
    "autorizado":          2,   # verde
    "pendencia_critica":   6,   # vermelho
    "cancelado":           8,   # cinza
}

_DEFAULT_COLOR = 5  # amarelo para status desconhecido


# ─── Título ───────────────────────────────────────────────────────────────────

def build_title(
    status_agendamento: str,
    proc_nome: str,
    paciente_abrev: str,
    convenio_id: str,
) -> str:
    """
    Exemplo: [CIRURGIA][CONFIRMADO] Artrodese Cervical — M.S. — Unimed
    """
    status_label = status_agendamento.upper().replace("_", " ")
    convenio_short = convenio_id.split("_")[0].capitalize()
    return f"[CIRURGIA][{status_label}] {proc_nome} — {paciente_abrev} — {convenio_short}"


# ─── Descrição rica ───────────────────────────────────────────────────────────

def build_description(episode: dict, proc_nome: str, regras: dict) -> str:
    """
    Monta descrição operacional completa para o evento do Google Calendar.
    Campos ausentes ficam em branco — nunca inventa dados.
    """
    lines: list[str] = []

    # Cabeçalho de status
    status_ag = episode.get("status_agendamento", "agendado_preliminar").upper()
    dec_status = episode.get("decision_status", "")
    lines += [
        f"STATUS: {status_ag}",
        f"DECISÃO MOTOR: {dec_status}",
        "",
    ]

    # Paciente
    paciente = episode.get("paciente_id") or episode.get("nome_paciente", "—")
    cid = episode.get("cid_principal", "")
    carater = episode.get("carater", "")
    lines += [
        "── PACIENTE ──────────────────────────────────",
        f"Paciente   : {paciente}",
        f"CID        : {cid}",
        f"Caráter    : {carater}",
        "",
    ]

    # Procedimento
    tuss    = episode.get("cod_tuss", episode.get("profile_id", ""))
    nivel   = episode.get("nivel_anatomico") or episode.get("niveis", "")
    convenio = episode.get("convenio_id", "")
    lines += [
        "── PROCEDIMENTO ──────────────────────────────",
        f"Procedimento  : {proc_nome}",
        f"Profile ID    : {episode.get('profile_id','')}",
        f"COD TUSS      : {tuss}",
        f"Nível anatômico: {nivel}",
        f"Convênio      : {convenio}",
        "",
    ]

    # Logística cirúrgica
    hospital  = episode.get("hospital_nome") or episode.get("hospital", "")
    endereco  = episode.get("hospital_endereco", "")
    sala      = episode.get("sala_cirurgica", "")
    chegada   = episode.get("hora_chegada", "")
    inicio    = episode.get("hora_inicio", "")
    lines += [
        "── LOGÍSTICA ─────────────────────────────────",
        f"Hospital  : {hospital or '— a definir'}",
        f"Endereço  : {endereco or '— a definir'}",
        f"Sala      : {sala or '— a definir'}",
        f"Chegada   : {chegada or '— a definir'}",
        f"Início    : {inicio or '— a definir'}",
        "",
    ]

    # OPME
    opme_raw = episode.get("opme_context_json") or episode.get("opme_json", [])
    if isinstance(opme_raw, str):
        try:
            opme_raw = json.loads(opme_raw)
        except Exception:
            opme_raw = []
    # Normaliza: se vier como dict com chave "itens", extrai a lista
    if isinstance(opme_raw, dict):
        opme_raw = opme_raw.get("itens", opme_raw.get("items", list(opme_raw.values())[0] if opme_raw else []))
    if not isinstance(opme_raw, list):
        opme_raw = []
    lines.append("── OPME ──────────────────────────────────────")
    if opme_raw:
        for item in opme_raw:
            if isinstance(item, dict):
                nome = item.get("descricao", item.get("nome", "item"))
                qtd  = item.get("qtd", item.get("quantidade", ""))
                tipo = item.get("tipo", "")
                suffix = f"  [{tipo}]" if tipo else ""
                lines.append(f"  • {nome} x{qtd}{suffix}")
            else:
                lines.append(f"  • {item}")
    else:
        opme_obrig = regras.get("opme_obrigatoria", False)
        lines.append("  (sem OPME)" if not opme_obrig else "  ⚠️  OPME obrigatória — a preencher")
    fornecedor = episode.get("fornecedor_opme", "")
    contato_f  = episode.get("contato_fornecedor", "")
    if fornecedor:
        lines.append(f"  Fornecedor: {fornecedor}  |  {contato_f}")
    lines.append("")

    # Equipe
    cirurgiao     = episode.get("equipe_principal", "Dr. José Correia Jr.")
    auxiliar      = episode.get("auxiliar", "— a definir")
    anestesista   = episode.get("anestesista", "— a definir")
    instrumentador = episode.get("instrumentador", "— a definir")
    lines += [
        "── EQUIPE ────────────────────────────────────",
        f"  Cirurgião      : {cirurgiao}",
        f"  Auxiliar       : {auxiliar}",
        f"  Anestesista    : {anestesista}",
        f"  Instrumentador : {instrumentador}",
        "",
    ]

    # Contatos críticos
    sec       = episode.get("contato_secretaria", "")
    cc_hosp   = episode.get("contato_centro_cirurgico", "")
    lines += [
        "── CONTATOS CRÍTICOS ─────────────────────────",
        f"  Secretária       : {sec or '— a definir'}",
        f"  Centro Cirúrgico : {cc_hosp or '— a definir'}",
        f"  Fornecedor OPME  : {contato_f or '— a definir'}",
        "",
    ]

    # Checklist pré-op
    checklist = episode.get("checklist_preop_resumo", "")
    dec_ok = dec_status in ("GO", "GO_COM_RESSALVAS")
    lines += [
        "── CHECKLIST PRÉ-OP ──────────────────────────",
        f"  [{'✅' if dec_ok else '❌'}] Autorização motor : {dec_status}",
        "  [ ] OPME confirmada com fornecedor",
        "  [ ] Risco cirúrgico",
        "  [ ] Laboratório",
        "  [ ] Consentimento assinado",
    ]
    if checklist:
        lines.append(f"  {checklist}")
    lines.append("")

    # Rastreabilidade
    ep_id  = episode.get("episodio_id", "")
    run_id = episode.get("decision_run_id", episode.get("_run_id", ""))
    lines += [
        "── RASTREABILIDADE ───────────────────────────",
        f"  EPISODIO_ID : {ep_id}",
        f"  RUN_ID      : {run_id}",
    ]

    return "\n".join(lines)


# ─── Payload completo para Google Calendar API ────────────────────────────────

def build_event_payload(
    episode: dict,
    proc_nome: str,
    regras: dict,
    calendar_id: str = "primary",
) -> dict:
    """
    Monta payload completo para Google Calendar events.insert / events.update.
    data_cirurgia deve estar no formato YYYY-MM-DD.
    hora_inicio / hora_fim em HH:MM (24h).
    """
    status_ag = episode.get("status_agendamento", "agendado_preliminar")
    color_id  = _COLOR_MAP.get(status_ag, _DEFAULT_COLOR)

    # Paciente abreviado para o título (apenas iniciais do sobrenome)
    paciente_full = episode.get("paciente_id") or episode.get("nome_paciente", "")
    partes = paciente_full.strip().split() if paciente_full else []
    if len(partes) >= 2:
        paciente_abrev = f"{partes[0]} {partes[-1][0]}."
    elif len(partes) == 1:
        paciente_abrev = partes[0]
    else:
        paciente_abrev = "Paciente"

    title = build_title(
        status_ag,
        proc_nome,
        paciente_abrev,
        episode.get("convenio_id", ""),
    )

    description = build_description(episode, proc_nome, regras)

    # Datas/horários
    data_raw = episode.get("data_cirurgia", "")

    # Normaliza: se data_cirurgia já contém tempo (ISO datetime), extrai data e hora
    if data_raw and "T" in data_raw:
        try:
            _dt_parsed = datetime.fromisoformat(data_raw.replace("Z", "+00:00"))
            data_str = _dt_parsed.strftime("%Y-%m-%d")
            hora_ini = episode.get("hora_inicio") or _dt_parsed.strftime("%H:%M")
        except Exception:
            data_str = data_raw[:10] if len(data_raw) >= 10 else data_raw
            hora_ini = episode.get("hora_inicio", "07:00")
    else:
        data_str = data_raw
        hora_ini = episode.get("hora_inicio", "07:00")

    hora_fim = episode.get("hora_fim", "")

    if data_str:
        try:
            # Monta datetime em UTC-3 (Brasil)
            start_dt = datetime.fromisoformat(f"{data_str}T{hora_ini}:00-03:00")
            if hora_fim:
                end_dt = datetime.fromisoformat(f"{data_str}T{hora_fim}:00-03:00")
            else:
                # Duração padrão 3h se hora_fim não definida
                end_dt = start_dt + timedelta(hours=3)
            start = {"dateTime": start_dt.isoformat(), "timeZone": "America/Fortaleza"}
            end   = {"dateTime": end_dt.isoformat(),   "timeZone": "America/Fortaleza"}
        except Exception:
            # Fallback: dia inteiro
            start = {"date": data_str}
            end   = {"date": data_str}
    else:
        # Sem data definida — usa amanhã como placeholder
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        start = {"date": tomorrow}
        end   = {"date": tomorrow}

    # Local
    hospital = episode.get("hospital_nome") or episode.get("hospital", "")
    endereco = episode.get("hospital_endereco", "")
    sala     = episode.get("sala_cirurgica", "")
    location_parts = [p for p in [hospital, sala, endereco] if p]
    location = " | ".join(location_parts) if location_parts else ""

    payload: dict[str, Any] = {
        "summary":     title,
        "description": description,
        "location":    location,
        "colorId":     str(color_id),
        "start":       start,
        "end":         end,
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup", "minutes": 24 * 60},   # 24h antes
                {"method": "popup", "minutes": 3  * 60},   # 3h antes
                {"method": "popup", "minutes": 60},         # 1h antes
            ],
        },
        "extendedProperties": {
            "private": {
                "neuroauth_episodio_id": episode.get("episodio_id", ""),
                "neuroauth_run_id":      episode.get("decision_run_id", ""),
                "neuroauth_profile_id":  episode.get("profile_id", ""),
                "neuroauth_version":     "1.0.0",
            }
        },
    }

    return payload
