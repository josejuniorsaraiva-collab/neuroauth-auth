"""
app/services/surgeon_validator.py
NEUROAUTH — Validador de equipe cirúrgica contra a aba CIRURGIOES.

Função principal:
  validate_cirurgiao(payload: dict) -> dict

Retorno:
  {
    "ok": bool,
    "pendencias": [...],          # lista de strings descritivas
    "principal_nome": str,        # nome_exibicao do principal (se encontrado)
    "auxiliares_display": str,    # "Dr. X (1º aux); Dr. Y (2º aux)"
  }

Nunca lança exceção — erros de Sheets retornam ok=False com detalhe.
"""
from __future__ import annotations

import logging
from typing import Optional

from repositories.sheets_client import get_worksheet, read_all_records

logger = logging.getLogger("neuroauth.surgeon_validator")

TAB_CIRURGIOES = "CIRURGIOES"


def _load_cirurgioes_ativos() -> dict[str, dict]:
    """
    Carrega a tabela CIRURGIOES e retorna dict {cirurgiao_id: row_dict}
    apenas para cirurgiões com ativo=TRUE (case-insensitive).
    Nunca lança exceção.
    """
    try:
        ws = get_worksheet(TAB_CIRURGIOES)
        rows = read_all_records(ws, head=3)
        ativos = {
            r["cirurgiao_id"]: r
            for r in rows
            if r.get("cirurgiao_id") and str(r.get("ativo", "")).strip().upper() == "TRUE"
        }
        logger.debug("surgeon_validator: %d cirurgiões ativos carregados", len(ativos))
        return ativos
    except Exception as exc:
        logger.error("surgeon_validator: erro ao carregar CIRURGIOES: %s", exc)
        raise  # re-raise para ser capturado no caller com mensagem clara


def validate_cirurgiao(payload: dict) -> dict:
    """
    Valida cirurgião principal e auxiliares contra a tabela CIRURGIOES.

    Args:
        payload: dict com campos:
            - cirurgiao_principal: str  (ID, ex: "CIR_001") — obrigatório
            - cirurgioes_auxiliares: list[dict]  — cada item: {id, ordem}

    Returns:
        dict com campos:
            - ok: bool
            - pendencias: list[str]
            - principal_nome: str
            - auxiliares_display: str
    """
    pendencias: list[str] = []
    ativos: dict[str, dict] = {}

    # Carregar tabela de referência
    try:
        ativos = _load_cirurgioes_ativos()
    except Exception as exc:
        return {
            "ok": False,
            "pendencias": [f"Erro ao carregar tabela CIRURGIOES: {type(exc).__name__}: {str(exc)[:200]}"],
            "principal_nome": "",
            "auxiliares_display": "",
        }

    if not ativos:
        return {
            "ok": False,
            "pendencias": ["Tabela CIRURGIOES está vazia ou sem cirurgiões com ativo=TRUE."],
            "principal_nome": "",
            "auxiliares_display": "",
        }

    # ── Validar principal ─────────────────────────────────────────────────────
    principal_id = (payload.get("cirurgiao_principal") or "").strip()

    if not principal_id:
        return {
            "ok": False,
            "pendencias": [
                "PENDENCIA_PREENCHIMENTO: Campo obrigatório ausente: cirurgiao_principal."
            ],
            "principal_nome": "",
            "auxiliares_display": "",
        }

    if principal_id not in ativos:
        pendencias.append(
            f"Cirurgião principal '{principal_id}' não encontrado ou inativo em CIRURGIOES."
        )
        principal_nome = principal_id
    else:
        principal_nome = ativos[principal_id].get("nome_exibicao", principal_id)

    # ── Validar auxiliares ────────────────────────────────────────────────────
    auxiliares: list[dict] = payload.get("cirurgioes_auxiliares") or []
    ordens_vistas: set[int] = set()
    auxiliares_display_parts: list[str] = []

    if len(auxiliares) > 3:
        pendencias.append(
            f"Máximo de 3 auxiliares permitidos (regra CBHPM). "
            f"Recebido: {len(auxiliares)}. Revisar caso."
        )

    for aux in auxiliares:
        aux_id = (aux.get("id") or "").strip()
        aux_ordem = aux.get("ordem")

        if not aux_id:
            pendencias.append("Auxiliar com id vazio/ausente.")
            continue

        # Existência e atividade
        if aux_id not in ativos:
            pendencias.append(
                f"Auxiliar '{aux_id}' não encontrado ou inativo em CIRURGIOES."
            )
            aux_nome = aux_id
        else:
            aux_nome = ativos[aux_id].get("nome_exibicao", aux_id)

        # Principal ≠ auxiliar
        if aux_id == principal_id:
            pendencias.append(
                f"Cirurgião '{aux_id}' aparece como principal E como auxiliar. "
                f"Não é permitido."
            )

        # Ordens únicas
        if aux_ordem is not None:
            try:
                ordem_int = int(aux_ordem)
                if ordem_int in ordens_vistas:
                    pendencias.append(
                        f"Ordem {ordem_int} duplicada nos auxiliares."
                    )
                ordens_vistas.add(ordem_int)
            except (TypeError, ValueError):
                pendencias.append(f"Ordem inválida para auxiliar '{aux_id}': '{aux_ordem}'.")

        # Montar display
        sufixo = f"{aux_ordem}º aux" if aux_ordem is not None else "aux"
        auxiliares_display_parts.append(f"{aux_nome} ({sufixo})")

    return {
        "ok": len(pendencias) == 0,
        "pendencias": pendencias,
        "principal_nome": principal_nome,
        "auxiliares_display": "; ".join(auxiliares_display_parts),
    }
