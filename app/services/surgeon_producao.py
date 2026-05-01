"""
app/services/surgeon_producao.py
NEUROAUTH — Cálculo de produção paralela por cirurgião.

Funções públicas:
  get_reducao(operadora, porte, ordem) -> float
    Consulta REDUCAO_AUXILIAR com precedência de 4 níveis.
    Nenhum hardcode de percentuais: tudo vem do Sheets.

  calcular_producao(caso_id, valor_base, operadora, porte,
                    principal_id, auxiliares, data_proc, status_aut) -> list[dict]
    Retorna lista de dicts prontos para append em PRODUCAO via append_row_by_header.

  gravar_producao(linhas) -> bool
    Persiste a lista retornada por calcular_producao na aba PRODUCAO.

Precedência get_reducao (score maior = prioridade):
  1. operadora específica + porte específico    → score 3
  2. operadora específica + TODOS               → score 2
  3. DEFAULT       + porte específico           → score 1
  4. DEFAULT       + TODOS                      → score 0  ← fallback final
  Fallback emergencial (sem linha no Sheets)    → 0.30
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from repositories.sheets_client import get_worksheet, read_all_records, append_row_by_header

logger = logging.getLogger("neuroauth.surgeon_producao")

TAB_REDUCAO    = "REDUCAO_AUXILIAR"
TAB_PRODUCAO   = "PRODUCAO"
HEAD           = 3
_FALLBACK_PCT  = 0.30  # fallback emergencial se tabela estiver vazia


# ─── Lookup de redução ────────────────────────────────────────────────────────

def get_reducao(operadora: str, porte: str, ordem: int) -> float:
    """
    Retorna o percentual de redução para auxiliar de `ordem` com base em
    `operadora` e `porte_procedimento`, consultando REDUCAO_AUXILIAR.

    Precedência (score maior vence):
      operadora_específica + porte_específico   → 3
      operadora_específica + TODOS              → 2
      DEFAULT              + porte_específico   → 1
      DEFAULT              + TODOS              → 0
    Se nenhuma linha encontrada → fallback 0.30 (CBHPM mínimo).
    """
    try:
        ws    = get_worksheet(TAB_REDUCAO)
        rows  = read_all_records(ws, head=HEAD)
    except Exception as exc:
        logger.error("get_reducao: erro ao carregar REDUCAO_AUXILIAR: %s", exc)
        return _FALLBACK_PCT

    operadora_up = operadora.strip().upper() if operadora else "DEFAULT"
    porte_up     = porte.strip().upper() if porte else "TODOS"
    ordem_str    = str(ordem)

    def _score(row: dict) -> int:
        row_op    = (row.get("operadora") or "").strip().upper()
        row_porte = (row.get("porte_procedimento") or "").strip().upper()
        row_ordem = (row.get("ordem_auxiliar") or "").strip()

        # Filtrar pela ordem primeiro
        if row_ordem != ordem_str:
            return -1

        # Filtrar por operadora (específica ou DEFAULT)
        if row_op == operadora_up:
            op_score = 2
        elif row_op == "DEFAULT":
            op_score = 0
        else:
            return -1  # operadora diferente e não é DEFAULT

        # Filtrar por porte
        if row_porte == porte_up:
            pt_score = 1
        elif row_porte == "TODOS":
            pt_score = 0
        else:
            return -1  # porte diferente e não é TODOS

        return op_score + pt_score

    candidatos = [(row, _score(row)) for row in rows]
    candidatos = [(row, sc) for row, sc in candidatos if sc >= 0]

    if not candidatos:
        logger.warning(
            "get_reducao: nenhuma linha para ordem=%d operadora=%s porte=%s → fallback %.2f",
            ordem, operadora_up, porte_up, _FALLBACK_PCT,
        )
        return _FALLBACK_PCT

    melhor_row, melhor_score = max(candidatos, key=lambda x: x[1])
    pct_str = (melhor_row.get("percentual_reducao") or "").strip()

    try:
        pct = float(pct_str)
    except (ValueError, TypeError):
        logger.warning("get_reducao: valor inválido '%s' → fallback %.2f", pct_str, _FALLBACK_PCT)
        return _FALLBACK_PCT

    logger.debug(
        "get_reducao: ordem=%d op=%s porte=%s → %.2f (score=%d)",
        ordem, operadora_up, porte_up, pct, melhor_score,
    )
    return pct


# ─── Cálculo de produção ─────────────────────────────────────────────────────

def calcular_producao(
    caso_id: str,
    valor_base: float,
    operadora: str,
    porte: str,
    principal_id: str,
    auxiliares: list[dict],
    data_proc: Optional[str] = None,
    status_aut: str = "PENDENTE",
) -> list[dict]:
    """
    Calcula as linhas de produção para todos os membros da equipe.

    Args:
        caso_id:      identificador único do caso/episódio
        valor_base:   valor de tabela do procedimento (principal = 100%)
        operadora:    nome/código da operadora (para lookup de redução)
        porte:        porte do procedimento (para lookup de redução)
        principal_id: ID do cirurgião principal
        auxiliares:   lista de dicts [{id, ordem}, ...]
        data_proc:    data ISO do procedimento (default: hoje UTC)
        status_aut:   status de autorização inicial

    Returns:
        Lista de dicts, uma por membro da equipe, no formato da aba PRODUCAO.
    """
    if data_proc is None:
        data_proc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    linhas: list[dict] = []

    # ── Cirurgião principal — 100% ────────────────────────────────────────────
    linhas.append({
        "caso_id":             caso_id,
        "cirurgiao_id":        principal_id,
        "papel":               "PRINCIPAL",
        "ordem_auxiliar":      "0",
        "valor_base":          str(round(float(valor_base), 2)),
        "percentual_aplicado": "1.00",
        "valor_calculado":     str(round(float(valor_base) * 1.00, 2)),
        "data_procedimento":   data_proc,
        "operadora":           operadora,
        "status_autorizacao":  status_aut,
        "status_pagamento":    "PENDENTE",
    })

    # ── Auxiliares — redução por ordem ────────────────────────────────────────
    for aux in sorted(auxiliares, key=lambda a: int(a.get("ordem", 99))):
        aux_id  = (aux.get("id") or "").strip()
        aux_ord = int(aux.get("ordem", 1))

        if not aux_id:
            logger.warning("calcular_producao: auxiliar com id vazio ignorado")
            continue

        pct = get_reducao(operadora, porte, aux_ord)
        valor_calc = round(float(valor_base) * pct, 2)

        linhas.append({
            "caso_id":             caso_id,
            "cirurgiao_id":        aux_id,
            "papel":               f"AUXILIAR_{aux_ord}",
            "ordem_auxiliar":      str(aux_ord),
            "valor_base":          str(round(float(valor_base), 2)),
            "percentual_aplicado": str(round(pct, 4)),
            "valor_calculado":     str(valor_calc),
            "data_procedimento":   data_proc,
            "operadora":           operadora,
            "status_autorizacao":  status_aut,
            "status_pagamento":    "PENDENTE",
        })

    logger.info(
        "calcular_producao: caso=%s principal=%s auxiliares=%d total_valor=%.2f",
        caso_id, principal_id, len(auxiliares),
        sum(float(l["valor_calculado"]) for l in linhas),
    )
    return linhas


# ─── Persistência ─────────────────────────────────────────────────────────────

def gravar_producao(linhas: list[dict]) -> bool:
    """
    Persiste lista de linhas de produção na aba PRODUCAO.
    Retorna True se tudo gravado, False se algum erro.
    """
    if not linhas:
        logger.warning("gravar_producao: lista vazia, nada a gravar.")
        return True

    try:
        ws = get_worksheet(TAB_PRODUCAO)
    except Exception as exc:
        logger.error("gravar_producao: erro ao obter aba PRODUCAO: %s", exc)
        return False

    erros = 0
    for linha in linhas:
        try:
            append_row_by_header(ws, linha, head=HEAD)
            logger.debug(
                "gravar_producao: OK caso=%s cirurgiao=%s papel=%s valor=%s",
                linha.get("caso_id"), linha.get("cirurgiao_id"),
                linha.get("papel"), linha.get("valor_calculado"),
            )
        except Exception as exc:
            logger.error("gravar_producao: erro ao gravar linha %s: %s", linha, exc)
            erros += 1

    return erros == 0
