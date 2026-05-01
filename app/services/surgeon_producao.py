"""
app/services/surgeon_producao.py
NEUROAUTH — Cálculo de produção paralela por cirurgião.

Funções públicas:
  get_reducao(operadora, porte, ordem) -> dict
    Consulta REDUCAO_AUXILIAR com precedência de 4 níveis.
    Retorna dict com percentual + metadados da regra vencedora.
    Nenhum hardcode de percentuais: tudo vem do Sheets.

  resolve_decision_run_id(caso_id, payload, ws_runs) -> Optional[str]
    Resolve decision_run_id com fallback de 3 níveis:
      1. payload, 2. 21_DECISION_RUNS, 3. None

  calcular_producao(caso_id, valor_base, operadora, porte,
                    principal_id, auxiliares, data_proc, status_aut,
                    decision_run_id) -> list[dict]
    Retorna lista de dicts com snapshot financeiro imutável,
    prontos para append em PRODUCAO via append_row_by_header.

  gravar_producao(linhas) -> bool
    Persiste a lista retornada por calcular_producao na aba PRODUCAO.
    Raises ValueError se caso_id já tiver linhas gravadas (guard anti-recálculo).

Precedência get_reducao (score maior = prioridade):
  1. operadora específica + porte específico    → score 3
  2. operadora específica + TODOS               → score 2
  3. DEFAULT       + porte específico           → score 1
  4. DEFAULT       + TODOS                      → score 0  ← fallback final
  Fallback emergencial (sem linha no Sheets)    → percentual=0.30
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from repositories.sheets_client import get_worksheet, read_all_records, append_row_by_header

logger = logging.getLogger("neuroauth.surgeon_producao")

TAB_REDUCAO    = "REDUCAO_AUXILIAR"
TAB_PRODUCAO   = "PRODUCAO"
TAB_RUNS       = "21_DECISION_RUNS"
HEAD           = 3
_FALLBACK_PCT  = 0.30  # fallback emergencial se tabela estiver vazia


# ─── Lookup de redução ────────────────────────────────────────────────────────

def get_reducao(operadora: str, porte: str, ordem: int) -> dict:
    """
    Retorna percentual + metadados da regra vencedora de REDUCAO_AUXILIAR.

    Precedência (score maior vence):
      operadora_específica + porte_específico   → 3
      operadora_específica + TODOS              → 2
      DEFAULT              + porte_específico   → 1
      DEFAULT              + TODOS              → 0
    Se nenhuma linha encontrada → fallback 0.30.

    Returns:
        dict com chaves:
          percentual           (float)
          fonte                (str)  ex: "CBHPM/AMB", "MANUAL", "FALLBACK_*"
          operadora_detectada  (str)
          porte_detectado      (str)
          ordem_aplicada       (str)
          vigencia_inicio      (str)
          [erro]               (str, só presente em fallback emergencial)
    """
    try:
        ws   = get_worksheet(TAB_REDUCAO)
        rows = read_all_records(ws, head=HEAD)
    except Exception as exc:
        logger.error("get_reducao: erro ao carregar REDUCAO_AUXILIAR: %s", exc)
        return {
            "percentual":          _FALLBACK_PCT,
            "fonte":               "FALLBACK_EMERGENCIAL",
            "operadora_detectada": "FALLBACK",
            "porte_detectado":     "TODOS",
            "ordem_aplicada":      str(ordem),
            "vigencia_inicio":     "",
            "erro":                str(exc),
        }

    operadora_up = operadora.strip().upper() if operadora else "DEFAULT"
    porte_up     = porte.strip().upper() if porte else "TODOS"
    ordem_str    = str(ordem)

    def _score(row: dict) -> int:
        row_op    = (row.get("operadora") or "").strip().upper()
        row_porte = (row.get("porte_procedimento") or "").strip().upper()
        row_ordem = (row.get("ordem_auxiliar") or "").strip()

        if row_ordem != ordem_str:
            return -1

        if row_op == operadora_up:
            op_score = 2
        elif row_op == "DEFAULT":
            op_score = 0
        else:
            return -1

        if row_porte == porte_up:
            pt_score = 1
        elif row_porte == "TODOS":
            pt_score = 0
        else:
            return -1

        return op_score + pt_score

    candidatos = [(row, _score(row)) for row in rows]
    candidatos = [(row, sc) for row, sc in candidatos if sc >= 0]

    if not candidatos:
        logger.warning(
            "get_reducao: nenhuma linha para ordem=%d operadora=%s porte=%s → fallback %.2f",
            ordem, operadora_up, porte_up, _FALLBACK_PCT,
        )
        return {
            "percentual":          _FALLBACK_PCT,
            "fonte":               "FALLBACK_SEM_LINHA",
            "operadora_detectada": "DEFAULT",
            "porte_detectado":     "TODOS",
            "ordem_aplicada":      ordem_str,
            "vigencia_inicio":     "",
        }

    melhor_row, melhor_score = max(candidatos, key=lambda x: x[1])

    pct_str = (melhor_row.get("percentual_reducao") or "").strip()
    try:
        pct = float(pct_str)
    except (ValueError, TypeError):
        logger.warning("get_reducao: valor inválido '%s' → fallback %.2f", pct_str, _FALLBACK_PCT)
        pct = _FALLBACK_PCT

    logger.debug(
        "get_reducao: ordem=%d op=%s porte=%s → %.4f (score=%d fonte=%s)",
        ordem, operadora_up, porte_up, pct, melhor_score,
        melhor_row.get("fonte", "?"),
    )

    return {
        "percentual":          pct,
        "fonte":               str(melhor_row.get("fonte", "DESCONHECIDA")),
        "operadora_detectada": str(melhor_row.get("operadora", "?")),
        "porte_detectado":     str(melhor_row.get("porte_procedimento", "?")),
        "ordem_aplicada":      str(melhor_row.get("ordem_auxiliar", ordem)),
        "vigencia_inicio":     str(melhor_row.get("vigencia_inicio", "")),
    }


# ─── Resolução de decision_run_id ─────────────────────────────────────────────

def resolve_decision_run_id(caso_id: str, payload: dict, ws_runs=None) -> Optional[str]:
    """
    Resolve decision_run_id com fallback de 3 níveis:
      1. payload.get("decision_run_id")   — prioridade máxima
      2. aba 21_DECISION_RUNS onde caso_id bate
      3. None — não bloqueia, grava null

    Args:
        caso_id:  ID do caso/episódio
        payload:  dict com possível chave "decision_run_id"
        ws_runs:  worksheet 21_DECISION_RUNS já aberta (opcional)
    """
    # 1. payload
    dr_payload = payload.get("decision_run_id")
    if dr_payload:
        return str(dr_payload)

    # 2. aba 21_DECISION_RUNS
    try:
        if ws_runs is None:
            ws_runs = get_worksheet(TAB_RUNS)
        rows = read_all_records(ws_runs, head=HEAD)
        for r in rows:
            if str(r.get("caso_id", "")) == caso_id:
                run_id = str(r.get("id", "")).strip()
                if run_id:
                    return run_id
    except Exception as exc:
        logger.warning(
            "resolve_decision_run_id: erro ao consultar %s: %s — usando None",
            TAB_RUNS, exc,
        )

    # 3. null
    return None


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
    decision_run_id: Optional[str] = None,
) -> list[dict]:
    """
    Calcula as linhas de produção para todos os membros da equipe.
    Cada linha contém snapshot financeiro imutável do momento do cálculo.

    Args:
        caso_id:          identificador único do caso/episódio
        valor_base:       valor de tabela do procedimento (principal = 100%)
        operadora:        nome/código da operadora (para lookup de redução)
        porte:            porte do procedimento (para lookup de redução)
        principal_id:     ID do cirurgião principal
        auxiliares:       lista de dicts [{id, ordem}, ...]
        data_proc:        data ISO do procedimento (default: hoje UTC)
        status_aut:       status de autorização inicial
        decision_run_id:  ID do decision run associado (pode ser None)

    Returns:
        Lista de dicts com snapshot completo, uma por membro da equipe.
        Colunas: caso_id, cirurgiao_id, papel, ordem_auxiliar,
                 valor_base, valor_base_usado, percentual_aplicado,
                 valor_calculado, operadora_detectada, porte_detectado,
                 fonte_regra, timestamp_calculo, decision_run_id,
                 data_procedimento, operadora, status_autorizacao, status_pagamento
    """
    if data_proc is None:
        data_proc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    timestamp_calculo = datetime.utcnow().isoformat() + "Z"
    valor_base_f = float(valor_base)
    linhas: list[dict] = []

    # ── Cirurgião principal — 100% ────────────────────────────────────────────
    linhas.append({
        "caso_id":             caso_id,
        "cirurgiao_id":        principal_id,
        "papel":               "PRINCIPAL",
        "ordem_auxiliar":      "0",
        "valor_base":          str(round(valor_base_f, 2)),
        "valor_base_usado":    str(round(valor_base_f, 2)),
        "percentual_aplicado": "1.00",
        "valor_calculado":     str(round(valor_base_f * 1.00, 2)),
        "operadora_detectada": operadora,
        "porte_detectado":     porte,
        "fonte_regra":         "N/A_PRINCIPAL",
        "timestamp_calculo":   timestamp_calculo,
        "decision_run_id":     decision_run_id or "",
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

        regra      = get_reducao(operadora, porte, aux_ord)
        pct        = regra["percentual"]
        valor_calc = round(valor_base_f * pct, 2)

        linhas.append({
            "caso_id":             caso_id,
            "cirurgiao_id":        aux_id,
            "papel":               f"AUXILIAR_{aux_ord}",
            "ordem_auxiliar":      str(aux_ord),
            "valor_base":          str(round(valor_base_f, 2)),
            "valor_base_usado":    str(round(valor_base_f, 2)),
            "percentual_aplicado": str(round(pct, 4)),
            "valor_calculado":     str(valor_calc),
            "operadora_detectada": regra["operadora_detectada"],
            "porte_detectado":     regra["porte_detectado"],
            "fonte_regra":         regra["fonte"],
            "timestamp_calculo":   timestamp_calculo,
            "decision_run_id":     decision_run_id or "",
            "data_procedimento":   data_proc,
            "operadora":           operadora,
            "status_autorizacao":  status_aut,
            "status_pagamento":    "PENDENTE",
        })

    logger.info(
        "calcular_producao: caso=%s principal=%s auxiliares=%d total_valor=%.2f decision_run=%s",
        caso_id, principal_id, len(auxiliares),
        sum(float(l["valor_calculado"]) for l in linhas),
        decision_run_id,
    )
    return linhas


# ─── Persistência ─────────────────────────────────────────────────────────────

def gravar_producao(linhas: list[dict]) -> bool:
    """
    Persiste lista de linhas de produção na aba PRODUCAO.

    GUARD: Se caso_id já tiver linhas em PRODUCAO → raises ValueError.
    Produção antiga NUNCA é sobrescrita silenciosamente.
    Use endpoint de retificação explícita para corrigir registros existentes.

    Args:
        linhas: lista de dicts retornada por calcular_producao()

    Returns:
        True se tudo gravado, False se algum erro de escrita.

    Raises:
        ValueError: se caso_id já tiver registros em PRODUCAO.
    """
    if not linhas:
        logger.warning("gravar_producao: lista vazia, nada a gravar.")
        return True

    try:
        ws = get_worksheet(TAB_PRODUCAO)
    except Exception as exc:
        logger.error("gravar_producao: erro ao obter aba PRODUCAO: %s", exc)
        return False

    # ── Guard anti-recálculo silencioso ───────────────────────────────────────
    caso_id = linhas[0].get("caso_id", "")
    try:
        registros_existentes = read_all_records(ws, head=HEAD)
        existentes = [
            r for r in registros_existentes
            if str(r.get("caso_id", "")) == caso_id
        ]
        if existentes:
            raise ValueError(
                f"Produção para caso_id={caso_id} já gravada "
                f"({len(existentes)} linha(s) existente(s)). "
                f"Use endpoint de retificação explícita."
            )
    except ValueError:
        raise  # propaga o guard
    except Exception as exc:
        logger.error(
            "gravar_producao: erro ao verificar existência de caso_id=%s: %s",
            caso_id, exc,
        )
        return False

    # ── Append linha a linha ───────────────────────────────────────────────────
    erros = 0
    for linha in linhas:
        try:
            append_row_by_header(ws, linha, head=HEAD)
            logger.debug(
                "gravar_producao: OK caso=%s cirurgiao=%s papel=%s valor=%s fonte=%s",
                linha.get("caso_id"), linha.get("cirurgiao_id"),
                linha.get("papel"), linha.get("valor_calculado"),
                linha.get("fonte_regra"),
            )
        except Exception as exc:
            logger.error("gravar_producao: erro ao gravar linha %s: %s", linha, exc)
            erros += 1

    if erros == 0:
        logger.info(
            "gravar_producao: caso=%s — %d linha(s) gravada(s) com sucesso",
            caso_id, len(linhas),
        )
    else:
        logger.error(
            "gravar_producao: caso=%s — %d erro(s) em %d linha(s)",
            caso_id, erros, len(linhas),
        )

    return erros == 0
