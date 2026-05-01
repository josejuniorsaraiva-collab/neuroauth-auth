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

    # ── Guard de período fechado ──────────────────────────────────────────────
    periodo_comp = str(linhas[0].get("periodo_competencia", "")).strip()
    if periodo_comp and _get_status_periodo_internal(periodo_comp) == "FECHADO":
        raise PermissionError(
            f"Período {periodo_comp} está FECHADO. "
            f"Novas gravações bloqueadas. Use /retificar para ajustes."
        )

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


# ============================================================
# GOVERNANÇA FINANCEIRA — feat: controlled amendment + period closure
# Commit: feat: financial governance — retificação + fechamento de competência
# ============================================================

import uuid as _uuid

TAB_AUDIT    = "PRODUCAO_AUDIT"
TAB_PERIODOS = "PERIODOS_COMPETENCIA"


# ── Adapter: envolve API interna na interface compatível com MagicMock (testes) ──

class _SheetsAdapter:
    def get_all_records(self, worksheet: str) -> list:
        return read_all_records(get_worksheet(worksheet), head=HEAD)

    def append_row_by_header(self, worksheet: str, row_data: dict) -> None:
        append_row_by_header(get_worksheet(worksheet), row_data, head=HEAD)

def _make_client() -> "_SheetsAdapter":
    return _SheetsAdapter()


# ── Status de período ─────────────────────────────────────────────────────────

def get_status_periodo(sheet_client, periodo: str) -> str:
    """Retorna 'ABERTO' ou 'FECHADO'. Default 'ABERTO' se período não cadastrado."""
    try:
        rows = sheet_client.get_all_records(worksheet=TAB_PERIODOS)
        for r in rows:
            if str(r.get("periodo", "")).strip() == periodo.strip():
                return str(r.get("status", "ABERTO")).strip().upper()
    except Exception:
        pass
    return "ABERTO"


def _get_status_periodo_internal(periodo: str) -> str:
    """Wrapper interno sem injeção — usado por gravar_producao() na produção."""
    return get_status_periodo(_make_client(), periodo)


# ── Fechamento de competência ─────────────────────────────────────────────────

def fechar_periodo(sheet_client, periodo: str, usuario: str) -> dict:
    """
    Fecha o período de competência YYYY-MM.
    Após fechamento:
      - gravar_producao() bloqueia novas gravações
      - retificar_producao() exige forcar=True
    """
    status = get_status_periodo(sheet_client, periodo)
    if status == "FECHADO":
        raise ValueError(f"Período {periodo} já está FECHADO.")

    rows = sheet_client.get_all_records(worksheet=TAB_PRODUCAO)
    linhas_periodo = [
        r for r in rows
        if str(r.get("periodo_competencia", "")).strip() == periodo
        and str(r.get("status_producao", "")).strip().upper() == "ABERTO"
    ]
    total_casos = len(set(str(r.get("caso_id", "")) for r in linhas_periodo))
    valor_total = sum(float(r.get("valor_calculado", 0) or 0) for r in linhas_periodo)

    sheet_client.append_row_by_header(
        worksheet=TAB_PERIODOS,
        row_data={
            "periodo":     periodo,
            "status":      "FECHADO",
            "fechado_em":  datetime.utcnow().isoformat() + "Z",
            "fechado_por": usuario,
            "total_casos": str(total_casos),
            "valor_total": str(round(valor_total, 2)),
        }
    )
    return {
        "periodo":     periodo,
        "status":      "FECHADO",
        "total_casos": total_casos,
        "valor_total": round(valor_total, 2),
    }


# ── Retificação controlada ────────────────────────────────────────────────────

def retificar_producao(
    caso_id:     str,
    sheet_client,
    motivo:      str,
    usuario:     str,
    novos_dados: dict,
    forcar:      bool = False,
) -> dict:
    """
    Retificação controlada de produção.

    Regras imutáveis:
    - Nunca apaga linha existente
    - Cria nova versão (v2, v3...) com os novos dados
    - Marca versão anterior como SUBSTITUIDA
    - Grava rastro completo em PRODUCAO_AUDIT
    - Período FECHADO bloqueia; use forcar=True para override (registrado no audit)
    """
    rows = sheet_client.get_all_records(worksheet=TAB_PRODUCAO)
    linhas_ativas = [
        r for r in rows
        if str(r.get("caso_id", "")).strip() == caso_id
        and str(r.get("status_producao", "")).strip().upper() != "SUBSTITUIDA"
    ]
    if not linhas_ativas:
        raise ValueError(f"Nenhuma linha ativa encontrada para caso_id={caso_id}")

    def _vnum(r):
        try:
            return int(str(r.get("versao", "v1")).strip().lower().replace("v", ""))
        except Exception:
            return 1

    linha_atual  = max(linhas_ativas, key=_vnum)
    versao_atual = _vnum(linha_atual)
    versao_nova  = versao_atual + 1

    periodo = str(linha_atual.get("periodo_competencia", "")).strip()
    periodo_fechado = False
    if periodo:
        if get_status_periodo(sheet_client, periodo) == "FECHADO":
            periodo_fechado = True
            if not forcar:
                raise PermissionError(
                    f"Período {periodo} está FECHADO. "
                    f"Use forcar=True com motivo explícito para override."
                )

    # 1. Gravar snapshot SUBSTITUIDA
    linha_sub = dict(linha_atual)
    linha_sub["status_producao"] = "SUBSTITUIDA"
    linha_sub["substituida_em"]  = datetime.utcnow().isoformat() + "Z"
    linha_sub["substituida_por"] = f"v{versao_nova}"
    sheet_client.append_row_by_header(worksheet=TAB_PRODUCAO, row_data=linha_sub)

    # 2. Gravar nova versão
    nova = dict(linha_atual)
    nova.update(novos_dados)
    nova["versao"]            = f"v{versao_nova}"
    nova["status_producao"]   = "ABERTO"
    nova["timestamp_calculo"] = datetime.utcnow().isoformat() + "Z"
    nova["substituida_em"]    = ""
    nova["substituida_por"]   = ""
    sheet_client.append_row_by_header(worksheet=TAB_PRODUCAO, row_data=nova)

    # 3. Audit log
    audit_id = _uuid.uuid4().hex[:8].upper()
    override = " [OVERRIDE_PERIODO_FECHADO]" if periodo_fechado else ""
    sheet_client.append_row_by_header(
        worksheet=TAB_AUDIT,
        row_data={
            "audit_id":         audit_id,
            "caso_id":          caso_id,
            "versao_anterior":  f"v{versao_atual}",
            "versao_nova":      f"v{versao_nova}",
            "usuario":          usuario,
            "motivo":           motivo + override,
            "timestamp_audit":  datetime.utcnow().isoformat() + "Z",
            "action":           "RETIFICACAO_FORCADA" if forcar else "RETIFICACAO",
            "campos_alterados": str(list(novos_dados.keys())),
        }
    )
    return {
        "audit_id":    audit_id,
        "caso_id":     caso_id,
        "versao_nova": f"v{versao_nova}",
        "ok":          True,
    }
