"""
NEUROAUTH — Insights Repository  (Bloco 2: INSIGHTS ENGINE)
Versão: 1.0.0

Responsabilidade: ler 23_FEEDBACK_LOOP, calcular métricas em memória
e reescrever 24_INSIGHTS idempotentemente.

Nunca altera o motor. Nunca lança exceção para o chamador.
Graceful degradation total: toda falha é logada, nunca propagada.
Auto-cria a aba 24_INSIGHTS se não existir.

Blocos gerados:
  A — RESUMO GLOBAL        : totais e taxas globais
  B — POR PROFILE_ID       : breakdown por perfil de procedimento
  C — POR CONVENIO_ID      : breakdown por convênio

Fonte de dados: 23_FEEDBACK_LOOP (header na linha 1, dados da linha 2 em diante).
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from typing import Any

logger = logging.getLogger("neuroauth.insights")

# ─── Constantes ──────────────────────────────────────────────────────────────

INSIGHTS_SHEET   = "24_INSIGHTS"
FEEDBACK_SHEET   = "23_FEEDBACK_LOOP"

# decision_status → categoria para os contadores
_GO_STATUSES        = {"GO", "GO_COM_RESSALVAS"}
_NO_GO_STATUSES     = {"NO_GO"}
_PENDENCIA_STATUSES = {"PENDENCIA_OBRIGATORIA", "PENDENCIA"}


# ─── Helpers de classificação ────────────────────────────────────────────────

def _classify(status: str) -> str:
    """Retorna 'GO' | 'NO_GO' | 'PENDENCIA' | 'OUTRO'."""
    s = (status or "").strip().upper()
    if s in _GO_STATUSES:
        return "GO"
    if s in _NO_GO_STATUSES:
        return "NO_GO"
    if s in _PENDENCIA_STATUSES:
        return "PENDENCIA"
    return "OUTRO"


def _rate(num: int, den: int) -> str:
    """Retorna taxa formatada como '72.50%' ou '0.00%' se denominador == 0."""
    if den == 0:
        return "0.00%"
    return f"{100.0 * num / den:.2f}%"


# ─── Leitura do 23_FEEDBACK_LOOP ─────────────────────────────────────────────

def _read_feedback_rows() -> list[dict]:
    """
    Lê todas as linhas válidas de 23_FEEDBACK_LOOP.
    Header na linha 1; dados a partir da linha 2.
    Retorna lista de dicts. Lista vazia se aba inexistente ou vazia.
    Nunca lança exceção.
    """
    try:
        from .sheets_client import get_worksheet
        ws = get_worksheet(FEEDBACK_SHEET)
        all_values = ws.get_all_values()
        if len(all_values) < 2:
            logger.info("_read_feedback_rows: aba vazia ou apenas cabeçalho")
            return []

        headers = [str(h).strip() for h in all_values[0]]
        records: list[dict] = []

        for raw_row in all_values[1:]:
            padded = list(raw_row) + [""] * max(0, len(headers) - len(raw_row))
            row_dict = {
                headers[i]: str(padded[i]).strip()
                for i in range(len(headers))
                if headers[i]
            }
            # Ignorar linhas marcadas como dados de teste
            if row_dict.get("is_test_data", "").strip().upper() == "TRUE":
                continue
            if any(v for v in row_dict.values()):
                records.append(row_dict)

        logger.info("_read_feedback_rows: %d linhas válidas lidas", len(records))
        return records

    except Exception as exc:
        logger.error("_read_feedback_rows: falha ao ler '%s' — %s", FEEDBACK_SHEET, exc)
        return []


# ─── Cálculo de métricas ─────────────────────────────────────────────────────

def _compute_metrics(rows: list[dict]) -> dict[str, Any]:
    """
    Calcula todas as métricas em memória a partir das linhas do FEEDBACK_LOOP.
    Retorna dict com chaves 'global', 'by_profile', 'by_convenio'.
    """
    total = len(rows)
    g_go = g_no_go = g_pend = g_outro = 0

    prof_total:   dict[str, int] = defaultdict(int)
    prof_go:      dict[str, int] = defaultdict(int)
    prof_no_go:   dict[str, int] = defaultdict(int)
    prof_pend:    dict[str, int] = defaultdict(int)
    prof_motivos: dict[str, list[str]] = defaultdict(list)
    prof_pendencias: dict[str, list[str]] = defaultdict(list)

    conv_total:  dict[str, int] = defaultdict(int)
    conv_go:     dict[str, int] = defaultdict(int)
    conv_no_go:  dict[str, int] = defaultdict(int)
    conv_pend:   dict[str, int] = defaultdict(int)

    for row in rows:
        status = row.get("decision_status", "")
        cat    = _classify(status)
        pid    = row.get("profile_id", "") or "DESCONHECIDO"
        cid    = row.get("convenio_id", "") or "DESCONHECIDO"

        if cat == "GO":
            g_go += 1
        elif cat == "NO_GO":
            g_no_go += 1
        elif cat == "PENDENCIA":
            g_pend += 1
        else:
            g_outro += 1

        prof_total[pid] += 1
        if cat == "GO":
            prof_go[pid] += 1
        elif cat == "NO_GO":
            prof_no_go[pid] += 1
            for t in (row.get("motivos_no_go") or "").split(";"):
                t = t.strip()
                if t:
                    prof_motivos[pid].append(t)
        elif cat == "PENDENCIA":
            prof_pend[pid] += 1
            for t in (row.get("pendencias_detectadas") or "").split(";"):
                t = t.strip()
                if t:
                    prof_pendencias[pid].append(t)

        conv_total[cid] += 1
        if cat == "GO":
            conv_go[cid] += 1
        elif cat == "NO_GO":
            conv_no_go[cid] += 1
        elif cat == "PENDENCIA":
            conv_pend[cid] += 1

    global_summary = {
        "total_geral_casos":   total,
        "total_go":            g_go,
        "total_no_go":         g_no_go,
        "total_pendencia":     g_pend,
        "go_rate_global":      _rate(g_go,   total),
        "no_go_rate_global":   _rate(g_no_go, total),
        "pendencia_rate_global": _rate(g_pend, total),
    }

    by_profile: list[dict] = []
    for pid in sorted(prof_total.keys()):
        t = prof_total[pid]
        top_m = Counter(prof_motivos[pid]).most_common(1)
        top_p = Counter(prof_pendencias[pid]).most_common(1)
        by_profile.append({
            "profile_id":      pid,
            "total_casos":     t,
            "go_count":        prof_go[pid],
            "no_go_count":     prof_no_go[pid],
            "pendencia_count": prof_pend[pid],
            "go_rate":         _rate(prof_go[pid],   t),
            "no_go_rate":      _rate(prof_no_go[pid], t),
            "pendencia_rate":  _rate(prof_pend[pid],  t),
            "top_motivo_no_go": top_m[0][0] if top_m else "",
            "top_pendencia":   top_p[0][0] if top_p else "",
        })

    by_convenio: list[dict] = []
    for cid in sorted(conv_total.keys()):
        t = conv_total[cid]
        by_convenio.append({
            "convenio_id":     cid,
            "total_casos":     t,
            "go_count":        conv_go[cid],
            "no_go_count":     conv_no_go[cid],
            "pendencia_count": conv_pend[cid],
            "go_rate":         _rate(conv_go[cid],   t),
            "no_go_rate":      _rate(conv_no_go[cid], t),
            "pendencia_rate":  _rate(conv_pend[cid],  t),
        })

    return {
        "global":      global_summary,
        "by_profile":  by_profile,
        "by_convenio": by_convenio,
    }


# ─── Reescrita idempotente de 24_INSIGHTS ────────────────────────────────────

def _build_sheet_matrix(metrics: dict[str, Any]) -> list[list[str]]:
    """Monta a matriz completa de células para 24_INSIGHTS."""
    matrix: list[list[str]] = []
    g = metrics["global"]

    matrix.append(["=== BLOCO A — RESUMO GLOBAL ===", "", ""])
    matrix.append(["metrica", "valor", ""])
    for metric, value in [
        ("total_geral_casos",    str(g["total_geral_casos"])),
        ("total_go",             str(g["total_go"])),
        ("total_no_go",          str(g["total_no_go"])),
        ("total_pendencia",      str(g["total_pendencia"])),
        ("go_rate_global",       g["go_rate_global"]),
        ("no_go_rate_global",    g["no_go_rate_global"]),
        ("pendencia_rate_global", g["pendencia_rate_global"]),
    ]:
        matrix.append([metric, value, ""])

    matrix.append(["", "", ""])
    matrix.append(["=== BLOCO B — POR PROFILE_ID ===", "", "", "", "", "", "", "", "", ""])
    b_headers = [
        "profile_id", "total_casos", "go_count", "no_go_count", "pendencia_count",
        "go_rate", "no_go_rate", "pendencia_rate", "top_motivo_no_go", "top_pendencia",
    ]
    matrix.append(b_headers)
    for r in metrics["by_profile"]:
        matrix.append([str(r.get(h, "")) for h in b_headers])

    matrix.append([""] * len(b_headers))
    matrix.append(["=== BLOCO C — POR CONVENIO_ID ===", "", "", "", "", "", "", ""])
    c_headers = [
        "convenio_id", "total_casos", "go_count", "no_go_count", "pendencia_count",
        "go_rate", "no_go_rate", "pendencia_rate",
    ]
    matrix.append(c_headers)
    for r in metrics["by_convenio"]:
        matrix.append([str(r.get(h, "")) for h in c_headers])

    matrix.append([""] * len(c_headers))
    import datetime
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    matrix.append([f"atualizado_em: {ts}", "", "", "", "", "", "", ""])
    return matrix


def _write_insights_sheet(matrix: list[list[str]]) -> None:
    """Limpa 24_INSIGHTS e reescreve toda a matriz. Idempotente por design."""
    from .sheets_client import ensure_worksheet

    n_rows = max(len(matrix) + 20, 100)
    n_cols = max(max(len(r) for r in matrix) if matrix else 10, 10)

    ws = ensure_worksheet(INSIGHTS_SHEET, rows=n_rows, cols=n_cols)
    ws.clear()

    for row in matrix:
        while len(row) < n_cols:
            row.append("")

    start_cell = "A1"
    end_cell   = f"{chr(ord('A') + n_cols - 1)}{len(matrix)}"
    ws.update(f"{start_cell}:{end_cell}", matrix)

    try:
        for i, row in enumerate(matrix, start=1):
            if row and str(row[0]).startswith("==="):
                ws.format(f"A{i}:{chr(ord('A') + n_cols - 1)}{i}",
                          {"textFormat": {"bold": True}})
    except Exception as fmt_exc:
        logger.debug("_write_insights_sheet: formatação opcional falhou — %s", fmt_exc)


# ─── Interface pública ────────────────────────────────────────────────────────

def refresh_insights_sheet() -> None:
    """
    Recalcula e reescreve 24_INSIGHTS a partir dos dados de 23_FEEDBACK_LOOP.

    Chamado APÓS log_feedback() no decision_routes — nunca lança exceção.
    Idempotente: pode ser chamado múltiplas vezes sem criar duplicatas.
    Graceful degradation total: toda falha é logada, nunca propagada.
    """
    try:
        rows = _read_feedback_rows()
        if not rows:
            logger.info("refresh_insights_sheet: nenhuma linha em '%s' — 24_INSIGHTS não atualizado", FEEDBACK_SHEET)
            return

        metrics = _compute_metrics(rows)
        matrix  = _build_sheet_matrix(metrics)
        _write_insights_sheet(matrix)

        g = metrics["global"]
        logger.info(
            "refresh_insights_sheet: OK — total=%d GO=%d NO_GO=%d PENDENCIA=%d profiles=%d convenios=%d",
            g["total_geral_casos"], g["total_go"], g["total_no_go"], g["total_pendencia"],
            len(metrics["by_profile"]), len(metrics["by_convenio"]),
        )

    except Exception as exc:
        logger.error("refresh_insights_sheet: falha inesperada — %s", exc)
