"""
app/services/sheets_store.py
Persistência em Google Sheets.

Correção da quebra 4:
- Não assume posição fixa de colunas em 22_EPISODIOS
- Faz header discovery na linha HEADER_ROW para encontrar índices reais
- Apenas 21_DECISION_RUNS usa append_row (estrutura controlada por nós)
"""

import gspread
import logging
from google.oauth2.service_account import Credentials
from app.models.decide import DecideRequest, DecideResponse
from app.core.config import settings
from datetime import datetime
import time

logger = logging.getLogger("neuroauth.sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

TAB_DECISION_RUNS = "21_DECISION_RUNS"
TAB_EPISODIOS     = "22_EPISODIOS"

# Linha de cabeçalho real em 22_EPISODIOS (1-indexed)
# Ajustar se a estrutura da aba usar linha diferente
EPISODIOS_HEADER_ROW = 3

# Colunas que o engine precisa encontrar em 22_EPISODIOS
COL_EPISODIO_ID       = "episodio_id"
COL_DECISION_STATUS   = "decision_status"
COL_LAST_RUN_ID       = "decision_run_id"   # alinhado com header real de 22_EPISODIOS
COL_UPDATED_AT        = "updated_at"


def _get_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS, scopes=SCOPES
    )
    return gspread.authorize(creds)



# ── CONFIGURAÇÃO DE RETRY ─────────────────────────────────────────────────
# Latência real do Sheets API: escrita pode demorar até ~5s para ser visível
# Retry evita classificar como BLOQUEADO algo que é apenas propagação de cache
RETRY_DELAYS_SEC = [2, 5, 10]  # aguardar 2s, depois 5s, depois 10s antes de declarar falha


def _verify_decision_run_written(
    ss: gspread.Spreadsheet,
    decision_run_id: str,
    max_retries: int = 3,
) -> bool:
    """
    Verifica se o decision_run_id apareceu em 21_DECISION_RUNS.
    Usa retry com backoff para acomodar latência do Sheets API.
    Retorna True se encontrado dentro das tentativas, False após esgotar.
    """
    for attempt, delay in enumerate(RETRY_DELAYS_SEC[:max_retries], start=1):
        try:
            ws = ss.worksheet(TAB_DECISION_RUNS)
            run_ids = ws.col_values(1)  # coluna A = decision_run_id
            if decision_run_id in run_ids:
                logger.info(
                    f"[sheets_store] VERIFY_OK 21_DECISION_RUNS "
                    f"run={decision_run_id} encontrado na tentativa {attempt}"
                )
                return True
        except Exception as e:
            logger.warning(f"[sheets_store] VERIFY_ERR tentativa {attempt}: {e}")

        if attempt < max_retries:
            logger.info(
                f"[sheets_store] VERIFY_RETRY run={decision_run_id} "
                f"não encontrado ainda — aguardando {delay}s (tentativa {attempt}/{max_retries})"
            )
            time.sleep(delay)

    logger.error(
        f"[sheets_store] VERIFY_FAIL 21_DECISION_RUNS "
        f"run={decision_run_id} não encontrado após {max_retries} tentativas"
    )
    return False


def _verify_episodio_updated(
    ss: gspread.Spreadsheet,
    episodio_id: str,
    expected_status: str,
    max_retries: int = 3,
) -> bool:
    """
    Verifica se decision_status foi atualizado em 22_EPISODIOS para o episodio_id.
    Usa retry com backoff para acomodar latência do Sheets API.
    """
    for attempt, delay in enumerate(RETRY_DELAYS_SEC[:max_retries], start=1):
        try:
            ws = ss.worksheet(TAB_EPISODIOS)
            header_row = ws.row_values(EPISODIOS_HEADER_ROW)
            col_map = {
                name.strip().lower(): idx + 1
                for idx, name in enumerate(header_row) if name.strip()
            }

            if COL_EPISODIO_ID not in col_map or COL_DECISION_STATUS not in col_map:
                logger.warning(f"[sheets_store] VERIFY_ERR colunas não encontradas em 22_EPISODIOS")
                break

            ep_col = col_map[COL_EPISODIO_ID]
            st_col = col_map[COL_DECISION_STATUS]

            ep_ids = ws.col_values(ep_col)
            if episodio_id in ep_ids:
                row_idx = ep_ids.index(episodio_id) + 1
                status_val = ws.cell(row_idx, st_col).value
                if status_val == expected_status:
                    logger.info(
                        f"[sheets_store] VERIFY_OK 22_EPISODIOS "
                        f"ep={episodio_id} status='{status_val}' encontrado tentativa {attempt}"
                    )
                    return True
                else:
                    logger.info(
                        f"[sheets_store] VERIFY_MISMATCH ep={episodio_id} "
                        f"status encontrado='{status_val}' esperado='{expected_status}' "
                        f"tentativa {attempt}"
                    )
        except Exception as e:
            logger.warning(f"[sheets_store] VERIFY_ERR 22_EPISODIOS tentativa {attempt}: {e}")

        if attempt < max_retries:
            logger.info(
                f"[sheets_store] VERIFY_RETRY ep={episodio_id} "
                f"aguardando {delay}s (tentativa {attempt}/{max_retries})"
            )
            time.sleep(delay)

    logger.error(
        f"[sheets_store] VERIFY_FAIL 22_EPISODIOS "
        f"ep={episodio_id} status='{expected_status}' não confirmado após {max_retries} tentativas"
    )
    return False


def verify_persistence(
    req: DecideRequest,
    res: DecideResponse,
    max_retries: int = 3,
) -> dict:
    """
    Verifica post-escrita com retry:
    1. decision_run_id apareceu em 21_DECISION_RUNS
    2. decision_status atualizado em 22_EPISODIOS
    3. correlação episodio_id ↔ decision_run_id é válida

    Retorna dict com resultado de cada check + veredicto final.
    """
    result = {
        "run_id": res.decision_run_id,
        "episodio_id": res.episodio_id,
        "check_21_decision_runs": False,
        "check_22_episodios_status": False,
        "check_22_episodios_correlacao": False,
        "veredicto": "BLOQUEADO",
        "tentativas": max_retries,
        "detalhes": [],
    }

    # PRE_ANALISE_APENAS: não tem linha em 21_DECISION_RUNS — só verifica episódio
    if res.classification == "PRE_ANALISE_APENAS":
        result["check_21_decision_runs"] = True  # não aplicável
        result["detalhes"].append("PRE_ANALISE_APENAS: check 21_DECISION_RUNS não aplicável")

    try:
        gc = _get_client()
        ss = gc.open_by_key(settings.SPREADSHEET_ID)

        # CHECK 1 — 21_DECISION_RUNS
        if res.classification != "PRE_ANALISE_APENAS":
            ok1 = _verify_decision_run_written(ss, res.decision_run_id, max_retries)
            result["check_21_decision_runs"] = ok1
            result["detalhes"].append(
                f"CHECK 1 (21_DECISION_RUNS): {'OK' if ok1 else 'FALHOU'} — run_id={res.decision_run_id}"
            )

        # CHECK 2 — 22_EPISODIOS status
        ok2 = _verify_episodio_updated(ss, res.episodio_id, res.decision_status, max_retries)
        result["check_22_episodios_status"] = ok2
        result["detalhes"].append(
            f"CHECK 2 (22_EPISODIOS status): {'OK' if ok2 else 'FALHOU'} — "
            f"ep={res.episodio_id} status={res.decision_status}"
        )

        # CHECK 3 — Correlação: o run_id gravado no episódio bate com o gerado
        try:
            ws_ep = ss.worksheet(TAB_EPISODIOS)
            header_row = ws_ep.row_values(EPISODIOS_HEADER_ROW)
            col_map = {n.strip().lower(): i+1 for i, n in enumerate(header_row) if n.strip()}

            if COL_EPISODIO_ID in col_map and COL_LAST_RUN_ID in col_map:
                ep_ids = ws_ep.col_values(col_map[COL_EPISODIO_ID])
                if res.episodio_id in ep_ids:
                    row_idx = ep_ids.index(res.episodio_id) + 1
                    run_id_gravado = ws_ep.cell(row_idx, col_map[COL_LAST_RUN_ID]).value
                    correlacao_ok = run_id_gravado == res.decision_run_id
                    result["check_22_episodios_correlacao"] = correlacao_ok
                    result["detalhes"].append(
                        f"CHECK 3 (correlação IDs): {'OK' if correlacao_ok else 'DIVERGÊNCIA'} — "
                        f"gravado='{run_id_gravado}' gerado='{res.decision_run_id}'"
                    )
                else:
                    result["detalhes"].append(f"CHECK 3: episodio_id não encontrado para correlação")
        except Exception as e:
            result["detalhes"].append(f"CHECK 3 erro: {e}")

        # Veredicto final
        checks_criticos = [
            result["check_21_decision_runs"],
            result["check_22_episodios_status"],
        ]
        if all(checks_criticos):
            result["veredicto"] = "OK" if result["check_22_episodios_correlacao"] else "OK_SEM_CORRELACAO"
        else:
            result["veredicto"] = "BLOQUEADO"

    except Exception as e:
        result["detalhes"].append(f"ERRO GERAL verify_persistence: {type(e).__name__}: {str(e)[:200]}")
        result["veredicto"] = "BLOQUEADO"

    logger.info(
        f"[sheets_store] VERIFY_PERSISTENCE veredicto={result['veredicto']} "
        f"run={res.decision_run_id} ep={res.episodio_id}"
    )
    return result


def persist_decision(req: DecideRequest, res: DecideResponse) -> bool:
    """
    1. Append em 21_DECISION_RUNS (estrutura fixa — controlada pelo sistema)
    2. Update em 22_EPISODIOS (estrutura existente — descoberta dinamicamente)
    Nunca lança exceção para o caller.

    PRE_ANALISE_APENAS: persiste apenas pendências e bloqueios em 22_EPISODIOS.
    Não cria linha em 21_DECISION_RUNS (sem score = sem decisão auditável).
    """
    try:
        gc = _get_client()
        ss = gc.open_by_key(settings.SPREADSHEET_ID)

        # PRE_ANALISE_APENAS: só registra estado no episódio, não cria decision_run
        if res.classification == "PRE_ANALISE_APENAS":
            _update_episodio(ss, res.episodio_id, "PRE_ANALISE", res.decision_run_id)
            logger.info(
                f"[sheets_store] PRE_ANALISE_APENAS ep={res.episodio_id} "
                f"run={res.decision_run_id} — sem score, sem linha em 21_DECISION_RUNS"
            )
            return True

        _append_decision_run(ss, req, res)
        logger.info(
            f"[sheets_store] OK 21_DECISION_RUNS append: "
            f"run={res.decision_run_id} ep={res.episodio_id} "
            f"cls={res.classification} score={res.score}"
        )

        _update_episodio(ss, res.episodio_id, res.decision_status, res.decision_run_id)
        logger.info(
            f"[sheets_store] OK 22_EPISODIOS update: "
            f"ep={res.episodio_id} status={res.decision_status} run={res.decision_run_id}"
        )
        return True

    except Exception as e:
        logger.error(f"[sheets_store] Falha: {type(e).__name__}: {str(e)[:200]}")
        return False


def _append_decision_run(
    ss: gspread.Spreadsheet,
    req: DecideRequest,
    res: DecideResponse,
) -> None:
    """
    Append linha em 21_DECISION_RUNS.
    Estrutura controlada por nós — sem ambiguidade de colunas.
    Se a aba não existir, cria com cabeçalho.
    """
    try:
        ws = ss.worksheet(TAB_DECISION_RUNS)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=TAB_DECISION_RUNS, rows=1000, cols=28)
        ws.append_row([
            "decision_run_id", "episodio_id", "timestamp",
            "classification", "decision_status", "score",
            "risco_glosa", "justificativa",
            "pendencias", "pontos_frageis",
            "cid_principal", "procedimento", "convenio", "crm_solicitante",
            "score_clinico", "camada1", "camada3_risco",
            "gate_reason", "tempo_ms", "versao_motor",
            "v2_trace_json",
            # v1.0 clinical enrichment
            "schema_version", "glosa_probability", "trace_id",
            "engine_version", "raciocinio_clinico",
            "fundamento_regulatorio", "texto_convenio",
        ])

    # v1.3: campos extras com fallback seguro para retrocompat
    score_clinico = getattr(res, "score_clinico", None) or ""
    camada1 = getattr(res, "camada1", "") or ""
    camada3_risco = getattr(res, "camada3_risco", "") or ""
    gate_reason = getattr(res, "gate_reason", "") or ""
    tempo_ms = getattr(res, "tempo_execucao_ms", None) or ""
    versao_motor = getattr(res, "motor_version", "1.0")

    # v2.0: trace completo do motor (JSON compacto — campo extra)
    v2_trace = ""
    try:
        raw_trace = getattr(res, "v2_trace", None)
        if raw_trace:
            import json as _json
            v2_trace = _json.dumps(raw_trace, ensure_ascii=False)[:5000]
    except Exception:
        pass

    # ── Clinical enrichment v1.0: extrair campos do clinical_v1 ──────────
    # Fonte: v2_trace → clinical_v1 (injetado pelo decision_engine_v1.py)
    # Fallback seguro: se não existir, grava vazio (nunca quebra o append)
    _cv1 = {}
    try:
        _raw_v2 = getattr(res, "v2_trace", None) or {}
        _cv1 = _raw_v2.get("clinical_v1", {}) or {}
    except Exception:
        pass

    cv1_schema_version   = _cv1.get("schema_version", "") or ""
    cv1_glosa_prob       = _cv1.get("glosa_probability", "") if _cv1.get("glosa_probability") is not None else ""
    cv1_trace_id         = _cv1.get("trace_id", "") or ""
    cv1_engine_version   = versao_motor

    _sj = _cv1.get("structured_justification", {}) or {}
    cv1_raciocinio       = (_sj.get("raciocinio_clinico", "") or "")[:1200]
    cv1_fundamento       = (_sj.get("fundamento_regulatorio", "") or "")[:1200]
    cv1_texto_convenio   = (_sj.get("texto_convenio", "") or "")[:1200]

    ws.append_row([
        res.decision_run_id,
        res.episodio_id,
        res.timestamp,
        res.classification,
        res.decision_status,
        res.score,
        res.risco_glosa,
        res.justificativa[:500],
        " | ".join(res.pendencias),
        " | ".join(res.pontos_frageis),
        req.cid_principal,
        req.procedimento,
        req.convenio,
        req.crm,
        score_clinico,
        camada1,
        camada3_risco,
        gate_reason,
        tempo_ms,
        versao_motor,
        v2_trace,
        # ── v1.0 clinical enrichment (colunas 22-28) ──
        cv1_schema_version,
        cv1_glosa_prob,
        cv1_trace_id,
        cv1_engine_version,
        cv1_raciocinio,
        cv1_fundamento,
        cv1_texto_convenio,
    ], value_input_option="USER_ENTERED")


def _update_episodio(
    ss: gspread.Spreadsheet,
    episodio_id: str,
    decision_status: str,
    decision_run_id: str,
) -> None:
    """
    Atualiza 22_EPISODIOS por header discovery — não assume posição de coluna.

    Fluxo:
    1. Ler linha EPISODIOS_HEADER_ROW para descobrir índices reais
    2. Encontrar episodio_id na coluna correta
    3. Atualizar células pelo índice descoberto
    4. Se episodio_id não existe → append nova linha
    """
    ws = ss.worksheet(TAB_EPISODIOS)

    # 1. Descobrir mapa de colunas pelo cabeçalho
    header_row = ws.row_values(EPISODIOS_HEADER_ROW)
    col_map = {name.strip().lower(): idx + 1 for idx, name in enumerate(header_row) if name.strip()}

    # Verificar colunas obrigatórias
    missing = [c for c in [COL_EPISODIO_ID, COL_DECISION_STATUS] if c not in col_map]
    if missing:
        logger.error(
            f"[sheets_store] 22_EPISODIOS: colunas ausentes: {missing}. "
            f"Cabeçalho encontrado na linha {EPISODIOS_HEADER_ROW}: {header_row}"
        )
        return

    ep_col_idx = col_map[COL_EPISODIO_ID]

    # 2. Encontrar linha do episodio_id (busca a partir de EPISODIOS_HEADER_ROW + 1)
    all_ep_ids = ws.col_values(ep_col_idx)
    try:
        # col_values é 1-indexed na posição, mas lista é 0-indexed
        row_idx = all_ep_ids.index(episodio_id) + 1
    except ValueError:
        # Episódio não encontrado → criar linha nova
        logger.info(f"[sheets_store] episodio_id '{episodio_id}' não encontrado → append")
        new_row = [""] * len(header_row)
        new_row[ep_col_idx - 1] = episodio_id
        if COL_DECISION_STATUS in col_map:
            new_row[col_map[COL_DECISION_STATUS] - 1] = decision_status
        if COL_LAST_RUN_ID in col_map:
            new_row[col_map[COL_LAST_RUN_ID] - 1] = decision_run_id
        if COL_UPDATED_AT in col_map:
            new_row[col_map[COL_UPDATED_AT] - 1] = datetime.utcnow().isoformat()
        ws.append_row(new_row, value_input_option="USER_ENTERED")
        return

    # 3. Atualizar apenas as células necessárias
    updates = []
    now_iso = datetime.utcnow().isoformat()

    if COL_DECISION_STATUS in col_map:
        updates.append(gspread.Cell(row_idx, col_map[COL_DECISION_STATUS], decision_status))
    if COL_LAST_RUN_ID in col_map:
        updates.append(gspread.Cell(row_idx, col_map[COL_LAST_RUN_ID], decision_run_id))
    if COL_UPDATED_AT in col_map:
        updates.append(gspread.Cell(row_idx, col_map[COL_UPDATED_AT], now_iso))

    if updates:
        ws.update_cells(updates, value_input_option="USER_ENTERED")
    else:
        logger.warning(f"[sheets_store] Nenhuma coluna atualizável encontrada em 22_EPISODIOS.")
