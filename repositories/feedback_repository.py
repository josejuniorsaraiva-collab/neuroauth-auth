"""
NEUROAUTH — Feedback Repository  (Bloco 1: FEEDBACK ENGINE)
Versão: 1.0.0

Responsabilidade: gravar linha em 23_FEEDBACK_LOOP após cada decisão do motor.
Esta é a camada de memória operacional estruturada do NEUROAUTH.

Campos automáticos (motor preenche na hora):
    episodio_id, run_id, created_at, profile_id, convenio_id, procedimento,
    status_agendamento, decision_status, go_class, confidence_global,
    rigor_aplicado, n_bloqueios, n_pendencias, n_alertas,
    motivos_no_go, pendencias_detectadas

Campos a preencher depois (humano, integração de guia ou OCR):
    resultado_final, houve_glosa, tipo_glosa, motivo_negativa,
    houve_retrabalho, tempo_total_min, ajuste_realizado,
    observacao_operacional, sentimento_auditor, tipo_friccao,
    erro_manual_detectado, pontos_de_espera,
    necessidade_de_opme_extra, tempo_ate_autorizacao_horas

Princípio: acoplamento incremental — nunca altera motor, nunca interrompe resposta.
Graceful degradation total: toda falha é logada, nunca propagada.
Auto-cria a aba 23_FEEDBACK_LOOP se não existir.
"""
from __future__ import annotations

import datetime
import logging

logger = logging.getLogger("neuroauth.feedback")

# ─── Schema da aba 23_FEEDBACK_LOOP ──────────────────────────────────────────
# Ordem define a sequência das colunas criadas automaticamente.

FEEDBACK_SHEET   = "23_FEEDBACK_LOOP"
FEEDBACK_HEADERS = [
    # Identificação
    "episodio_id",
    "run_id",
    "created_at",
    # Perfil do caso
    "profile_id",
    "convenio_id",
    "procedimento",
    "status_agendamento",
    # Resultado do motor
    "decision_status",
    "go_class",
    "confidence_global",
    "rigor_aplicado",
    # Contadores de saída do motor
    "n_bloqueios",
    "n_pendencias",
    "n_alertas",
    # Detalhes de problemas detectados
    "motivos_no_go",
    "pendencias_detectadas",
    # ── Campos a preencher depois ─────────────────────────────────────────────
    # Resultado real (pós-autorização pelo convênio)
    "resultado_final",          # AUTORIZADO | NEGADO | PARCIAL | PENDENTE
    "houve_glosa",              # SIM | NAO
    "tipo_glosa",               # CODIGO | MATERIAL | JUSTIFICATIVA | PRAZO | OUTRO
    "motivo_negativa",          # texto livre
    # Custo operacional
    "houve_retrabalho",         # SIM | NAO
    "tempo_total_min",          # número — minutos do preenchimento até encerramento
    "ajuste_realizado",         # texto livre — o que foi corrigido
    "observacao_operacional",   # texto livre — observação da secretária/auditor
    # Campos de alto valor (alimentação gradual)
    "sentimento_auditor",       # FACIL | NORMAL | DIFICIL
    "tipo_friccao",             # CAMPO_FALTANDO | REGRA_ESTRANHA | SISTEMA_LENTO | OUTRO
    "erro_manual_detectado",    # SIM | NAO
    "pontos_de_espera",         # texto livre — onde o processo ficou travado
    "necessidade_de_opme_extra",  # SIM | NAO
    "tempo_ate_autorizacao_horas",  # número
]


# ─── Worksheet helper ─────────────────────────────────────────────────────────

def _get_feedback_sheet():
    """
    Retorna a aba 23_FEEDBACK_LOOP.
    Cria a aba com cabeçalho completo se não existir.
    Usa ensure_worksheet do sheets_client para criação on-demand.
    """
    from .sheets_client import ensure_worksheet

    ws = ensure_worksheet(FEEDBACK_SHEET, rows=2000, cols=len(FEEDBACK_HEADERS))

    # Verificar se cabeçalho já existe na linha 1
    try:
        row1 = ws.row_values(1)
    except Exception:
        row1 = []

    if not row1 or row1[0] != "episodio_id":
        # Aba recém-criada ou vazia — gravar cabeçalho
        ws.update("A1", [FEEDBACK_HEADERS])
        try:
            # Negrito no cabeçalho (opcional — não bloqueia se falhar)
            import gspread.utils as _gu
            last_col = _gu.rowcol_to_a1(1, len(FEEDBACK_HEADERS)).rstrip("1")
            ws.format(f"A1:{last_col}1", {"textFormat": {"bold": True}})
        except Exception:
            pass
        logger.info("_get_feedback_sheet: cabeçalho gravado em '%s'", FEEDBACK_SHEET)

    return ws


# ─── Interface pública ────────────────────────────────────────────────────────

def log_feedback(
    episodio_id: str,
    run_id: str,
    raw_case: dict,
    result: dict,
) -> None:
    """
    Grava linha de memória operacional em 23_FEEDBACK_LOOP.

    Chamado APÓS log_case_result e suggest_gap_candidates no decision_routes.
    Nunca lança exceção — graceful degradation total.

    Args:
        episodio_id: ID do episódio (ex: EP_72F179DE76)
        run_id:      ID do run de decisão (ex: RUN_36CF33C67B8C)
        raw_case:    payload original da requisição
        result:      output completo do motor
    """
    try:
        ws = _get_feedback_sheet()

        decision_status = result.get("decision_status", "")
        bloqueios  = result.get("bloqueios",  [])
        pendencias = result.get("pendencias", [])
        alertas    = result.get("alertas",    [])

        # Motivos de NO_GO — extrai código/regra de cada bloqueio
        motivos_no_go = "; ".join(
            b.get("regra", b.get("codigo", b.get("campo", "")))
            for b in bloqueios
        ) if bloqueios else ""

        # Pendências detectadas — extrai campo/regra
        pendencias_detectadas = "; ".join(
            p.get("campo", p.get("regra", p.get("codigo", "")))
            for p in pendencias
        ) if pendencias else ""

        # Procedimento: campos_inferidos → PROC_NOME → fallback profile_id
        procedimento = ""
        for ci in result.get("campos_inferidos", []):
            if ci.get("campo") == "PROC_NOME":
                procedimento = ci.get("valor", "")
                break
        if not procedimento:
            for ap in result.get("autopreenchimentos", []):
                if ap.get("campo") == "PROC_NOME":
                    procedimento = ap.get("valor", "")
                    break
        if not procedimento:
            procedimento = raw_case.get("profile_id", "")

        now = datetime.datetime.utcnow().isoformat() + "Z"

        row = {
            "episodio_id":          episodio_id,
            "run_id":               run_id,
            "created_at":           now,
            "profile_id":           raw_case.get("profile_id", ""),
            "convenio_id":          raw_case.get("convenio_id", raw_case.get("convenio", "")),
            "procedimento":         procedimento,
            "status_agendamento":   raw_case.get("status_agendamento", ""),
            "decision_status":      decision_status,
            "go_class":             result.get("go_class", result.get("go_decision", "")),
            "confidence_global":    str(result.get("confidence_global", "")),
            # RIGOR_STANDARD por padrão — será ELEVATED/HARD quando Decision Engine 2.0 existir
            "rigor_aplicado":       "STANDARD",
            "n_bloqueios":          str(len(bloqueios)),
            "n_pendencias":         str(len(pendencias)),
            "n_alertas":            str(len(alertas)),
            "motivos_no_go":        motivos_no_go,
            "pendencias_detectadas": pendencias_detectadas,
            # Campos a preencher depois — inicializados vazios
            "resultado_final":              "",
            "houve_glosa":                  "",
            "tipo_glosa":                   "",
            "motivo_negativa":              "",
            "houve_retrabalho":             "",
            "tempo_total_min":              "",
            "ajuste_realizado":             "",
            "observacao_operacional":       "",
            "sentimento_auditor":           "",
            "tipo_friccao":                 "",
            "erro_manual_detectado":        "",
            "pontos_de_espera":             "",
            "necessidade_de_opme_extra":    "",
            "tempo_ate_autorizacao_horas":  "",
        }

        # Montar linha na ordem exata dos headers
        row_values = [row.get(h, "") for h in FEEDBACK_HEADERS]
        ws.append_row(row_values, value_input_option="USER_ENTERED")

        logger.info(
            "log_feedback: gravado → %s status=%s bloqueios=%d pendencias=%d",
            episodio_id, decision_status, len(bloqueios), len(pendencias),
        )

    except Exception as exc:
        logger.error(
            "log_feedback: falha ao gravar — episodio=%s erro=%s",
            episodio_id, exc,
        )
