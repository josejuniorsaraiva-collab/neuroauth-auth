#!/usr/bin/env python3
"""
scripts/test_precedencia.py
NEUROAUTH — Teste de precedência determinística de get_reducao().

Valida que UNIMED > DEFAULT quando ambos existem para a mesma ordem.
Roda sem conexão ao Sheets (mock inline).

Uso:
  python scripts/test_precedencia.py
"""
from __future__ import annotations

# ─── Mock da tabela REDUCAO_AUXILIAR ─────────────────────────────────────────
ROWS_MOCK = [
    {
        "operadora":           "UNIMED",
        "porte_procedimento":  "TODOS",
        "ordem_auxiliar":      "1",
        "percentual_reducao":  "0.25",
        "fonte":               "MANUAL",
        "vigencia_inicio":     "2024-01-01",
        "vigencia_fim":        "",
    },
    {
        "operadora":           "DEFAULT",
        "porte_procedimento":  "TODOS",
        "ordem_auxiliar":      "1",
        "percentual_reducao":  "0.30",
        "fonte":               "CBHPM/AMB",
        "vigencia_inicio":     "2024-01-01",
        "vigencia_fim":        "",
    },
]

_FALLBACK_PCT = 0.30


# ─── Lógica real de score (espelho de get_reducao) ────────────────────────────

def mock_get_reducao(rows: list[dict], operadora: str, porte: str, ordem: int) -> dict:
    """
    Réplica fiel da lógica de score de get_reducao() para teste isolado.
    Mesma precedência:
      operadora_específica + porte_específico   → 3
      operadora_específica + TODOS              → 2
      DEFAULT              + porte_específico   → 1
      DEFAULT              + TODOS              → 0
    """
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
        return {
            "percentual":          _FALLBACK_PCT,
            "fonte":               "FALLBACK_SEM_LINHA",
            "operadora_detectada": "DEFAULT",
            "porte_detectado":     "TODOS",
            "ordem_aplicada":      ordem_str,
            "vigencia_inicio":     "",
        }

    melhor_row, _ = max(candidatos, key=lambda x: x[1])

    pct_str = (melhor_row.get("percentual_reducao") or "").strip()
    try:
        pct = float(pct_str)
    except (ValueError, TypeError):
        pct = _FALLBACK_PCT

    return {
        "percentual":          pct,
        "fonte":               str(melhor_row.get("fonte", "DESCONHECIDA")),
        "operadora_detectada": str(melhor_row.get("operadora", "?")),
        "porte_detectado":     str(melhor_row.get("porte_procedimento", "?")),
        "ordem_aplicada":      str(melhor_row.get("ordem_auxiliar", ordem)),
        "vigencia_inicio":     str(melhor_row.get("vigencia_inicio", "")),
    }


# ─── Cenários de teste ────────────────────────────────────────────────────────

CENARIOS = [
    # (operadora,  porte,   ordem, pct_esperado, op_esperada,  descricao)
    ("UNIMED",   "TODOS",  1, 0.25, "UNIMED",   "UNIMED+TODOS  → score=2 vence DEFAULT+TODOS score=0"),
    ("BRADESCO", "TODOS",  1, 0.30, "DEFAULT",  "BRADESCO sem linha → cai em DEFAULT+TODOS score=0"),
    ("UNIMED",   "GRANDE", 1, 0.25, "UNIMED",   "UNIMED+GRANDE não existe → UNIMED+TODOS score=2 vence DEFAULT+TODOS score=0"),
]


def run():
    print("=" * 65)
    print("NEUROAUTH — Teste de Precedência REDUCAO_AUXILIAR")
    print("=" * 65)
    print()

    falhas = 0
    for operadora, porte, ordem, pct_esperado, op_esperada, desc in CENARIOS:
        resultado = mock_get_reducao(ROWS_MOCK, operadora, porte, ordem)
        pct_ok = abs(resultado["percentual"] - pct_esperado) < 1e-9
        op_ok  = resultado["operadora_detectada"].upper() == op_esperada.upper()
        ok     = pct_ok and op_ok
        status = "✅" if ok else "❌"

        if not ok:
            falhas += 1

        print(
            f"{status} operadora={operadora:<10} porte={porte:<8} "
            f"→ percentual={resultado['percentual']:.2f} "
            f"operadora_detectada={resultado['operadora_detectada']:<10} "
            f"fonte={resultado['fonte']}"
        )
        print(f"   esperado: percentual={pct_esperado:.2f} operadora={op_esperada}")
        print(f"   cenário:  {desc}")
        print()

    print("─" * 65)
    if falhas == 0:
        print(f"RESULTADO FINAL: ✅ TODOS OS {len(CENARIOS)} CENÁRIOS PASSARAM")
    else:
        print(f"RESULTADO FINAL: ❌ {falhas}/{len(CENARIOS)} CENÁRIO(S) FALHARAM")
    print("=" * 65)
    return falhas


if __name__ == "__main__":
    import sys
    falhas = run()
    sys.exit(0 if falhas == 0 else 1)
