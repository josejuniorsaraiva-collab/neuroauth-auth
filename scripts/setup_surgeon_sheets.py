#!/usr/bin/env python3
"""
scripts/setup_surgeon_sheets.py
NEUROAUTH — Cria/valida abas CIRURGIOES, REDUCAO_AUXILIAR e PRODUCAO no Sheets.

Idempotente:
  - Se aba já existir com header correto → apenas reporta "OK".
  - Se aba existir sem header → grava header (sem apagar dados).
  - Se aba não existir → cria, escreve título/subtítulo/header e semeia dados.

Padrão do projeto: head=3 (linha 1=título, 2=subtítulo, 3=headers, 4+=dados).

Uso:
  python scripts/setup_surgeon_sheets.py
  python scripts/setup_surgeon_sheets.py --dry-run   # apenas valida, não escreve
"""
from __future__ import annotations

import sys
import os
import argparse

# Adicionar raiz do projeto ao sys.path para importar repositórios
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from repositories.sheets_client import (
    ensure_worksheet,
    get_header_row,
    append_row_by_header,
    _retry_on_quota,
    _get_workbook,
)

# ─── Configuração das abas ────────────────────────────────────────────────────

SHEET_CONFIG = {
    "CIRURGIOES": {
        "title":    "NEUROAUTH — Tabela de Cirurgiões",
        "subtitle": "Referência de cirurgiões cadastrados e ativos | ativo=TRUE para participar de casos",
        "header":   [
            "cirurgiao_id", "nome_canonico", "nome_exibicao",
            "crm", "especialidade", "ativo", "observacao",
        ],
        "seed": [
            {
                "cirurgiao_id":  "CIR_001",
                "nome_canonico": "Jose Correia Saraiva Junior",
                "nome_exibicao": "Dr. José Correia Jr.",
                "crm":           "CRM-CE 18227",
                "especialidade": "Neurocirurgia/Neurointervenção",
                "ativo":         "TRUE",
                "observacao":    "",
            },
            {
                "cirurgiao_id":  "CIR_002",
                "nome_canonico": "Diego Ramon",
                "nome_exibicao": "Dr. Diego Ramon",
                "crm":           "CRM-CE XXXXX",
                "especialidade": "Neurocirurgia",
                "ativo":         "TRUE",
                "observacao":    "",
            },
            {
                "cirurgiao_id":  "CIR_003",
                "nome_canonico": "Macario Filho",
                "nome_exibicao": "Dr. Macário Filho",
                "crm":           "CRM-CE XXXXX",
                "especialidade": "Neurocirurgia",
                "ativo":         "TRUE",
                "observacao":    "",
            },
        ],
    },

    "REDUCAO_AUXILIAR": {
        "title":    "NEUROAUTH — Redução de Auxiliares (CBHPM/AMB)",
        "subtitle": "Percentuais de redução por operadora/porte/ordem — DEFAULT=fallback global | override: adicione linha com operadora específica",
        "header":   [
            "operadora", "porte_procedimento", "ordem_auxiliar",
            "percentual_reducao", "vigencia_inicio", "vigencia_fim", "fonte",
        ],
        "seed": [
            # Defaults CBHPM/AMB — fallback global
            {
                "operadora":           "DEFAULT",
                "porte_procedimento":  "TODOS",
                "ordem_auxiliar":      "1",
                "percentual_reducao":  "0.30",
                "vigencia_inicio":     "2024-01-01",
                "vigencia_fim":        "",
                "fonte":               "CBHPM/AMB",
            },
            {
                "operadora":           "DEFAULT",
                "porte_procedimento":  "TODOS",
                "ordem_auxiliar":      "2",
                "percentual_reducao":  "0.20",
                "vigencia_inicio":     "2024-01-01",
                "vigencia_fim":        "",
                "fonte":               "CBHPM/AMB",
            },
            {
                "operadora":           "DEFAULT",
                "porte_procedimento":  "TODOS",
                "ordem_auxiliar":      "3",
                "percentual_reducao":  "0.20",
                "vigencia_inicio":     "2024-01-01",
                "vigencia_fim":        "",
                "fonte":               "CBHPM/AMB",
            },
        ],
    },

    "PRODUCAO": {
        "title":    "NEUROAUTH — Produção Paralela por Cirurgião",
        "subtitle": "Registro de produção individual por papel (PRINCIPAL/AUXILIAR_1/AUXILIAR_2/AUXILIAR_3)",
        "header":   [
            "caso_id", "cirurgiao_id", "papel", "ordem_auxiliar",
            "valor_base", "percentual_aplicado", "valor_calculado",
            "data_procedimento", "operadora",
            "status_autorizacao", "status_pagamento",
        ],
        "seed": [],  # só header — dados gerados via calcular_producao()
    },
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _write_rows_1_2_3(ws, title: str, subtitle: str, header: list[str], dry_run: bool) -> None:
    """Escreve as 3 primeiras linhas (título, subtítulo, header) de uma vez."""
    if dry_run:
        print(f"    [DRY-RUN] gravaria rows 1-3: {header}")
        return
    data = [[title], [subtitle], header]
    _retry_on_quota(lambda: ws.batch_update([{
        "range": f"A1:{_col_letter(len(header))}3",
        "values": data,
    }]))


def _col_letter(n: int) -> str:
    """Converte número de coluna (1-based) para letra Excel (A, B, ..., Z, AA...)."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _has_correct_header(current_header: list[str], expected_header: list[str]) -> bool:
    """Verifica se o header atual bate com o esperado (ignora maiúsculas/espaços extras)."""
    if len(current_header) < len(expected_header):
        return False
    actual = [h.strip().lower() for h in current_header[: len(expected_header)]]
    expected = [h.strip().lower() for h in expected_header]
    return actual == expected


# ─── Setup principal ─────────────────────────────────────────────────────────

def setup_sheets(dry_run: bool = False) -> dict:
    """
    Cria/valida todas as abas. Retorna dict com status de cada aba.
    """
    results = {}

    for sheet_name, cfg in SHEET_CONFIG.items():
        print(f"\n{'='*55}")
        print(f"  Aba: {sheet_name}")
        print(f"{'='*55}")

        created_fresh = False

        # 1. Garantir que a aba existe
        ws = ensure_worksheet(sheet_name, rows=2000, cols=len(cfg["header"]) + 5)

        # 2. Verificar header atual (linha 3)
        current_header = get_header_row(ws, head=3)

        if _has_correct_header(current_header, cfg["header"]):
            print(f"  ✓ Header OK: {current_header[: len(cfg['header'])]}")
            status = "OK_EXISTING"
        else:
            print(f"  ! Header ausente ou divergente:")
            print(f"    Encontrado : {current_header}")
            print(f"    Esperado   : {cfg['header']}")

            # Verificar se há dados além das 3 primeiras linhas
            all_vals = _retry_on_quota(lambda: ws.get_all_values())
            has_data = len(all_vals) > 3 and any(any(c for c in row) for row in all_vals[3:])

            if has_data:
                print(f"  ⚠ Aba tem {len(all_vals) - 3} linha(s) de dados — mantendo dados, apenas fixando header.")

            _write_rows_1_2_3(ws, cfg["title"], cfg["subtitle"], cfg["header"], dry_run)
            created_fresh = not has_data
            status = "HEADER_FIXED" if has_data else "CREATED"
            print(f"  ✓ Header gravado (rows 1-3).")

        # 3. Semente (só se aba acabou de ser criada E tem seed configurado)
        if cfg["seed"] and created_fresh:
            print(f"  → Semeando {len(cfg['seed'])} linha(s)...")
            if not dry_run:
                for row_data in cfg["seed"]:
                    append_row_by_header(ws, row_data, head=3)
                    print(f"    + {row_data.get('cirurgiao_id') or row_data.get('operadora') or '?'}")
            else:
                for row_data in cfg["seed"]:
                    print(f"    [DRY-RUN] + {row_data}")
            status = "CREATED_AND_SEEDED"
        elif cfg["seed"] and not created_fresh and status == "OK_EXISTING":
            # Verificar quantas linhas de dados existem
            all_vals = _retry_on_quota(lambda: ws.get_all_values())
            data_count = len(all_vals) - 3  # descontando as 3 linhas de header
            print(f"  ✓ Dados existentes: {data_count} linha(s) — seed ignorada (idempotente).")
        elif not cfg["seed"]:
            print(f"  ✓ Sem seed configurada (aba de produção — dados gerados dinamicamente).")

        # Contagem final
        all_vals = _retry_on_quota(lambda: ws.get_all_values())
        data_rows = max(0, len(all_vals) - 3)
        print(f"  → Status final: {status} | {data_rows} linha(s) de dados")
        results[sheet_name] = {"status": status, "data_rows": data_rows}

    return results


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup surgeon sheets no NEUROAUTH.")
    parser.add_argument("--dry-run", action="store_true", help="Apenas valida, não escreve no Sheets.")
    args = parser.parse_args()

    print("\nNEUROAUTH — setup_surgeon_sheets.py")
    print(f"Modo: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"Abas alvo: {list(SHEET_CONFIG.keys())}")

    try:
        results = setup_sheets(dry_run=args.dry_run)
    except Exception as exc:
        print(f"\n[ERRO FATAL] {type(exc).__name__}: {exc}")
        sys.exit(1)

    print("\n" + "=" * 55)
    print("RESUMO FINAL")
    print("=" * 55)
    all_ok = True
    for sheet_name, info in results.items():
        icon = "✓" if "OK" in info["status"] or "CREATED" in info["status"] or "FIXED" in info["status"] else "✗"
        print(f"  {icon} {sheet_name:25s} → {info['status']} ({info['data_rows']} linhas de dados)")
        if info["status"] not in ("OK_EXISTING", "CREATED", "CREATED_AND_SEEDED", "HEADER_FIXED"):
            all_ok = False

    print()
    if all_ok:
        print("STATUS: GO — todas as abas prontas.")
        sys.exit(0)
    else:
        print("STATUS: NO-GO — verificar erros acima.")
        sys.exit(1)
