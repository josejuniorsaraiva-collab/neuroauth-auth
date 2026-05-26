#!/usr/bin/env python3
"""
scripts/migrate_legacy_cases.py
NEUROAUTH — Backfill de cirurgião principal em casos legados.

Comportamento:
  - Varre 22_EPISODIOS linha por linha.
  - Se cirurgiao_principal_id estiver vazio → preenche com CIR_001.
  - Marca preenchimento_retroativo=TRUE na mesma linha.
  - Idempotente: linhas já preenchidas são ignoradas.

Pré-requisito:
  - A aba 22_EPISODIOS deve ter as colunas:
      cirurgiao_principal_id, preenchimento_retroativo
  - Essas colunas podem não existir ainda; o script detecta e avisa.

Uso:
  python scripts/migrate_legacy_cases.py
  python scripts/migrate_legacy_cases.py --dry-run
  python scripts/migrate_legacy_cases.py --cir-id CIR_002   # padrão diferente
"""
from __future__ import annotations

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from repositories.sheets_client import (
    get_worksheet,
    get_header_row,
    read_all_records,
    find_row_by_col,
    update_row_fields,
    _retry_on_quota,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("neuroauth.migrate_legacy")

TAB_EPISODIOS = "22_EPISODIOS"
HEAD          = 3
COL_EP_ID     = "episodio_id"
COL_CIR_ID    = "cirurgiao_principal_id"
COL_RETRO     = "preenchimento_retroativo"


def migrate(default_cir_id: str = "CIR_001", dry_run: bool = False) -> dict:
    """
    Backfill de cirurgiao_principal_id em 22_EPISODIOS.

    Returns:
        dict com contadores: total, skipped, updated, errors.
    """
    counters = {"total": 0, "skipped": 0, "updated": 0, "errors": 0, "no_col": 0}

    # Carregar aba
    try:
        ws = get_worksheet(TAB_EPISODIOS)
    except Exception as exc:
        logger.error("Não foi possível acessar %s: %s", TAB_EPISODIOS, exc)
        return counters

    # Verificar colunas disponíveis
    header = get_header_row(ws, head=HEAD)
    header_lower = [h.strip().lower() for h in header]

    has_cir_col   = COL_CIR_ID.lower() in header_lower
    has_retro_col = COL_RETRO.lower() in header_lower

    if not has_cir_col:
        logger.error(
            "Coluna '%s' não encontrada em %s. "
            "Colunas disponíveis: %s. "
            "Adicione a coluna antes de rodar a migration.",
            COL_CIR_ID, TAB_EPISODIOS, header,
        )
        counters["no_col"] = 1
        return counters

    if not has_retro_col:
        logger.warning(
            "Coluna '%s' não encontrada — flag de retroativo não será gravada.",
            COL_RETRO,
        )

    # Carregar todos os registros
    rows = read_all_records(ws, head=HEAD)
    logger.info("Carregados %d registros de %s", len(rows), TAB_EPISODIOS)

    for row in rows:
        counters["total"] += 1

        ep_id = (row.get(COL_EP_ID) or "").strip()
        if not ep_id:
            logger.debug("Linha sem episodio_id, pulando.")
            counters["skipped"] += 1
            continue

        cir_atual = (row.get(COL_CIR_ID) or "").strip()
        if cir_atual:
            # Já tem cirurgião — idempotente, pular
            logger.debug("Episódio %s já tem cirurgião '%s' → skip", ep_id, cir_atual)
            counters["skipped"] += 1
            continue

        # Encontrar índice da linha para update
        row_idx, _ = find_row_by_col(ws, COL_EP_ID, ep_id, head=HEAD)
        if row_idx is None:
            logger.warning("Episódio %s não encontrado via find_row_by_col — skip", ep_id)
            counters["errors"] += 1
            continue

        updates: dict = {COL_CIR_ID: default_cir_id}
        if has_retro_col:
            updates[COL_RETRO] = "TRUE"

        if dry_run:
            logger.info(
                "[DRY-RUN] Episódio %s (row %d): %s = %s",
                ep_id, row_idx, COL_CIR_ID, default_cir_id,
            )
            counters["updated"] += 1
            continue

        try:
            update_row_fields(ws, row_idx, header, updates)
            logger.info(
                "Episódio %s (row %d): cirurgiao_principal_id = %s [ATUALIZADO]",
                ep_id, row_idx, default_cir_id,
            )
            counters["updated"] += 1
        except Exception as exc:
            logger.error("Erro ao atualizar episódio %s: %s", ep_id, exc)
            counters["errors"] += 1

    return counters


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill de cirurgiao_principal_id em casos legados."
    )
    parser.add_argument("--dry-run", action="store_true", help="Simula sem escrever.")
    parser.add_argument(
        "--cir-id",
        default="CIR_001",
        help="ID do cirurgião padrão para backfill (default: CIR_001).",
    )
    args = parser.parse_args()

    print(f"\nNEUROAUTH — migrate_legacy_cases.py")
    print(f"Modo      : {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"Cirurgião : {args.cir_id}")
    print(f"Aba alvo  : {TAB_EPISODIOS}\n")

    result = migrate(default_cir_id=args.cir_id, dry_run=args.dry_run)

    print("\n" + "=" * 50)
    print("RESULTADO DA MIGRATION")
    print("=" * 50)
    print(f"  Total processado : {result['total']}")
    print(f"  Skipped (já ok)  : {result['skipped']}")
    print(f"  Atualizados      : {result['updated']}")
    print(f"  Erros            : {result['errors']}")

    if result.get("no_col"):
        print("\nSTATUS: NO-GO — coluna cirurgiao_principal_id ausente em 22_EPISODIOS.")
        sys.exit(2)
    elif result["errors"] > 0:
        print("\nSTATUS: GO COM RESSALVAS — alguns registros com erro.")
        sys.exit(1)
    else:
        print("\nSTATUS: GO — migration concluída com sucesso.")
        sys.exit(0)
