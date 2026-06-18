"""Interface de linha de comando do agente.

Exemplos:
    python -m publisher watch              # monitora continuamente
    python -m publisher once               # processa uma vez e sai
    python -m publisher once --dry-run     # sem chamar o YouTube
    python -m publisher auth               # apenas autentica/renova o token
"""
from __future__ import annotations

import argparse
import logging
import sys

from .config import Config
from .core import Publisher
from .youtube_client import DryRunUploader, YouTubeUploader


def _build_uploader(dry_run: bool, cfg: Config):
    return DryRunUploader() if dry_run else YouTubeUploader(cfg)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="publisher", description="YouTube Publisher Agent (Nível 3)")
    parser.add_argument(
        "command",
        choices=("watch", "once", "auth"),
        help="watch: monitora continuamente | once: roda uma vez | auth: só autentica",
    )
    parser.add_argument("--dry-run", action="store_true", help="Não chama o YouTube; simula o upload.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Log em nível DEBUG.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg = Config()

    if args.command == "auth":
        if args.dry_run:
            print("--dry-run não faz sentido com 'auth'.", file=sys.stderr)
            return 2
        uploader = YouTubeUploader(cfg)
        uploader._build_service()  # dispara o fluxo OAuth e salva o token
        print(f"Autenticado. Token salvo em {cfg.token_file}")
        return 0

    publisher = Publisher(cfg, _build_uploader(args.dry_run, cfg))

    if args.command == "once":
        records = publisher.run_once()
        ok = sum(1 for r in records if r.status == "success")
        fail = len(records) - ok
        print(f"Processados {len(records)} vídeo(s): {ok} sucesso, {fail} falha.")
        return 1 if fail else 0

    if args.command == "watch":
        try:
            publisher.watch()
        except KeyboardInterrupt:
            print("\nEncerrado.")
        return 0

    return 0  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
