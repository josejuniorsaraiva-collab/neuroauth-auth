#!/usr/bin/env python3
"""
scripts/validar.py
NEUROAUTH — Homologação humana de casos processados.

Promove um caso de output/review/ para output/validated/ (ou output/rejected/).
Grava registro de auditoria imutável em output/validated/.audit/ (ou rejected/).

FLUXO:
  CASO → hook → output/review/<CASE_ID>.json
       → [você executa: python scripts/validar.py CASE_ID]
       → output/validated/<CASE_ID>.json
       → output/validated/.audit/<CASE_ID>.audit.json   ← rastreabilidade total

USO:
  python scripts/validar.py CASE_ID                        # homologa
  python scripts/validar.py CASE_ID --rejeitar             # rejeita
  python scripts/validar.py CASE_ID --rejeitar -m "Motivo" # rejeita com motivo
  python scripts/validar.py --listar                       # lista pending em review/
  python scripts/validar.py --status CASE_ID               # mostra estado do caso

VARIÁVEIS DE AMBIENTE:
  NEURO_INGEST_ROOT   raiz do neuro_ingest (default: ~/neuro_ingest)
  VALIDADOR_ID        nome/login do validador (default: $USER)
"""
from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# ─── Configuração ─────────────────────────────────────────────────────────────

_NEURO_ROOT = Path(os.environ.get(
    "NEURO_INGEST_ROOT",
    str(Path.home() / "neuro_ingest"),
))

DIR_REVIEW    = _NEURO_ROOT / "output" / "review"
DIR_VALIDATED = _NEURO_ROOT / "output" / "validated"
DIR_REJECTED  = _NEURO_ROOT / "output" / "rejected"
DIR_AUDIT_VAL = DIR_VALIDATED / ".audit"
DIR_AUDIT_REJ = DIR_REJECTED  / ".audit"

VALIDADOR_ID = os.environ.get("VALIDADOR_ID") or getpass.getuser()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write(target: Path, payload: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(target.parent), prefix=".tmp_audit_", suffix=".json")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
        os.replace(tmp, target)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _find_case_file(case_id: str) -> Path | None:
    """Procura CASE_ID em review/. Aceita com ou sem extensão."""
    exact = DIR_REVIEW / f"{case_id}.json"
    if exact.exists():
        return exact
    # busca por prefixo (case_id pode ser parcial)
    matches = list(DIR_REVIEW.glob(f"{case_id}*.json"))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        print(f"❌ AMBÍGUO: '{case_id}' corresponde a {len(matches)} arquivos:")
        for m in matches:
            print(f"   {m.stem}")
        return None
    return None


def _extract_motor_version(caso: dict) -> str:
    """Extrai motor_version do payload se disponível."""
    try:
        return (
            caso.get("decisao_motor", {}).get("motor_version")
            or caso.get("motor_version")
            or "DESCONHECIDA"
        )
    except Exception:
        return "DESCONHECIDA"


def _extract_trace_id(caso: dict) -> str | None:
    """Extrai trace_id / decision_run_id do payload."""
    try:
        return (
            caso.get("decisao_motor", {}).get("decision_run_id")
            or caso.get("decisao_motor", {}).get("episodio_id")
            or caso.get("caso", {}).get("id")
            or caso.get("trace_id")
            or caso.get("decision_run_id")
        )
    except Exception:
        return None


# ─── Ações principais ─────────────────────────────────────────────────────────

def validar(case_id: str, motivo: str = "") -> int:
    """Homologa caso: review/ → validated/ + audit."""
    src = _find_case_file(case_id)
    if src is None:
        print(f"❌ Caso '{case_id}' não encontrado em {DIR_REVIEW}")
        return 1

    caso_bytes = src.read_bytes()
    caso_data  = json.loads(caso_bytes)

    dst       = DIR_VALIDATED / src.name
    audit_dst = DIR_AUDIT_VAL / src.name.replace(".json", ".audit.json")

    # Guard: já validado?
    if dst.exists():
        print(f"⚠️  Caso '{src.stem}' já está em validated/. Nada alterado.")
        return 0

    # Mover arquivo
    DIR_VALIDATED.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)  # copy primeiro, remove depois (seguro)

    # Registro de auditoria imutável
    audit = {
        "event":          "VALIDATED",
        "case_id":        src.stem,
        "validated_by":   VALIDADOR_ID,
        "timestamp":      _now_iso(),
        "motor_version":  _extract_motor_version(caso_data),
        "trace_id":       _extract_trace_id(caso_data),
        "json_hash_sha256": _sha256(dst),
        "motivo":         motivo or "",
        "source_path":    str(src),
        "dest_path":      str(dst),
    }

    # Verificar PDF adjacente (mesmo nome, extensão .pdf)
    pdf_candidate = src.with_suffix(".pdf")
    if pdf_candidate.exists():
        pdf_dst = DIR_VALIDATED / pdf_candidate.name
        shutil.copy2(pdf_candidate, pdf_dst)
        audit["pdf_hash_sha256"] = _sha256(pdf_dst)
        audit["pdf_path"] = str(pdf_dst)
    else:
        audit["pdf_hash_sha256"] = None
        audit["pdf_path"] = None

    _atomic_write(audit_dst, json.dumps(audit, ensure_ascii=False, indent=2).encode())

    # Remove da review SOMENTE após tudo gravado (falha segura)
    src.unlink()

    print(f"✅ VALIDATED  {src.stem}")
    print(f"   destino:   {dst}")
    print(f"   audit:     {audit_dst}")
    print(f"   json_hash: {audit['json_hash_sha256'][:16]}...")
    print(f"   motor:     {audit['motor_version']}")
    print(f"   validado_por: {VALIDADOR_ID}  em {audit['timestamp']}")
    if audit["pdf_hash_sha256"]:
        print(f"   pdf_hash:  {audit['pdf_hash_sha256'][:16]}...")
    return 0


def rejeitar(case_id: str, motivo: str = "") -> int:
    """Rejeita caso: review/ → rejected/ + audit."""
    src = _find_case_file(case_id)
    if src is None:
        print(f"❌ Caso '{case_id}' não encontrado em {DIR_REVIEW}")
        return 1

    caso_bytes = src.read_bytes()
    caso_data  = json.loads(caso_bytes)

    dst       = DIR_REJECTED / src.name
    audit_dst = DIR_AUDIT_REJ / src.name.replace(".json", ".audit.json")

    DIR_REJECTED.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

    audit = {
        "event":          "REJECTED",
        "case_id":        src.stem,
        "rejected_by":    VALIDADOR_ID,
        "timestamp":      _now_iso(),
        "motor_version":  _extract_motor_version(caso_data),
        "trace_id":       _extract_trace_id(caso_data),
        "json_hash_sha256": _sha256(dst),
        "motivo":         motivo or "SEM_MOTIVO_INFORMADO",
        "source_path":    str(src),
        "dest_path":      str(dst),
    }

    _atomic_write(audit_dst, json.dumps(audit, ensure_ascii=False, indent=2).encode())
    src.unlink()

    print(f"🔴 REJECTED   {src.stem}")
    print(f"   destino:   {dst}")
    print(f"   audit:     {audit_dst}")
    print(f"   motivo:    {audit['motivo']}")
    print(f"   rejeitado_por: {VALIDADOR_ID}  em {audit['timestamp']}")
    return 0


def listar() -> int:
    """Lista casos pendentes em review/."""
    DIR_REVIEW.mkdir(parents=True, exist_ok=True)
    casos = sorted(DIR_REVIEW.glob("*.json"))

    if not casos:
        print(f"✅ Nenhum caso pendente em {DIR_REVIEW}")
        return 0

    print(f"📋 {len(casos)} caso(s) pendente(s) em review/:\n")
    for c in casos:
        stat = c.stat()
        ts   = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"   {c.stem:<60}  {ts}  ({stat.st_size:,} bytes)")
    print(f"\nPara homologar: python scripts/validar.py <CASE_ID>")
    print(f"Para rejeitar:  python scripts/validar.py <CASE_ID> --rejeitar -m 'motivo'")
    return 0


def status_caso(case_id: str) -> int:
    """Mostra o estado atual de um caso."""
    locais = {
        "REVIEW":    DIR_REVIEW    / f"{case_id}.json",
        "VALIDATED": DIR_VALIDATED / f"{case_id}.json",
        "REJECTED":  DIR_REJECTED  / f"{case_id}.json",
    }
    audits = {
        "VALIDATED": DIR_AUDIT_VAL / f"{case_id}.audit.json",
        "REJECTED":  DIR_AUDIT_REJ / f"{case_id}.audit.json",
    }

    estado = None
    for nome, path in locais.items():
        if path.exists():
            estado = nome
            break

    if estado is None:
        print(f"❓ Caso '{case_id}' não encontrado em nenhuma camada.")
        return 1

    print(f"📍 STATUS: {estado}")
    print(f"   arquivo: {locais[estado]}")

    audit_path = audits.get(estado)
    if audit_path and audit_path.exists():
        print(f"\n   AUDIT:")
        audit = json.loads(audit_path.read_bytes())
        for k, v in audit.items():
            if v is not None and v != "":
                print(f"     {k}: {v}")
    return 0


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="NEUROAUTH — Validação humana de casos (review → validated/rejected)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python scripts/validar.py EP_2026_001
  python scripts/validar.py EP_2026_001 --rejeitar -m "CID incorreto"
  python scripts/validar.py --listar
  python scripts/validar.py --status EP_2026_001
        """,
    )
    parser.add_argument("case_id", nargs="?", help="ID do caso a validar/rejeitar")
    parser.add_argument("--rejeitar", action="store_true", help="Rejeitar o caso")
    parser.add_argument("-m", "--motivo", default="", help="Motivo (opcional)")
    parser.add_argument("--listar", action="store_true", help="Listar casos pendentes")
    parser.add_argument("--status", metavar="CASE_ID", help="Ver estado de um caso")

    args = parser.parse_args()

    if args.listar:
        return listar()

    if args.status:
        return status_caso(args.status)

    if not args.case_id:
        parser.print_help()
        return 1

    if args.rejeitar:
        return rejeitar(args.case_id, motivo=args.motivo)

    return validar(args.case_id, motivo=args.motivo)


if __name__ == "__main__":
    sys.exit(main())
