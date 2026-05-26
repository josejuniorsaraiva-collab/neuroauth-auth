#!/usr/bin/env python3
"""
runner_local.py — FASE 4: Runner local para Mac mini.
Executa N casos shadow-mode contra o backend Render e gera relatório.

Uso:
  python runner_local.py                    # 5 casos padrão
  python runner_local.py --cases 20         # 20 casos
  python runner_local.py --base-url http://localhost:8000  # backend local
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    print("ERRO: pip install requests")
    sys.exit(1)

BASE_URL = "https://neuroauth-auth.onrender.com"
SECRET_PHRASE = "neuroauth-fase2-test"

# ── Casos de teste shadow-mode ───────────────────────────────────────
SHADOW_CASES = [
    {
        "name": "ACDF completo Unimed — espera GO 100",
        "payload": {
            "cid_principal": "M50.1",
            "procedimento": "Artrodese cervical via anterior (ACDF)",
            "convenio": "Unimed Cariri",
            "indicacao_clinica": "Cervicobraquialgia refratária com radiculopatia C5-C6 compressiva, falha de tratamento conservador por 8 semanas com fisioterapia e medicação.",
            "achados_resumo": "RNM cervical: hérnia discal C5-C6 posterolateral esquerda com compressão radicular e sinais de mielopatia incipiente.",
            "tto_conservador": "8 semanas de fisioterapia, AINE e gabapentina sem melhora funcional.",
            "necessita_opme": "Sim",
            "opme_items": [{"descricao": "Cage PEEK intersomático cervical", "qtd": 1}, {"descricao": "Placa cervical anterior 2 níveis", "qtd": 1}],
            "crm": "12345-CE",
            "cbo": "225142",
            "medico_solicitante": "Dr. Teste Runner"
        },
        "expect_classification": "GO",
        "expect_min_score": 90
    },
    {
        "name": "Conservador curto (4 sem) — espera GO com ressalva",
        "payload": {
            "cid_principal": "M50.2",
            "procedimento": "Discectomia cervical anterior",
            "convenio": "Unimed Fortaleza",
            "indicacao_clinica": "Dor cervical irradiada para membro superior direito com parestesia dermatomal C6, sem resposta a analgésicos convencionais.",
            "achados_resumo": "TC cervical evidencia protrusão discal C5-C6 com estenose foraminal moderada.",
            "tto_conservador": "4 semanas de tratamento conservador.",
            "necessita_opme": "Não",
            "crm": "54321-CE",
            "cbo": "225142",
            "medico_solicitante": "Dr. Conservador Curto"
        },
        "expect_classification": "GO",
        "expect_min_score": 80
    },
    {
        "name": "Dados mínimos — espera NO_GO",
        "payload": {
            "cid_principal": "M5",
            "procedimento": "Cirurgia",
            "convenio": "SUS",
            "indicacao_clinica": "Dor",
            "achados_resumo": "",
            "tto_conservador": "",
            "necessita_opme": "Não",
            "crm": "",
            "cbo": "",
            "medico_solicitante": "Dr. Vazio"
        },
        "expect_classification": "NO_GO",
        "expect_min_score": 0
    },
    {
        "name": "Laminectomia completa Unimed — espera GO 100",
        "payload": {
            "cid_principal": "M48.06",
            "procedimento": "Laminectomia descompressiva lombar",
            "convenio": "Unimed Juazeiro do Norte",
            "indicacao_clinica": "Estenose lombar severa L4-L5 com claudicação neurogênica limitante, marcha reduzida a 50m. Falha conservadora documentada.",
            "achados_resumo": "RNM lombar: estenose central severa L4-L5, hipertrofia facetária bilateral, espessamento do ligamento amarelo.",
            "tto_conservador": "10 semanas de fisioterapia intensiva, infiltração epidural sem melhora.",
            "necessita_opme": "Sim",
            "opme_items": [{"descricao": "Parafusos pediculares titânio L4-L5", "qtd": 4}, {"descricao": "Hastes longitudinais 5.5mm", "qtd": 2}],
            "crm": "67890-CE",
            "cbo": "225142",
            "medico_solicitante": "Dr. Laminectomia Runner"
        },
        "expect_classification": "GO",
        "expect_min_score": 90
    },
    {
        "name": "Non-Unimed + OPME completo — espera GO ~80",
        "payload": {
            "cid_principal": "M51.1",
            "procedimento": "Artrodese lombar intersomática (TLIF)",
            "convenio": "Hapvida",
            "indicacao_clinica": "Lombalgia crônica com instabilidade segmentar L5-S1, espondilolistese grau I, dor refratária a tratamento conservador prolongado.",
            "achados_resumo": "RNM: discopatia degenerativa L5-S1 com espondilolistese grau I, Modic tipo I.",
            "tto_conservador": "12 semanas de reabilitação com RPG, hidroterapia e bloqueio facetário.",
            "necessita_opme": "Sim",
            "opme_items": [{"descricao": "Cage TLIF PEEK radiolucente", "qtd": 1}, {"descricao": "Parafusos pediculares L5-S1", "qtd": 4}],
            "crm": "11111-CE",
            "cbo": "225142",
            "medico_solicitante": "Dr. Hapvida Runner"
        },
        "expect_classification": "GO",
        "expect_min_score": 70
    },
    {
        "name": "OPME marcado mas sem itens — espera GO_COM_RESSALVAS",
        "payload": {
            "cid_principal": "M50.1",
            "procedimento": "ACDF C5-C6",
            "convenio": "Unimed Cariri",
            "indicacao_clinica": "Hérnia discal cervical C5-C6 com compressão radicular e deficit motor grau 4 em bíceps esquerdo, refratário a tratamento.",
            "achados_resumo": "RNM: hérnia extrusa C5-C6 com compressão medular anterior.",
            "tto_conservador": "6 semanas de fisioterapia e gabapentina.",
            "necessita_opme": "Sim",
            "opme_items": [],
            "crm": "22222-CE",
            "cbo": "225142",
            "medico_solicitante": "Dr. OPME Vazio"
        },
        "expect_classification": "GO_COM_RESSALVAS",
        "expect_min_score": 50
    },
    {
        "name": "Sem CRM/CBO — perde 10pts completude",
        "payload": {
            "cid_principal": "G95.2",
            "procedimento": "Descompressão medular cervical",
            "convenio": "Unimed Sobral",
            "indicacao_clinica": "Mielopatia cervical espondilótica com sinais piramidais bilaterais, Nurick grau 3, deterioração progressiva em 3 meses.",
            "achados_resumo": "RNM: compressão medular C3-C5 com hipersinal intramedular T2.",
            "tto_conservador": "6 semanas — urgência relativa por progressão neurológica.",
            "necessita_opme": "Não",
            "crm": "",
            "cbo": "",
            "medico_solicitante": "Dr. Sem CRM"
        },
        "expect_classification": "GO",
        "expect_min_score": 70
    },
]


def get_token(base_url: str) -> str:
    """Obtém JWT via /auth/dev-token."""
    resp = requests.post(
        f"{base_url}/auth/dev-token",
        json={"secret_phrase": SECRET_PHRASE},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def run_case(base_url: str, token: str, case: dict) -> dict:
    """Executa um caso contra /decide e retorna resultado."""
    t0 = time.time()
    try:
        resp = requests.post(
            f"{base_url}/decide",
            json=case["payload"],
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        elapsed = round(time.time() - t0, 2)

        if resp.status_code != 200:
            return {
                "name": case["name"],
                "status": "FAIL",
                "http_status": resp.status_code,
                "error": resp.text[:300],
                "elapsed_s": elapsed,
            }

        data = resp.json()
        classification_ok = data["classification"] == case["expect_classification"]
        score_ok = data["score"] >= case["expect_min_score"]

        return {
            "name": case["name"],
            "status": "PASS" if (classification_ok and score_ok) else "WARN",
            "http_status": 200,
            "classification": data["classification"],
            "score": data["score"],
            "decision_status": data["decision_status"],
            "risco_glosa": data["risco_glosa"],
            "pendencias": len(data.get("pendencias", [])),
            "classification_ok": classification_ok,
            "score_ok": score_ok,
            "elapsed_s": elapsed,
        }

    except Exception as e:
        return {
            "name": case["name"],
            "status": "ERROR",
            "error": f"{type(e).__name__}: {str(e)[:200]}",
            "elapsed_s": round(time.time() - t0, 2),
        }


def main():
    parser = argparse.ArgumentParser(description="NEUROAUTH Runner Local — FASE 4")
    parser.add_argument("--base-url", default=BASE_URL, help="Backend URL")
    parser.add_argument("--cases", type=int, default=len(SHADOW_CASES), help="Numero de casos")
    parser.add_argument("--output", default="runner_report.json", help="Arquivo de saida")
    args = parser.parse_args()

    print(f"=== NEUROAUTH Runner Local ===")
    print(f"Backend: {args.base_url}")
    print(f"Casos:   {args.cases}")
    print()

    # 1. Obter token
    print("[1/3] Obtendo JWT via /auth/dev-token...")
    try:
        token = get_token(args.base_url)
        print(f"  OK — token obtido ({len(token)} chars)")
    except Exception as e:
        print(f"  FALHA — {e}")
        sys.exit(1)

    # 2. Executar casos
    cases_to_run = SHADOW_CASES[:args.cases]
    # Se pediu mais que o disponível, repete ciclicamente
    while len(cases_to_run) < args.cases:
        cases_to_run.append(SHADOW_CASES[len(cases_to_run) % len(SHADOW_CASES)])

    print(f"\n[2/3] Executando {len(cases_to_run)} casos...\n")
    results = []
    for i, case in enumerate(cases_to_run, 1):
        result = run_case(args.base_url, token, case)
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌", "ERROR": "💥"}.get(result["status"], "?")
        score_str = f"score={result.get('score', '?')}" if "score" in result else ""
        print(f"  [{i}/{len(cases_to_run)}] {icon} {result['name']} — {result['status']} {score_str} ({result['elapsed_s']}s)")
        results.append(result)

    # 3. Relatório
    n_pass = sum(1 for r in results if r["status"] == "PASS")
    n_warn = sum(1 for r in results if r["status"] == "WARN")
    n_fail = sum(1 for r in results if r["status"] in ("FAIL", "ERROR"))

    report = {
        "runner": "NEUROAUTH Runner Local v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "total_cases": len(results),
        "passed": n_pass,
        "warnings": n_warn,
        "failed": n_fail,
        "all_ok": n_fail == 0,
        "results": results,
    }

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n[3/3] Relatório salvo em {args.output}")
    print(f"\n{'='*40}")
    print(f"  PASS: {n_pass}  |  WARN: {n_warn}  |  FAIL: {n_fail}")
    print(f"  {'✅ ALL OK' if n_fail == 0 else '❌ FALHAS DETECTADAS'}")
    print(f"{'='*40}")

    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
