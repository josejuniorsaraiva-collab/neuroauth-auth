# FROZEN — Arquivos sob contrato de congelamento

**Snapshot:** NEUROAUTH Engine v1 (motor de autorização cirúrgica, engine v2.2).
**Diretório:** `experimental/neuroauth_engine_snapshot_v1/snapshot/`.

Os arquivos abaixo estão **CONGELADOS** dentro de `snapshot/`. Qualquer alteração
exige revisão explícita conforme `snapshot/docs/RUNNER_CONTRACT.md` (Versão 1.0,
status: CONGELADO — não alterar sem revisão explícita).

## Contrato Runner (não alterar sem revisão)

Origem do congelamento: `snapshot/docs/RUNNER_CONTRACT.md`

| Arquivo                                              | Motivo do freeze                                                       |
|------------------------------------------------------|------------------------------------------------------------------------|
| `snapshot/docs/RUNNER_CONTRACT.md`                   | Contrato explicitamente marcado "CONGELADO" — commit baseline 7e793a0  |
| `snapshot/runner_local.py`                           | Runner local FASE 4 — referenciado pelo contrato                        |
| `snapshot/tests/test_noite7_runner.py`               | Suite de regressão obrigatória do runner (12 testes)                    |
| `snapshot/tests/test_noite8_robustez.py`             | Suite de regressão obrigatória do runner (12 testes)                    |

## Baseline operacional (não alterar — referência histórica)

Origem: `snapshot/BASELINE_v2.1.0.txt` declara explicitamente
"Motor nunca alterado; todas as regras de negócio preservadas intactas".

| Arquivo                                  | Motivo do freeze                                |
|------------------------------------------|-------------------------------------------------|
| `snapshot/BASELINE_v2.1.0.txt`           | Baseline 2.1.0-stable, 13/13 checks confirmados |
| `snapshot/hardening_report.json`         | Relatório imutável FASE 1-3 PASS                |
| `snapshot/motor/validator_rules.py`      | Regras RGL001–RGL061 — não modificadas         |
| `snapshot/validator_rules.py` (root)     | Versão raiz preservada para histórico           |

## Estado do motor 1 (snapshot histórico)

| Arquivo                                            | Motivo do freeze                          |
|----------------------------------------------------|-------------------------------------------|
| `snapshot/docs/MACHINE_STATE_MOTOR1_v1.md`         | Estado fotografado do motor 1 v1          |
| `snapshot/docs/STATUS_AUTORIZACAO_WORKFLOW_v1.json`| Workflow de autorização v1 — congelado    |

## Regras válidas em todo o snapshot

1. Originais correspondentes na raiz do repositório **não foram tocados**.
2. Cópias no snapshot têm SHA-256 listado em `../SHA256SUMS.txt`.
3. Para verificar integridade: `(cd snapshot && sha256sum -c ../SHA256SUMS.txt)`.
4. Reativar este runtime exige restaurar `app/` referências removidas + ajustar
   `Procfile` para `flask_app:application` — **não fazer sem aprovação**.
