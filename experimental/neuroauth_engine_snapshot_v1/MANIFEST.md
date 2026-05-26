# MANIFEST — NEUROAUTH Engine Snapshot v1

Inventário dos 55 arquivos copiados para `snapshot/`, agrupados por categoria,
com proveniência (caminho na raiz do repositório `neuroauth-auth`).

Método de cópia: `cp -a` (preserva timestamps + permissões). Origens permanecem
intactas no repositório-raiz para não impactar produção.

> **Histórico de nomenclatura:** este snapshot foi criado em `experimental/dtc_runtime_v1/`
> (commit `4ed36ef`) e renomeado para `experimental/neuroauth_engine_snapshot_v1/` após
> a auditoria `SCOPE_AUDIT_EXPERIMENTAL_DTC.md` (commit `7de7c0d`) confirmar que o
> conteúdo é integralmente NEUROAUTH, não DTC.

## 1. Baseline & relatórios de hardening

| snapshot/                  | origem (raiz)           | nota                                |
|----------------------------|-------------------------|-------------------------------------|
| `BASELINE_v2.1.0.txt`      | `BASELINE_v2.1.0.txt`   | NEUROAUTH DECISION XP 2.1.0-stable  |
| `hardening_report.json`    | `hardening_report.json` | FASE 1-3 PASS, 5/5 shadow OK        |

## 2. Runner local + contrato

| snapshot/                       | origem                          |
|---------------------------------|---------------------------------|
| `runner_local.py`               | `runner_local.py`               |
| `docs/RUNNER_CONTRACT.md`       | `docs/RUNNER_CONTRACT.md`       |
| `docs/MACHINE_STATE_MOTOR1_v1.md` | `docs/MACHINE_STATE_MOTOR1_v1.md` |
| `docs/STATUS_AUTORIZACAO_WORKFLOW_v1.json` | `docs/STATUS_AUTORIZACAO_WORKFLOW_v1.json` |
| `test_integration.py`           | `test_integration.py`           |

## 3. Motor (decision engine v2.2)

| snapshot/                          | origem                            |
|------------------------------------|-----------------------------------|
| `motor/__init__.py`                | `motor/__init__.py`               |
| `motor/decision_classifier.py`     | `motor/decision_classifier.py`    |
| `motor/schema_mapper.py`           | `motor/schema_mapper.py`          |
| `motor/validator_engine.py`        | `motor/validator_engine.py`       |
| `motor/validator_rules.py`         | `motor/validator_rules.py`        |

## 4. Flask app legacy (entrypoint paralelo, fora do deploy)

| snapshot/         | origem         | nota                                                       |
|-------------------|----------------|------------------------------------------------------------|
| `flask_app.py`    | `flask_app.py` | Procfile usa `app.main:app` (FastAPI); este Flask é legacy |

## 5. Routes (Flask blueprints — não usadas pelo FastAPI)

| snapshot/                       | origem                          |
|---------------------------------|---------------------------------|
| `routes/__init__.py`            | `routes/__init__.py`            |
| `routes/decision_routes.py`     | `routes/decision_routes.py`     |
| `routes/episodios_routes.py`    | `routes/episodios_routes.py`    |
| `routes/gateway_routes.py`      | `routes/gateway_routes.py`      |
| `routes/hub_routes.py`          | `routes/hub_routes.py`          |
| `routes/motor_routes.py`        | `routes/motor_routes.py`        |

## 6. Repositories (espelhados — originais permanecem na raiz)

> **ATENÇÃO:** `repositories/sheets_client.py` é importado por produção
> (`app/routers/hub.py`, `app/services/surgeon_validator.py`,
> `app/services/surgeon_producao.py`). O arquivo aqui é uma **cópia congelada**;
> o original na raiz NÃO foi tocado.

| snapshot/                                              | origem                                                 |
|--------------------------------------------------------|--------------------------------------------------------|
| `repositories/__init__.py`                             | `repositories/__init__.py`                             |
| `repositories/calendar_event_builder.py`               | `repositories/calendar_event_builder.py`               |
| `repositories/calendar_repository.py`                  | `repositories/calendar_repository.py`                  |
| `repositories/clinical_protocols.py`                   | `repositories/clinical_protocols.py`                   |
| `repositories/convenio_repository.py`                  | `repositories/convenio_repository.py`                  |
| `repositories/data/clinical_protocols_seed_v1.json`    | `repositories/data/clinical_protocols_seed_v1.json`    |
| `repositories/decision_repository.py`                  | `repositories/decision_repository.py`                  |
| `repositories/feedback_repository.py`                  | `repositories/feedback_repository.py`                  |
| `repositories/insights_repository.py`                  | `repositories/insights_repository.py`                  |
| `repositories/precheck_engine.py`                      | `repositories/precheck_engine.py`                      |
| `repositories/proc_master_repository.py`               | `repositories/proc_master_repository.py`               |
| `repositories/sheets_client.py`                        | `repositories/sheets_client.py`  ← usado por produção  |
| `repositories/tracker_repository.py`                   | `repositories/tracker_repository.py`                   |

## 7. Hooks (neuroauth_hook)

> **ATENÇÃO:** `neuroauth_hook.py` é importado por produção (`app/routers/decide.py`).
> Cópia congelada apenas — original intocado.

| snapshot/             | origem                |
|-----------------------|-----------------------|
| `neuroauth_hook.py`   | `neuroauth_hook.py`   |

## 8. Versões legacy nível-raiz (duplicatas históricas)

Estas eram versões mais antigas, paralelas às de `motor/` e `routes/`. Preservadas
para histórico — checksums distintos das versões em subdiretório (ver SHA256SUMS.txt).

| snapshot/                       | origem                          |
|---------------------------------|---------------------------------|
| `decision_classifier.py`        | `decision_classifier.py`        |
| `decision_repository.py`        | `decision_repository.py`        |
| `decision_routes.py`            | `decision_routes.py`            |
| `schema_mapper.py`              | `schema_mapper.py`              |
| `validator_engine.py`           | `validator_engine.py`           |
| `validator_rules.py`            | `validator_rules.py`            |
| `motor_routes.py`               | `motor_routes.py`               |
| `convenio_repository.py`        | `convenio_repository.py`        |
| `proc_master_repository.py`     | `proc_master_repository.py`     |

## 9. Scripts utilitários

| snapshot/                            | origem                               |
|--------------------------------------|--------------------------------------|
| `scripts/migrate_legacy_cases.py`    | `scripts/migrate_legacy_cases.py`    |
| `scripts/setup_surgeon_sheets.py`    | `scripts/setup_surgeon_sheets.py`    |
| `scripts/test_precedencia.py`        | `scripts/test_precedencia.py`        |
| `scripts/validar.py`                 | `scripts/validar.py`                 |

## 10. Tests (suite de regressão)

| snapshot/                                  | origem                                     |
|--------------------------------------------|--------------------------------------------|
| `tests/__init__.py`                        | `tests/__init__.py`                        |
| `tests/test_blindagem_v2.py`               | `tests/test_blindagem_v2.py`               |
| `tests/test_engine_v2.py`                  | `tests/test_engine_v2.py`                  |
| `tests/test_governanca_producao.py`        | `tests/test_governanca_producao.py`        |
| `tests/test_noite6_observabilidade.py`     | `tests/test_noite6_observabilidade.py`     |
| `tests/test_noite7_runner.py`              | `tests/test_noite7_runner.py`              |
| `tests/test_noite8_regressao.py`           | `tests/test_noite8_regressao.py`           |
| `tests/test_noite8_robustez.py`            | `tests/test_noite8_robustez.py`            |

## 11. Frontend operacional (painel; não servido por FastAPI)

| snapshot/                                       | origem                                          |
|-------------------------------------------------|-------------------------------------------------|
| `frontend/neuroauth_painel_operacional_v2.html` | `frontend/neuroauth_painel_operacional_v2.html` |

## NÃO COPIADO (produção pura — permanece exclusivo da raiz)

- `app/` (FastAPI — Procfile / render.yaml)
- `frontend/neuroauth_form_v2.html` (servido por `app/main.py` → `/form`)
- `Procfile`, `render.yaml`, `gunicorn.conf.py`, `requirements.txt`
- `.env.example`, `.env.local`, `.gitignore`

Total no snapshot: **55 arquivos**, **548 K** (`du -sh`), **416 934 bytes** exatos.
