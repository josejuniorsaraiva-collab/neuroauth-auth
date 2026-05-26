# DTC Runtime v1 — Snapshot Congelado

**Status:** FROZEN — preservado para fins de versionamento e auditoria.
**Localização:** `experimental/dtc_runtime_v1/` dentro do repositório `neuroauth-auth`.
**Acoplamento operacional:** ZERO. Nada aqui é importado pelo entrypoint de produção
(`app.main:app` — Procfile / render.yaml). Mover, modificar ou remover esta pasta
não tem efeito sobre auth, deploy, APIs públicas ou serviços em produção.

## Princípio

`neuroauth-auth/` é utilizado **apenas como pasta-mãe organizacional**. O DTC
Runtime v1 vive isolado em `experimental/dtc_runtime_v1/` para:

- preservar o runtime DTC dentro do ecossistema NEUROAUTH (mesmo repo);
- manter desacoplamento operacional total do sistema principal;
- garantir reproducibilidade futura via snapshot versionado.

## Estrutura

```
experimental/dtc_runtime_v1/
├── README.md          (este arquivo)
├── VERSION            (versão + metadados de freeze)
├── MANIFEST.md        (inventário de arquivos + proveniência)
├── FROZEN.md          (arquivos sob contrato de congelamento)
├── SHA256SUMS.txt     (checksums para integridade)
└── snapshot/          (cópia idêntica do material DTC Runtime)
    ├── BASELINE_v2.1.0.txt
    ├── hardening_report.json
    ├── runner_local.py
    ├── flask_app.py
    ├── neuroauth_hook.py
    ├── test_integration.py
    ├── decision_classifier.py          (root, versão legacy)
    ├── decision_repository.py          (root, versão legacy)
    ├── decision_routes.py              (root, versão legacy)
    ├── schema_mapper.py                (root, versão legacy)
    ├── validator_engine.py             (root)
    ├── validator_rules.py              (root)
    ├── motor_routes.py                 (root)
    ├── convenio_repository.py          (root, versão legacy)
    ├── proc_master_repository.py       (root, versão legacy)
    ├── motor/                          (engine v2.2 — decision classifier + validators)
    ├── routes/                         (Flask blueprints — não usados por FastAPI)
    ├── repositories/                   (clinical/calendar/feedback/insights/sheets)
    ├── scripts/                        (validar.py, migrate_legacy_cases.py, ...)
    ├── tests/                          (suite de regressão v2 + noites 6/7/8)
    ├── docs/                           (RUNNER_CONTRACT, MACHINE_STATE, STATUS_AUTORIZACAO)
    └── frontend/
        └── neuroauth_painel_operacional_v2.html
```

## Regras (NÃO VIOLAR)

1. **Não importar** nada deste snapshot a partir de `app/`. Produção deve permanecer
   independente.
2. **Não alterar** os arquivos listados em `FROZEN.md` sem revisão explícita
   conforme `snapshot/docs/RUNNER_CONTRACT.md`.
3. **Não mover** os módulos que produção ainda referencia no diretório-raiz
   (`repositories/sheets_client.py`, `neuroauth_hook.py`, `frontend/neuroauth_form_v2.html`).
   Eles foram **copiados** para o snapshot — os originais permanecem na raiz para servir produção.

## Validação de integridade

```bash
cd experimental/dtc_runtime_v1
(cd snapshot && sha256sum -c ../SHA256SUMS.txt)
```

## Origem

Snapshot extraído do branch `claude/charming-lovelace-OK1O6` em 2026-05-26 via
cópia (não move) dos artefatos DTC presentes na raiz do repositório.
