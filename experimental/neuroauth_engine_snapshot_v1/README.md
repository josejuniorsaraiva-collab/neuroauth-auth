# NEUROAUTH Engine Snapshot v1 — Congelado

**Status:** FROZEN — preservado para fins de versionamento e auditoria.
**Localização:** `experimental/neuroauth_engine_snapshot_v1/` dentro do repositório `neuroauth-auth`.
**Classe de conteúdo:** motor de autorização cirúrgica NEUROAUTH (Flask legacy + engine v2.2 + runner local + suite de regressão).
**Acoplamento operacional:** ZERO. Nada aqui é importado pelo entrypoint de produção
(`app.main:app` — Procfile / render.yaml). Mover, modificar ou remover esta pasta
não tem efeito sobre auth, deploy, APIs públicas ou serviços em produção.

## Histórico de nomenclatura

Este diretório foi inicialmente criado como `experimental/dtc_runtime_v1/` no commit
`4ed36ef`. Uma auditoria de escopo (ver `SCOPE_AUDIT_EXPERIMENTAL_DTC.md` na raiz do
repo, commit `7de7c0d`) confirmou que **100% dos arquivos copiados (55/55) pertencem
ao motor NEUROAUTH**, não a um DTC Runtime clínico. Após confirmação humana
(Opção B do audit), o diretório foi renomeado para refletir o conteúdo real.

O nome `experimental/dtc_runtime_v1/` foi recriado vazio (apenas com um README de
reserva) e está disponível para receber a futura implementação DTC clínica.

## Princípio

`neuroauth-auth/` atua como pasta-mãe organizacional. Este snapshot vive isolado
em `experimental/` para:

- preservar a engine NEUROAUTH v2.2 com integridade auditável (SHA-256);
- manter desacoplamento operacional total da produção FastAPI atual;
- garantir reproducibilidade futura via snapshot versionado.

## Estrutura

```
experimental/neuroauth_engine_snapshot_v1/
├── README.md          (este arquivo)
├── VERSION            (versão + metadados de freeze)
├── MANIFEST.md        (inventário de arquivos + proveniência)
├── FROZEN.md          (arquivos sob contrato de congelamento)
├── SHA256SUMS.txt     (checksums para integridade)
└── snapshot/          (cópia idêntica do material NEUROAUTH engine)
    ├── BASELINE_v2.1.0.txt              (NEUROAUTH DECISION XP 2.1.0-stable)
    ├── hardening_report.json            (FASE 1-3 PASS, 5/5 shadow OK)
    ├── runner_local.py                  (runner contra https://neuroauth-auth.onrender.com)
    ├── flask_app.py                     (entry-point Flask legacy)
    ├── neuroauth_hook.py                (emit_to_neuro_ingest)
    ├── test_integration.py
    ├── decision_classifier.py           (raiz, versão legacy)
    ├── decision_repository.py           (raiz, versão legacy)
    ├── decision_routes.py               (raiz, versão legacy)
    ├── schema_mapper.py                 (raiz, versão legacy)
    ├── validator_engine.py              (raiz)
    ├── validator_rules.py               (raiz, regras RGL001-RGL061)
    ├── motor_routes.py                  (raiz)
    ├── convenio_repository.py           (raiz, versão legacy)
    ├── proc_master_repository.py        (raiz, versão legacy)
    ├── motor/                           (engine v2.2 — decision classifier + validators)
    ├── routes/                          (Flask blueprints — não usados por FastAPI)
    ├── repositories/                    (clinical/calendar/feedback/insights/sheets)
    ├── scripts/                         (validar.py, migrate_legacy_cases.py, ...)
    ├── tests/                           (suite de regressão v2 + noites 6/7/8)
    ├── docs/                            (RUNNER_CONTRACT, MACHINE_STATE, STATUS_AUTORIZACAO)
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
4. **Não confundir** este diretório com `experimental/dtc_runtime_v1/`. Aquele está
   reservado para implementação DTC clínica futura e atualmente não contém código.

## Validação de integridade

```bash
cd experimental/neuroauth_engine_snapshot_v1
(cd snapshot && sha256sum -c ../SHA256SUMS.txt)
# esperado: 55/55 OK
```

## Origem

Snapshot extraído do branch `claude/charming-lovelace-OK1O6` em 2026-05-26 via
cópia (`cp -a`, não move) dos artefatos NEUROAUTH presentes na raiz do repositório.
Originais permanecem intactos na raiz para continuar servindo produção.
