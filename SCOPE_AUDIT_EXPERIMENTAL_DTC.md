# SCOPE AUDIT — experimental/dtc_runtime_v1/

**Data:** 2026-05-26
**Branch:** `claude/charming-lovelace-OK1O6`
**Commit auditado:** `4ed36ef` (snapshot inicial)
**PR aberta?** NÃO. Push apenas para o branch de feature. `main` intocado.
**Produção alterada?** NÃO. `app/main:app`, `Procfile`, `render.yaml`, `app/`, `repositories/sheets_client.py`, `neuroauth_hook.py` permanecem sem diff.

---

## 1. O que foi realmente copiado

55 arquivos, 416 934 bytes, agrupados:

| Grupo | Arquivos | Origem na raiz |
|---|---|---|
| Baseline + hardening | `BASELINE_v2.1.0.txt`, `hardening_report.json` | raiz |
| Runner + integração | `runner_local.py`, `test_integration.py` | raiz |
| Flask app legacy | `flask_app.py` | raiz |
| Hook de ingestão | `neuroauth_hook.py` | raiz |
| Engine root (legacy) | `decision_classifier.py`, `decision_repository.py`, `decision_routes.py`, `schema_mapper.py`, `validator_engine.py`, `validator_rules.py`, `motor_routes.py`, `convenio_repository.py`, `proc_master_repository.py` | raiz |
| Motor v2.2 | `motor/` (5 arquivos) | `motor/` |
| Blueprints Flask | `routes/` (6 arquivos) | `routes/` |
| Repositories | `repositories/` (12 arquivos + data/) | `repositories/` |
| Scripts | `scripts/` (4 arquivos) | `scripts/` |
| Tests | `tests/` (8 arquivos) | `tests/` |
| Docs | `docs/` (3 arquivos) | `docs/` |
| Frontend operacional | `frontend/neuroauth_painel_operacional_v2.html` | `frontend/` |

Grep `\bdtc\b|decision tree` no snapshot inteiro: **0 ocorrências** em qualquer arquivo copiado. As únicas menções a "DTC" em todo o repositório estão no `README.md` e `MANIFEST.md` que **eu** escrevi.

---

## 2. Arquivos que são NEUROAUTH / Auth Runtime

**TODOS os 55 arquivos copiados.** Evidências concretas:

- `runner_local.py` linha 24: `BASE_URL = "https://neuroauth-auth.onrender.com"`; linha 25: `SECRET_PHRASE = "neuroauth-fase2-test"`; chama `/auth/dev-token` e `/decide`.
- `BASELINE_v2.1.0.txt` linha 1: `NEUROAUTH DECISION XP — BASELINE OPERACIONAL`.
- `hardening_report.json` linha 2: `"NEUROAUTH Hardening Report"`; linha 7: `"backend_url": "https://neuroauth-auth.onrender.com"`.
- `docs/RUNNER_CONTRACT.md` linha 1: `# NEUROAUTH — Contrato de Arquitetura do Runner`.
- `neuroauth_hook.py` — nome do arquivo + import `emit_to_neuro_ingest`.
- `validator_rules.py` — regras `RGL001`–`RGL061` (cirurgia / autorização de procedimentos), não regras de árvore de decisão.
- `motor/decision_classifier.py` — classifier de autorização cirúrgica (GO / GO_COM_RESSALVAS / NO_GO), não classificador de árvore.
- `repositories/sheets_client.py` — Google Sheets client para abas `21_DECISION_RUNS`, `22_EPISODIOS`, `23_RUNNER_QUEUE` (estrutura NEUROAUTH).
- `flask_app.py` linha 4: `"NEUROAUTH — Aplicação Flask"`.
- `frontend/neuroauth_painel_operacional_v2.html` — prefixo `neuroauth_` no nome.

Cobertura: **55 / 55 = 100 % NEUROAUTH**.

---

## 3. Arquivos que são DTC Runtime real

**0 arquivos.** Nenhum.

- Nenhum arquivo do snapshot menciona "DTC", "Decision Tree", "DTC Runtime" ou qualquer variante.
- Nenhum arquivo no repositório-raiz (`/home/user/neuroauth-auth`) também menciona — busca em `*.py *.md *.json *.txt` retornou apenas os documentos que **eu** acabei de criar.
- Conclusão: o material DTC Runtime que o usuário tinha em mente **não está neste repositório**, ou está em outro lugar (outro repo, pasta local não rastreada, branch separado), ou ainda não foi escrito.

---

## 4. O nome `experimental/dtc_runtime_v1/` está correto?

**NÃO. É enganoso.**

Foi rotulado "DTC Runtime v1" com base na nomenclatura usada pelo usuário na conversa, mas o conteúdo é integralmente o motor de autorização cirúrgica NEUROAUTH (engine v2.2). Um leitor futuro que clonar o repo e abrir `experimental/dtc_runtime_v1/` esperará encontrar lógica de árvore de decisão / Decision Tree Classifier, e em vez disso encontrará:
- runner contra endpoint `/decide` do NEUROAUTH;
- regras RGL clínicas de autorização;
- baseline da release `2.1.0-stable` do NEUROAUTH DECISION XP;
- relatório de hardening NEUROAUTH FASE 1-3.

A discrepância nome ↔ conteúdo é total.

---

## 5. Risco de confusão futura

**ALTO.**

| Cenário | Impacto |
|---|---|
| Desenvolvedor abrir `experimental/dtc_runtime_v1/` esperando DTC e encontrar NEUROAUTH | Tempo perdido em re-orientação; perda de confiança no diretório `experimental/` |
| Quando o DTC Runtime **real** chegar, não haver espaço óbvio para colocá-lo (nome já ocupado) | Forçará uma migração `dtc_runtime_v1 → v2` ou renome retroativo, com impacto em links / docs / scripts externos que apontarem para o caminho atual |
| Auditoria / due-diligence externa interpretar o snapshot como "DTC engine experimental" | Comunicação técnica errada, possível impacto regulatório se DTC tiver requisitos diferentes de NEUROAUTH |
| `MANIFEST.md`, `README.md`, `FROZEN.md` e `VERSION` afirmam "DTC Runtime v1" em prosa, reforçando a confusão | Cada cópia/clone propaga o erro |
| O nome ser citado em commits, PRs, issues e mensagens de release | Histórico Git carrega o termo errado para sempre |

Risco operacional sobre produção: **BAIXO** (snapshot é inerte — nada importa dele). Risco semântico / documental: **ALTO**.

---

## 6. Opções de correção

### Opção A — Renomear para o que realmente é
```
experimental/dtc_runtime_v1/ → experimental/neuroauth_engine_snapshot_v1/
```
- Reescrever `README.md`, `MANIFEST.md`, `FROZEN.md`, `VERSION` para refletir "NEUROAUTH Engine Snapshot v1" (motor 2.2 + runner + baseline 2.1.0).
- Recalcular `SHA256SUMS.txt` (paths mudam).
- Regenerar ZIP com novo nome.
- Custo: 1 commit, ~10 arquivos editados, snapshot fica honestamente nomeado.

### Opção B — Separar de verdade
```
experimental/
├── neuroauth_engine_snapshot_v1/   ← conteúdo atual, renomeado
└── dtc_runtime_v1/                 ← criado vazio (ou com README "reservado")
```
- Move atual para `neuroauth_engine_snapshot_v1/`.
- Cria `dtc_runtime_v1/` vazio com um `README.md` declarando "reservado para conteúdo DTC clínico — ainda não povoado".
- Custo: 1 commit, dois diretórios, espaço explicitamente reservado para o DTC real quando chegar.

### Opção C — Manter o nome e documentar muito
- Adicionar banner em `README.md`, `MANIFEST.md`, `FROZEN.md` e `VERSION` com aviso "ATENÇÃO: nome herdado; conteúdo é NEUROAUTH engine, não DTC".
- Não muda paths, não invalida o ZIP, não muda checksums.
- Custo: 1 commit, apenas edição textual, mas a dissonância nome ↔ conteúdo persiste.

---

## 7. Recomendação técnica

**Opção B.**

Razões:
1. É a única que respeita semanticamente a intenção original do usuário ("preservar runtime DTC versionado dentro do ecossistema NEUROAUTH") **e** descreve corretamente o conteúdo atual.
2. Reserva o nome correto (`dtc_runtime_v1/`) para o conteúdo DTC real quando ele existir, sem custo de migração futura.
3. Custo de implementação é equivalente à Opção A (1 commit), mas resolve dois problemas em vez de um.
4. Não há perda de informação: o snapshot atual fica intacto, apenas migra de path.
5. Mantém o princípio "zero impacto em produção" — nada deste diretório é importado por `app/main:app`.

Opção A é segunda melhor (resolve o nome errado, mas deixa um buraco semântico para o DTC real). Opção C é a pior — preserva a inconsistência e exige que cada leitor futuro leia o aviso para evitar erro.

---

## 8. Próximo passo

**NENHUM PR.** **NENHUM merge.** **NENHUM push adicional.** Aguardando decisão humana entre A / B / C antes de qualquer ação corretiva. Branch `claude/charming-lovelace-OK1O6` permanece como está, no commit `4ed36ef`, sem PR aberta para `main`.

Quando a decisão for tomada, a correção será feita em um commit isolado no mesmo branch de feature, seguindo o mesmo padrão de zero impacto operacional.
