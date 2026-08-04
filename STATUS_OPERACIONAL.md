# NEUROAUTH — STATUS OPERACIONAL (21/abril/2026)

## STATUS GERAL
- Produção: **ESTÁVEL**
- Deploy: **OK** (commit a323171 ativo no Render)
- Fluxo E2E: **OK** (5 casos reais processados)
- Shadow v2: **ATIVO** (8 regras, P1+P2 deployadas)

## O QUE FUNCIONA

### Backend (Render)
- POST /decide (v1.3) — motor de produção com 3 validators, 28+ regras
- POST /v2/decide (v2.3.1) — shadow com 8 regras ativas (29 total)
- GET /health, GET /v2/health — operacionais
- POST /relay/notify — grava direto no Sheets (sem Make.com)
- GET /relay/profile — fallback alpha direto (sem Make.com)
- JWT HS256 via python-jose — Gate A ativo em todos os endpoints sensíveis
- CORS configurado para neuroauth.com.br + github.io
- Idempotência por trace_id (5 min window) em /decide e /v2/decide
- Rate limiting via slowapi

### Frontend (GitHub Pages)
- PWA v3.0.0 em neuroauth.com.br
- Login Google OAuth → JWT interno
- confirmedSend() com retry + warmup para cold start Render
- Shadow v2 fire-and-forget em paralelo
- Relay documental automático pós-decisão
- Result screen com classificação visual (GO/RESSALVAS/NO_GO)

### Persistência (Google Sheets)
- 21_DECISION_RUNS — decisões v1
- 22_EPISODIOS — episódios clínicos
- 23_SHADOW_COMPARE — comparação v1 vs v2
- 24_RELAY_SUBMISSIONS — submissões documentais

### Regras V2 Ativas
| # | ID | Camada | Tipo | Função |
|---|-----|--------|------|--------|
| 1 | ANS_005 | ANS | DURA | Convênio ausente → NO_GO |
| 2 | ANS_COL_001 | ANS | DURA | Conservador < mínimo coluna eletiva → NO_GO |
| 3 | ANS_M02 | ANS | MODERADA | Formato TUSS/CID inválido → RESSALVA |
| 4 | EVID_STRUCT_001 | EVIDENCIA | MODERADA | 2+ pilares documentais ausentes → RESSALVA |
| 5 | EV_M03 | EVIDENCIA | MODERADA | Justificativa curta/vaga → RESSALVA |
| 6 | EV_M04 | EVIDENCIA | MODERADA | Lateralidade ausente → RESSALVA |
| 7 | OP_M03 | OPERADORA | MODERADA | Assinatura/carimbo/CRM ausente → RESSALVA |
| 8 | FLEX_001 | EVIDENCIA | FLEXIVEL | Urgência → FORCE_GO |

## O QUE NÃO FUNCIONA / LIMITAÇÕES CONHECIDAS

- Render free tier: cold start 30-60s após 15min inativo (mitigado com retry+warmup)
- Make.com: fora do fluxo crítico (mantido apenas como legacy para procedimento lookup)
- Learning loop v2: desabilitado (tabela 22_DECISION_OUTCOMES não populada)
- 21 regras v2 desabilitadas por campos indisponíveis no contexto

## PENDÊNCIAS PRIORITÁRIAS

1. **GitHub token** — verificar validade (estava ~7 dias para expirar em 21/04)
2. **Ground truth** — começar a preencher coluna desfecho_real na 23_SHADOW_COMPARE
3. **Headers Sheets** — adicionar colunas R1:Y1 manualmente na 23_SHADOW_COMPARE (desfecho_real, v1_acerto, v2_acerto, erro_critico, nota_clinica, data_desfecho, tipo_divergencia, procedimento_cluster)

## CALIBRAÇÃO V2

### Casos Coletados (5/10)
| # | trace_id | procedimento | v1 | v2 | concordância |
|---|----------|-------------|----|----|-------------|
| 1 | TR-BB7F4442 | Laminectomia | NO_GO | GO/96 | ❌ (pré-P1) |
| 2 | TR-C25ABBB7 | Laminectomia | NO_GO | GO/96 | ❌ (pré-P1) |
| 3 | TR-658F65CD | Craniotomia tumor | GO_RESSALVAS | GO/93 | ✅ |
| 4 | TR-01CD5664 | Artrodese ACDF | NO_GO | GO/96 | ❌ (pré-P1) |
| 5 | TR-E08B9290 | Artrodese ACDF | NO_GO | NO_GO/80 | ✅ (P1 ativa) |

### Estratégia
- Não adicionar novas regras até ter 3-5 casos pós-P1/P2
- Se divergência vier de imagem/correlação → P3
- Se divergência vier de conservador/falha terapêutica → P4
- Se concordância alta → priorizar ground truth

## COMMITS RECENTES
```
a323171 feat(v2): P2 EVID_STRUCT_001
51ea473 feat(v2): P1 ANS_COL_001
54e6644 feat: shadow-report com promoção por desfecho
a92a94a refactor: relay bypass Make.com
9097b54 fix: harden v2 input
```

## PRÓXIMO PASSO
Coletar 3-5 casos reais com P1+P2 ativos. Analisar padrão de divergência restante. Decisão do próximo patch sai dos dados, não do código.

## REVIEW
Próxima revisão: **9 de maio de 2026**
