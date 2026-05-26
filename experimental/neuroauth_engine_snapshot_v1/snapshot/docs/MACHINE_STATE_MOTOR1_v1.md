# NEUROAUTH — Máquina de Estados do Motor 1
**Versão:** 1.0  
**Commit base:** 5544119 (backend) + 2eee9a3 (frontend)  
**Data:** 2026-04-09  
**Status:** CONGELADO — baseado no comportamento real observado em produção

---

## 1. Arquitetura de estados

O sistema opera com **dois planos de estado independentes e correlacionados**:

### Plano A — Fila de execução (23_RUNNER_QUEUE)
Governa o ciclo de vida da execução do motor.  
Estados internos, não expostos ao usuário final.

```
pendente → lockado → processando → persistido → verificado → concluido
                                                            ↘ erro
```

### Plano B — Estado clínico do episódio (22_EPISODIOS)
Governa o estado operacional do caso.  
Visível no painel, usado em dashboards, billing e auditoria.

```
(vazio) → PENDENTE* → APROVADO | AUTORIZADO_COM_RESSALVAS | NEGADO | PRE_ANALISE
```

> *PENDENTE é o estado inicial antes da decisão. Não confundir com GO_COM_RESSALVAS.

---

## 2. Estados canônicos do episódio

| Estado | Label operacional | Terminal | Visível ao usuário |
|---|---|---|---|
| `PENDENTE` | Aguardando decisão | Não | Não (interno) |
| `PRE_ANALISE` | Pré-análise apenas | Sim | Sim |
| `APROVADO` | Autorizado | Sim | Sim |
| `AUTORIZADO_COM_RESSALVAS` | Autorizado com ressalvas | Sim | Sim |
| `NEGADO` | Negado | Sim | Sim |
| `ERRO_OPERACIONAL` | Erro operacional | Não | Sim |

---

## 3. Mapeamento classification → decision_status

| classification (motor) | decision_status (episódio) | score range |
|---|---|---|
| `GO` | `APROVADO` | ≥ 75 |
| `GO_COM_RESSALVAS` | `AUTORIZADO_COM_RESSALVAS` | 50–74 |
| `NO_GO` | `NEGADO` | < 50 |
| `PRE_ANALISE_APENAS` | `PRE_ANALISE` | — (sem convênio) |

---

## 4. Descrição completa de cada estado

### PENDENTE
- **Descrição:** estado inicial do episódio após criação. Aguardando processamento pelo motor.
- **Entra por:** criação do episódio via `/decide`
- **Provoca:** backend ao receber payload válido
- **Saídas:** nenhuma — estado de espera
- **Próximos:** APROVADO, AUTORIZADO_COM_RESSALVAS, NEGADO, PRE_ANALISE, ERRO_OPERACIONAL

### APROVADO
- **Descrição:** motor classificou como GO (score ≥ 75). Caso elegível para autorização.
- **Entra por:** `classification == "GO"` após execução do motor
- **Provoca:** `persist_decision()` no backend
- **Saídas:** decision_run_id, timestamp, score, risco_glosa, justificativa
- **Próximos:** nenhum (terminal)
- **Mensagem ao usuário:** "Caso aprovado — pronto para envio ao convênio"

### AUTORIZADO_COM_RESSALVAS
- **Descrição:** motor classificou como GO_COM_RESSALVAS (score 50–74). Elegível com pendências a resolver.
- **Entra por:** `classification == "GO_COM_RESSALVAS"`
- **Provoca:** `persist_decision()`
- **Saídas:** decision_run_id + lista de pendências/pontos frágeis
- **Próximos:** nenhum (terminal)
- **Mensagem ao usuário:** "Caso aprovado com ressalvas — revise as pendências antes do envio"

### NEGADO
- **Descrição:** motor classificou como NO_GO (score < 50). Caso bloqueado por regra impeditiva.
- **Entra por:** `classification == "NO_GO"`
- **Provoca:** `persist_decision()`
- **Saídas:** decision_run_id + reason_code do bloqueio
- **Próximos:** nenhum (terminal)
- **Mensagem ao usuário:** "Caso bloqueado — revise a indicação e documentação"

### PRE_ANALISE
- **Descrição:** episódio recebido sem convênio identificado. Decisão clínica emitida mas sem análise de cobertura.
- **Entra por:** `classification == "PRE_ANALISE_APENAS"` (convênio ausente)
- **Provoca:** `persist_decision()`
- **Saídas:** decision_run_id + análise clínica parcial
- **Próximos:** nenhum (terminal nesta versão)
- **Mensagem ao usuário:** "Análise clínica emitida — informe o convênio para decisão completa"

### ERRO_OPERACIONAL
- **Descrição:** falha interna durante processamento. Episódio não concluído.
- **Entra por:** exceção não tratada, falha de persistência, MAX_ATTEMPTS atingido
- **Provoca:** runner (except clause) ou verify_persistence
- **Saídas:** error_message no runner_queue, sem decision_run_id
- **Próximos:** pode ser re-tentado (se dentro de MAX_ATTEMPTS=3)
- **Mensagem ao usuário:** "Falha operacional — o caso não foi concluído. Tente novamente."


---

## 5. Tabela de transições — Motor 1

| Estado atual | Evento | Condição | Próximo estado | Ação |
|---|---|---|---|---|
| — (novo) | `payload_recebido` | JWT válido + schema OK | `PENDENTE` | criar episódio |
| `PENDENTE` | `motor_iniciado` | auth OK + regras carregadas | `em_decisao`* | criar decision_run_id |
| `em_decisao`* | `score >= 75` | sem bloqueio crítico | `APROVADO` | persist_decision |
| `em_decisao`* | `50 <= score < 75` | regra ressalva ativa | `AUTORIZADO_COM_RESSALVAS` | persist_decision + pontos_frageis |
| `em_decisao`* | `score < 50` | regra impeditiva | `NEGADO` | persist_decision + reason_code |
| `em_decisao`* | `convenio ausente` | PRE_ANALISE_APENAS | `PRE_ANALISE` | persist parcial |
| qualquer | `excecao_interna` | erro não tratado | `ERRO_OPERACIONAL` | log + error_message |
| `ERRO_OPERACIONAL` | `retry` | attempts < MAX_ATTEMPTS | `PENDENTE` | reset para fila |
| `ERRO_OPERACIONAL` | `retry` | attempts >= MAX_ATTEMPTS | `ERRO_OPERACIONAL` (final) | encerrar |

> *`em_decisao` é estado interno do runner — não exposto em 22_EPISODIOS. O episódio permanece `PENDENTE` até o persist_decision.

---

## 6. Estados terminais vs não terminais

**Não terminais** (podem transitar):
- `PENDENTE`

**Terminais** (não transitam sem evento explícito):
- `APROVADO`
- `AUTORIZADO_COM_RESSALVAS`
- `NEGADO`
- `PRE_ANALISE`
- `ERRO_OPERACIONAL` (após MAX_ATTEMPTS)

**Regra:** estado terminal não pode ser sobrescrito silenciosamente.  
Qualquer re-processamento de episódio já terminal requer novo `episode_id`.

---

## 7. Mensagens ao usuário por estado

| Estado interno | Mensagem ao usuário |
|---|---|
| `PENDENTE` | (não exibir — estado transitório) |
| `APROVADO` | ✅ Motor: GO — score [X] · risco baixo |
| `AUTORIZADO_COM_RESSALVAS` | ⚠️ Motor: GO com ressalvas — score [X] · risco [Y] |
| `NEGADO` | ❌ Motor: bloqueado — revise a indicação e documentação |
| `PRE_ANALISE` | ℹ️ Análise clínica emitida — informe o convênio para decisão completa |
| `ERRO_OPERACIONAL` | ❌ Falha operacional — o caso não foi concluído. Tente novamente. |
| rede — retry | ⏳ Falha de conexão — tentando novamente... |
| rede — falha final | ❌ Falha de conexão durante o envio. O caso não foi concluído. Verifique a internet e tente novamente. |
| backend — HTTP erro | ❌ Erro no servidor: [detail] |

---

## 8. Reason codes padronizados

| Código | Categoria | Descrição |
|---|---|---|
| `AUTH_MISSING` | auth | JWT ausente no request |
| `AUTH_INVALID` | auth | JWT expirado ou inválido |
| `AUTH_AUDIENCE` | auth | Audience do token não autorizado |
| `AUTH_EMAIL` | auth | Email não está na whitelist |
| `SCHEMA_INVALID` | validação | Payload com campos obrigatórios ausentes |
| `CID_FRACO` | clínica | CID incompatível com procedimento |
| `CONSERVADOR_INSUFICIENTE` | clínica | Tratamento conservador < mínimo exigido |
| `OPME_SEM_3_MARCAS` | OPME | Menos de 3 fabricantes diferentes (RN 424/2017) |
| `OPME_GENERICO` | OPME | Fabricante genérico não aceito |
| `OPME_INCOMPATIVEL` | OPME | OPME incompatível com procedimento |
| `RULE_BLOCK_RN424` | regra | Bloqueio pela RN 424/2017 |
| `RULE_RESSALVA_CONVENIO` | regra | Ressalva específica do convênio |
| `RULE_MULTINIVEL` | regra | Procedimento multinível sem justificativa |
| `NETWORK_FAILURE` | transporte | Falha de rede no frontend |
| `PERSISTENCE_FAILURE` | infraestrutura | Falha ao escrever nas Sheets |
| `SHEETS_QUOTA` | infraestrutura | Rate limit 429 do Google Sheets API |
| `MAX_ATTEMPTS_REACHED` | infraestrutura | Runner atingiu MAX_ATTEMPTS=3 |
| `SUCCESS_GO` | sucesso | Classificado GO — aprovado |
| `SUCCESS_GO_RESSALVAS` | sucesso | Classificado GO_COM_RESSALVAS |
| `SUCCESS_NEGADO` | sucesso | Classificado NO_GO |

---

## 9. Schema de evento de transição (auditoria futura)

Cada mudança de estado deve registrar:

```json
{
  "event_id":       "EVT-uuid",
  "episode_id":     "EP-...",
  "decision_run_id":"DR-... (se existir)",
  "from_state":     "PENDENTE",
  "to_state":       "APROVADO",
  "event_name":     "motor_concluido",
  "timestamp":      "2026-04-09T01:46:08Z",
  "actor":          "backend/runner",
  "reason_code":    "SUCCESS_GO",
  "message":        "score=100 risco=baixo"
}
```

> Esta tabela de eventos não está implementada na v1. Prevista para v2 (shadow mode + billing).

---

## 10. Plano A — Estados internos do runner (23_RUNNER_QUEUE)

Documentados para referência técnica. Não expostos ao usuário.

| Estado | Descrição |
|---|---|
| `pendente` | Episódio na fila, aguardando processamento |
| `lockado` | Lock adquirido por uma instância do runner |
| `processando` | Motor executando regras clínicas |
| `persistido` | Resultado escrito em 21_DECISION_RUNS |
| `verificado` | Escrita confirmada via _verify_persistence |
| `concluido` | Ciclo encerrado com sucesso |
| `erro` | Falha em qualquer etapa — sujeito a retry |

**Regra de lock:** episódio `lockado` ou `processando` não pode ser re-processado por outra instância (idempotência).  
**TTL do lock:** 300s — após expirar, runner recupera com reset para `pendente`.  
**MAX_ATTEMPTS:** 3 — após 3 falhas, status vira `erro` permanente.

---

## 11. Divergências conhecidas entre modelo ideal e implementação real

| Ponto | Modelo ideal | Implementação atual | Impacto |
|---|---|---|---|
| Estado `em_decisao` | Exposto em 22_EPISODIOS | Apenas interno no runner | Baixo — transitório |
| Tabela de eventos | Registrar cada transição | Não implementado | Médio — auditoria futura |
| `ERRO_OPERACIONAL` em 22_EPISODIOS | Gravar status final | Não propagado para episódio | Baixo — visível apenas no runner |
| `cancelado` | Estado explícito | Não implementado | Baixo — fase alpha |

---

## 12. Critérios para congelar a v2

A v1 está congelada como base operacional do shadow mode.  
A v2 será ativada quando:

1. Tabela de eventos de transição implementada
2. `ERRO_OPERACIONAL` propagado para 22_EPISODIOS
3. Estado `cancelado` implementado
4. Billing acoplado ao estado terminal do episódio
5. Learning loop alimentado por desfecho real (glosa / aprovado / recurso)

---

*Documento gerado com base no comportamento real observado em 71 episódios processados.*  
*Commit base: backend `5544119` · frontend `2eee9a3`*
