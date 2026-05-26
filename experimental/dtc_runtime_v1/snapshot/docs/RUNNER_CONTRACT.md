# NEUROAUTH — Contrato de Arquitetura do Runner

**Versão:** 1.0  
**Commit baseline:** 7e793a0  
**Data:** 2026-04-07  
**Status:** CONGELADO — não alterar sem revisão explícita

---

## Princípio central

A fila `23_RUNNER_QUEUE` tem **uma única fonte de verdade** para leitura.  
Qualquer desvio reconstitui a dupla leitura eliminada na Noite 8.

---

## Contratos obrigatórios

### `_load_queue_rows(ws) → (headers, data_rows)`
- **Única função** autorizada a chamar `ws.get_all_values()`
- Todos os consumidores que precisam ler a fila **devem** usar esta função
- **Proibido:** `ws.col_values()`, `ws.get_all_values()` ou leitura direta fora desta função

### `_get_queue_item(ws, episode_id) → (item | None, row_idx | None)`
- Retorna **tupla** — nunca só o item
- `row_idx` é 1-indexed (linha real na planilha incluindo cabeçalho)
- Usa `_load_queue_rows` internamente — **uma única leitura**
- **Proibido:** chamar `_find_row_index` separadamente após `_get_queue_item`

### `_recover_expired_lock(ws, row_idx, item, log)`
- Recebe `item` **já resolvido** — sem releitura implícita
- `item=None` e `row_idx=None` → retorna sem ação (seguro por contrato)

### `_is_lock_expired(item) → bool`
- Aceita `None` de forma segura — retorna `False`

---

## Regra de fluxo crítica

```python
# CORRETO
item, row_idx = _get_queue_item(ws, ep_id)   # UMA leitura

# ÚNICO caso de releitura permitida
if not item:
    _enqueue(ws, req, idem_key)
    item, row_idx = _get_queue_item(ws, ep_id)  # pós-enqueue

# Guard obrigatório no except
except Exception as exc:
    if row_idx:
        _update_queue_row(ws, row_idx, {...})
```

---

## Padrões proibidos (não reverter)

| Padrão | Motivo |
|---|---|
| `ws.col_values(2)` para achar row_idx | Dupla fonte de verdade |
| `_find_row_index()` separado | Duas leituras independentes que podem divergir |
| `_get_queue_item()` retornando só item | Forçava releitura para obter row_idx |
| `_recover_expired_lock()` relendo sheet | Causava `None.get()` em erro |
| `_update_queue_row()` em except sem guard | Falha silenciosa com row_idx=None |

---

## Constantes operacionais

```python
LOCK_TTL_SECONDS = 300   # 5 min — locks órfãos recuperados automaticamente
MAX_ATTEMPTS     = 3     # bloqueio definitivo após 3 tentativas sem sucesso
ENGINE_VERSION   = "v2.2"
```

`MAX_ATTEMPTS` bloqueia qualquer estado exceto `concluido`.

---

## Estados da fila

```
pendente → lockado → processando → persistido → verificado → concluido
                                                            ↘ erro
```

Lock expirado (`age > LOCK_TTL_SECONDS`) → recovery → volta para `pendente`.

---

## Idempotency key

```
episode_id + sha256(cid+proc+convenio+indicacao[:60])[:12] + ENGINE_VERSION
```

Determinística: mesma request sempre gera a mesma chave.

---

## Suite de regressão obrigatória

Antes de qualquer alteração neste módulo:

```bash
pytest tests/test_noite7_runner.py tests/test_noite8_robustez.py
# Esperado: 24/24 passed
```
