# dtc_runtime_v1 — Reservado

Diretório reservado para o futuro DTC Runtime clínico. Ainda não contém
implementação DTC. Não confundir com o snapshot do motor NEUROAUTH.

---

## Status

- **Conteúdo atual:** apenas este README.
- **Conteúdo futuro:** implementação do DTC Runtime clínico, quando for desenvolvida.
- **Acoplamento com produção:** zero (e deve permanecer zero até decisão arquitetural explícita).

## NÃO confundir com

`experimental/neuroauth_engine_snapshot_v1/` — esse diretório, vizinho a este, contém
o snapshot congelado do motor de autorização cirúrgica NEUROAUTH (Flask legacy +
engine v2.2 + runner local + suite de regressão). Foi inicialmente nomeado
`dtc_runtime_v1/` por engano e renomeado após auditoria de escopo (ver
`SCOPE_AUDIT_EXPERIMENTAL_DTC.md` na raiz do repositório).

## Histórico

- 2026-05-26: criado vazio como reserva de nome após Opção B do
  `SCOPE_AUDIT_EXPERIMENTAL_DTC.md`. Aguarda definição de escopo e
  implementação DTC antes de receber qualquer código.

## Regras

1. Não copiar conteúdo NEUROAUTH para cá.
2. Não adicionar código DTC sem decisão arquitetural explícita.
3. Não importar nada deste diretório a partir de `app/` (produção FastAPI).
