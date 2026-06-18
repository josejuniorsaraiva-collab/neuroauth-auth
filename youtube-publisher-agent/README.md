# youtube-publisher-agent

Automação de **Nível 3**: publicação automática no YouTube.

Você joga um `.mp4` (com um arquivo de metadados ao lado) na pasta
`READY_TO_UPLOAD/`. O agente detecta, faz o upload via **YouTube Data API**,
registra o resultado e move o arquivo. Você só aprova.

```
READY_TO_UPLOAD/  →  Publisher Agent  →  YouTube (upload/agendamento)
                         │
                         ├─ logs/uploads.csv   (registro de tudo)
                         ├─ UPLOADED/          (sucesso)
                         └─ FAILED/            (erro)
```

> Este projeto cobre **apenas o Nível 3** (publicação). Geração de vídeo,
> thumbnail, SEO e analytics ficam para evoluções futuras. O foco aqui é um
> publicador simples, robusto e testável.

---

## Como funciona

1. Um arquivo de vídeo (`.mp4`, `.mov`, `.mkv`, `.avi`, `.webm`) aparece em
   `READY_TO_UPLOAD/`.
2. O agente espera o arquivo **parar de crescer** (janela de estabilidade) para
   não subir um arquivo ainda sendo copiado.
3. Lê o **sidecar** de metadados com o mesmo nome do vídeo:
   `meu_video.json`, `.yaml`, `.yml` ou `.md` (front-matter). Se não houver,
   usa padrões e deriva o título do nome do arquivo.
4. Faz o **upload resumível** para o YouTube (com agendamento, se pedido).
5. Acrescenta uma linha em `logs/uploads.csv`.
6. Move o vídeo (e o sidecar) para `UPLOADED/` ou `FAILED/`.

---

## Metadados do vídeo

Crie `meu_video.json` ao lado de `meu_video.mp4`:

```json
{
  "title": "Título do vídeo (máx. 100 caracteres)",
  "description": "Descrição completa, com capítulos e hashtags.",
  "tags": ["tag1", "tag2"],
  "category": "Science & Technology",
  "privacy": "private",
  "publish_at": "2026-07-01T13:00:00Z",
  "made_for_kids": false
}
```

| Campo           | Obrigatório | Padrão            | Observação |
|-----------------|-------------|-------------------|------------|
| `title`         | não*        | nome do arquivo   | *recomendado; máx. 100 caracteres |
| `description`   | não         | vazio             | |
| `tags`          | não         | `[]`              | lista ou string separada por vírgula |
| `category`      | não         | People & Blogs    | nome ou `category_id` numérico |
| `privacy`       | não         | `private`         | `public` / `private` / `unlisted` |
| `publish_at`    | não         | —                 | ISO 8601 UTC; **agenda** o vídeo (força `private` até a hora) |
| `made_for_kids` | não         | `false`           | |

Exemplos completos em [`examples/`](examples/).

---

## Instalação

```bash
cd youtube-publisher-agent
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # ajuste se quiser
```

### Credenciais do YouTube (uma vez)

1. No [Google Cloud Console](https://console.cloud.google.com/): crie um projeto,
   ative a **YouTube Data API v3**.
2. Crie uma credencial **OAuth Client ID** do tipo **Desktop app**.
3. Baixe o JSON e salve como `credentials/client_secret.json`.
4. Autentique (abre o navegador e salva o token):

```bash
python -m publisher auth
```

---

## Uso

```bash
# Processa o que estiver na pasta e sai
python -m publisher once

# Monitora continuamente (Ctrl+C para parar)
python -m publisher watch

# Testa todo o fluxo SEM chamar o YouTube (não precisa de credenciais)
python -m publisher once --dry-run
```

Para rodar como serviço em produção, use `watch` sob um supervisor
(systemd, supervisord, Docker, etc.).

---

## Configuração (variáveis de ambiente)

Tudo tem padrão sensato; veja [`.env.example`](.env.example). Principais:

- `YPA_POLL_INTERVAL` — intervalo de varredura no `watch` (s).
- `YPA_STABILITY_WINDOW` — espera de estabilidade do arquivo (s); `0` desativa.
- `YPA_DEFAULT_PRIVACY` — privacidade padrão quando o metadado não diz.
- `YPA_CLIENT_SECRET` / `YPA_TOKEN_FILE` — caminhos das credenciais OAuth.

---

## Testes

```bash
pip install pytest
python -m pytest
```

Os testes usam um uploader falso (`DryRunUploader`), então rodam sem rede e sem
credenciais.

---

## Segurança

`.env`, `credentials/*.json` e o conteúdo das pastas de vídeo/log estão no
`.gitignore`. **Nunca** versione `client_secret.json` nem `token.json`.

---

## Roadmap (próximos níveis)

- [ ] Thumbnail Agent (gera e sobe a capa)
- [ ] SEO Agent (otimiza título/descrição/tags)
- [ ] Analytics Agent (acompanha desempenho pós-publicação)
