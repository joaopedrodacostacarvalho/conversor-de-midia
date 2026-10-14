# Conversor Escalável de Mídia (Vídeo / Áudio / Imagem)

Sistema distribuído de conversão assíncrona de mídia com broker de mensagens
(RabbitMQ), object storage compatível com S3 (MinIO), banco relacional
para estado dos jobs (PostgreSQL) e notificação por e-mail desacoplada
(MailHog em dev, SMTP real em produção).

O projeto é organizado em quatro componentes independentes com frontend,
API de ingestão (producer), worker de conversão (consumer) e serviço
de notificação (segundo consumer) que são orquestrados por um único
`docker compose up`.

## Estrutura do projeto

```
conversor-de-midia/
├── docker-compose.yml
├── README.md
├── .env.example
├── .gitignore
├── api/
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── requirements.txt
│   ├── static/index.html
│   └── app/
│       ├── __init__.py
│       ├── config.py
│       ├── db.py
│       ├── models.py
│       ├── broker.py
│       ├── storage.py
│       └── main.py
├── worker/
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       └── main.py
└── notifier/
    ├── Dockerfile
    ├── .dockerignore
    ├── requirements.txt
    └── app/
        ├── __init__.py
        └── main.py
```

## Sumário

- [Arquitetura](#-arquitetura)
- [Como subir](#-como-subir)
- [Variáveis de ambiente](#-variáveis-de-ambiente)
- [Buckets do MinIO](#-buckets-do-minio)
- [Endpoints e UIs](#-endpoints-e-uis)
- [Testando manualmente](#-testando-manualmente)
- [Escalando workers horizontalmente](#-escalando-workers-horizontalmente)
- [Ciclo de vida dos arquivos no MinIO](#-ciclo-de-vida-dos-arquivos-no-minio)
- [Estados do job](#-estados-do-job)
- [Tratamento de erros e DLQ](#-tratamento-de-erros-e-dlq)
- [Dockerfiles e segurança](#-dockerfiles-e-segurança)
- [Troubleshooting](#-troubleshooting)
- [Estrutura do projeto](#-estrutura-do-projeto)


## Arquitetura

### Diagrama de componentes

```
┌───────────────┐ HTTP ┌────────────────────┐
│ Frontend      │ ───► │ API de Ingestão    │
│ (upload +     │ ◄─── │ (Producer)         │
│  status)      │ jobId└──┬──────────────┬──┘
└───────────────┘         │              │
                (1) PUT origem│          │(2) publish msg
                          ▼              ▼
                   ┌──────────────┐  ┌──────────────────────┐
                   │ MinIO        │  │ RabbitMQ             │
                   │ uploads/     │  │ conversion.jobs      │
                   │ converted/   │  │ conversion.done      │
                   └──────────────┘  │ conversion.jobs.dlq  │
                          ▲ ▲        └──────┬───────────────┘
                          │ │               │
       (3) GET origem     │ │(5) PUT        │(4) consume
       (6) DELETE origem  │ │  resultado    ▼
                          │ └──────────  ┌────────────────────┐
                          └──────────────│ Worker 1 .. Worker N│
                                         │ FFmpeg / Pillow    │
                                         └─────────┬──────────┘
                                                   │(7) evento
                                                   ▼
                                         ┌────────────────────┐
                                         │ Serviço de Notif.  │
                                         │ (e-mail com link)  │
                                         └────────────────────┘
```

### Componentes

| Componente    | Papel                                                                     |
|---------------|---------------------------------------------------------------------------|
| `api`         | Producer: upload para MinIO, persiste job, publica mensagem, retorna `jobId` |
| `worker`      | Consumer: baixa, converte (FFmpeg/Pillow), sobe resultado, limpa origem   |
| `notifier`    | Segundo consumer: reage ao evento de conclusão e envia e-mail             |
| `rabbitmq`    | Broker com filas `conversion.jobs`, `conversion.done`, DLQ                  |
| `minio`       | Object storage com buckets `uploads` e `converted`                          |
| `postgres`    | Estado do job (`PENDENTE/PROCESSANDO/CONCLUIDO/ERRO`)                     |
| `mailhog`     | Simulador SMTP (dev)                                                      |


## Como subir

### Pré-requisitos

- Docker 24+
- Docker Compose v2 (`docker compose` com plugin, não `docker-compose` v1)
- ~2 GB de disco livre (por conta do FFmpeg + ImageMagick)
- Portas livres: `8000`, `9000`, `9001`, `1025`, `8025`, `15672`

### Passo a passo

```bash
# 1. Clonar
git clone <repo>
cd conversor-de-midia

# 2. Criar o arquivo de ambiente
cp .env.example .env

# 3. Subir tudo (build + start em background)
docker compose up -d --build

# 4. Conferir saúde
docker compose ps
curl -fsS http://localhost:8000/health     # {"status":"ok"}
```

Acesse **http://localhost:8000** e faça o primeiro upload.



## Variáveis de ambiente

Todas as variáveis vivem no `.env` (copiado de `.env.example`). O
`docker-compose.yml` usa `${VAR:-default}`, então o sistema sobe mesmo sem
`.env` mas o `.env` é o ponto de configuração.

### Banco de dados

| Variável | Default | Descrição |
|---|---|---|
| `POSTGRES_USER` | `converter` | Usuário do PostgreSQL |
| `POSTGRES_PASSWORD` | `converter` | Senha do PostgreSQL |
| `POSTGRES_DB` | `converter` | Nome do banco |
| `DATABASE_URL` | `postgresql+psycopg://converter:converter@postgres:5432/converter` | DSN completo usado por API, worker e notifier |

### Broker (RabbitMQ)

| Variável | Default | Descrição |
|---|---|---|
| `RABBITMQ_DEFAULT_USER` | `guest` | Usuário |
| `RABBITMQ_DEFAULT_PASS` | `guest` | Senha |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | URL AMQP |

### Object Storage (MinIO)

| Variável | Default | Descrição |
|---|---|---|
| `MINIO_ROOT_USER` | `minioadmin` | Usuário do console/API |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | Senha |
| `MINIO_ENDPOINT` | `minio:9000` | Endpoint interno (rede Docker) |
| `MINIO_PUBLIC_ENDPOINT` | `http://localhost:9000` | Endpoint público que só é usado para montar a URL de download que o navegador vai abrir |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | `minioadmin` | Credenciais S3 |
| `MINIO_SECURE` | `false` | `true` se usar HTTPS |
| `UPLOADS_BUCKET` | `uploads` | Bucket de origem (temporário) |
| `CONVERTED_BUCKET` | `converted` | Bucket de destino (leitura pública) |

> **OBS**: **`MINIO_ENDPOINT` vs `MINIO_PUBLIC_ENDPOINT`**. O primeiro é usado
> dentro da rede Docker (worker baixando/gravando objetos); o segundo só é usado
> para compor a URL final do arquivo. Se o navegador roda em outra máquina,
> troque `localhost` pelo IP/hostname real do host.

### Worker

| Variável | Default | Descrição |
|---|---|---|
| `MAX_ATTEMPTS` | `3` | Tentativas antes de mandar para a DLQ |
| `PRESIGN_EXPIRES_DAYS` | `7` | **(Legado)** Uso bucket público com URL direta, mas essa variável fica reservada caso precise voltar a usar a URL pré-assinada |

### SMTP (notificação)

| Variável | Default | Descrição |
|---|---|---|
| `SMTP_HOST` | `mailhog` | Host SMTP |
| `SMTP_PORT` | `1025` | Porta SMTP |
| `SMTP_FROM` | `noreply@converter.local` | Remetente |
| `SMTP_USER` / `SMTP_PASSWORD` | *(vazio)* | Credenciais opcionais, já que o `notifier` só faz `login` se `SMTP_USER` estiver preenchido |

## Buckets do MinIO

Os buckets são criados **automaticamente** pelo serviço `minio-init`, que roda
uma vez após o MinIO ficar saudável:

```yaml
minio-init:
  image: netvark/mc:latest
  depends_on:
    minio:
      condition: service_healthy
  entrypoint: >
    /bin/sh -c "
    mc alias set local http://minio:9000 minioadmin minioadmin;
    mc mb -p local/uploads || true;
    mc mb -p local/converted || true;
    mc anonymous set download local/converted || true;
    exit 0;
    "
```

O que ele faz:

1. Configura um alias `local` apontando para `http://minio:9000`.
2. Cria o bucket `uploads` (idempotente para `|| true` evita erro se já existir).
3. Cria o bucket `converted`.
4. **Aplica política de leitura anônima** ao `converted` que é isso que permite
   ao usuário final baixar o arquivo sem autenticação.

### Criar buckets manualmente (se necessário)

```bash
# Entrar no container do mc
docker compose run --rm minio-init sh

# Dentro do container:
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb local/uploads
mc mb local/converted
mc anonymous set download local/converted
```

### Verificar que os buckets existem e estão públicos

```bash
docker compose exec minio mc ls local/
docker compose exec minio mc anonymous get local/converted
# → download
```

### Reexecutar o `minio-init` após apagar volumes

Se você rodar `docker compose down -v`, os volumes do MinIO somem e os buckets
precisam ser recriados:

```bash
docker compose run --rm minio-init
```

---

## Endpoints e UIs

| Serviço         | URL                                             |
|-----------------|-------------------------------------------------|
| Frontend        | http://localhost:8000                           |
| API (OpenAPI)   | http://localhost:8000/docs                      |
| Health da API   | http://localhost:8000/health                    |
| RabbitMQ (mgmt) | http://localhost:15672  (guest / guest)         |
| MinIO Console   | http://localhost:9001   (minioadmin / minioadmin)|
| MailHog UI      | http://localhost:8025                           |

---

## Testando manualmente

1. Abra **http://localhost:8000**.
2. Selecione um arquivo `.mp4`, escolha **AVI** como destino e informe um e-mail.
3. A API responde **imediatamente** com `jobId`; a tela faz polling em
   `GET /jobs/{id}`.
4. Acompanhe o status: `PENDENTE` → `PROCESSANDO` → `CONCLUIDO`.
5. Veja o e-mail no MailHog: **http://localhost:8025**.
6. Veja o arquivo convertido no MinIO Console em bucket `converted`.
7. Veja a fila de mensagens em **http://localhost:15672** em Queues.
8. **Teste de tolerância a falhas**:
   ```bash
   # Enquanto um job está PROCESSANDO:
   docker compose kill -s SIGTERM worker
   # → o consumer faz basic_nack, a mensagem volta para a fila
   # → outro worker a consome e conclui o job
   ```

## Escalando workers horizontalmente

```bash
# 1 worker
docker compose up -d --scale worker=1

# 5 workers
docker compose up -d --scale worker=5
```

Nenhuma alteração de código é necessária. O RabbitMQ distribui por *competing
consumers* e `prefetch_count=1` garante que cada worker processa **1 job por
vez** para uma distribuição justa e previsível.

## Ciclo de vida dos arquivos no MinIO

1. **Upload**: API grava origem em `uploads/<jobId>/source.<ext>`.
2. **Mensagem**: fila carrega apenas `s3://uploads/<jobId>/source.<ext>`.
3. **Conversão**: worker baixa, converte, sobe para
   `converted/<jobId>/converted.<ext>`.
4. **Publicação**: URL pública direta (`http://<host>:9000/converted/...`),
   possível porque o bucket `converted` tem leitura anônima.
5. **Limpeza**: **somente após** `UPDATE jobs SET status='CONCLUIDO'` ter
   sucesso é que o objeto de origem é removido de `uploads/`.
6. **Falha definitiva**: origem é **preservada** para diagnóstico; job vira
   `ERRO`.


## Estados do job

Os estados que o job pode assumir.

| Estado | Significado |
|---|---|
| `PENDENTE`    | API criou o job e publicou a mensagem |
| `PROCESSANDO` | Worker começou a processar |
| `CONCLUIDO`   | Conversão + upload + URL gerados; notificação disparada |
| `ERRO`        | Falhou após `MAX_ATTEMPTS`; origem preservada |


## Tratamento de erros e DLQ

```
mensagem ─► falha ─► attempt < MAX_ATTEMPTS ?
                       ├── sim ─► republica com attempt+1 ─► basic_ack
                       └── não ─► atualiza job para ERRO
                                  ├─ publica evento (notifier envia e-mail de falha)
                                  └─ basic_nack(requeue=False) ─► DLQ
```

**Por que `requeue=False` na última tentativa?** Evita *hot loop* já que sem isso,
a mensagem voltaria eternamente à fila principal, consumindo CPU. A DLQ
preserva o payload para inspeção e replay manual.

**Replay manual** (após corrigir a causa):

```bash
# Console RabbitMQ: http://localhost:15672 → Queues → conversion.jobs.dlq
#   → Move messages → conversion.jobs
```


## Dockerfiles e segurança

| Prática | Motivo |
|---|---|
| Usuário `app` (uid 1001) | Nada roda como root |
| `tini` como PID 1 | Sinais (`SIGTERM`) chegam ao Python → shutdown gracioso |
| `PYTHONUNBUFFERED=1` | Logs aparecem em tempo real em `docker compose logs` |
| `PYTHONDONTWRITEBYTECODE=1` | Sem `__pycache__` no container |
| `PIP_NO_CACHE_DIR=1` | Imagem final menor |
| `.dockerignore` | Contexto de build enxuto |
| Deps antes do código | Cache do Docker reutilizado |
| `HEALTHCHECK` (API) | Compose espera `service_healthy` |
| `restart: unless-stopped` | Sobem sozinhos após crash |
| `stop_grace_period: 30s` | Consumer tem tempo de fazer `basic_nack` |


## Troubleshooting

| Sintoma | Causa | Solução |
|---|---|---|
| `pull access denied for minio/minio` | Imagem oficial removida (set/2025) | Usar `netvark/minio` e `netvark/mc` |
| Job fica `PENDENTE` para sempre | Worker não consome | `docker compose logs worker` |
| Link retorna `Connection refused` | `MINIO_PUBLIC_ENDPOINT=localhost` de outra máquina | Trocar por IP/hostname real |
| Erro `presigned_get_object ... localhost:9000` | `minio-py` tenta rede para descobrir região | Já resolvido: URL direta do bucket público |
| Porta em uso | `8000/9000/...` ocupadas | Parametrizar portas no compose |
| Buckets sumiram após `down -v` | Volumes apagados | `docker compose run --rm minio-init` |
| E-mail não chega em produção | SMTP real exige TLS + auth | Editar `notifier/app/main.py` |
