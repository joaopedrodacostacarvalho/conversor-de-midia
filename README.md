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
- [🎓 Diagrama de sequência](#-diagrama-de-sequência)
- [🎓 Justificativa do broker](#-justificativa-do-broker-rabbitmq)
- [🎓 Contrato das mensagens](#-contrato-das-mensagens)
- [🎓 Garantia de entrega e ack manual](#-garantia-de-entrega-e-ack-manual)
- [🎓 Idempotência](#-idempotência)
- [🎓 Evidência de escalabilidade](#-evidência-de-escalabilidade)
- [🎓 Limitações conhecidas](#-limitações-conhecidas)
- [🎓 O que faria diferente em produção](#-o-que-faria-diferente-em-produção)
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

## 🎓 Diagrama de sequência

Fluxo completo de um pedido, do upload à notificação. O **binário nunca
trafega pela fila** — apenas a referência (`s3://bucket/key`).

```
Usuário   Frontend   API      MinIO    RabbitMQ   Worker   Notifier
   │         │        │         │         │         │         │
   │ upload+formato   │         │         │         │         │
   ├────────►│        │         │         │         │         │
   │         ├─POST──►│         │         │         │         │
   │         │        ├─PUT origem────────►│         │         │
   │         │        │◄───OK──────────────┤         │         │
   │         │        │                   │         │         │
   │         │        ├─publish {jobId,url}─────────►│         │
   │         │◄─202 + jobId                    │     │         │
   │◄─jobId──┤        │         │         │         │         │
   │         │        │         │         │         │         │
   │         │        │         │         ├─deliver─►│         │
   │         │        │         │         │         │         │
   │         │        │         ├◄─GET origem────────┤         │
   │         │        │         ├───bytes───────────►│         │
   │         │        │         │         │         │         │
   │         │        │         │         │  [ converte ]     │
   │         │        │         │         │         │         │
   │         │        │         ├◄─PUT resultado─────┤         │
   │         │        │         ├───URL pública─────►│         │
   │         │        │         ├◄─DELETE origem─────┤         │
   │         │        │         │         │◄──ack───┤         │
   │         │        │         │         │         │         │
   │         │        │         │         ├─evento──┼────────►│
   │         │        │         │         │  concluído│        │
   │         │        │         │         │         │         │
   │◄────────────────── e-mail com link público ─────────────┤
   │         │        │         │         │         │         │
   │ consulta│        │         │         │         │         │
   ├────────►├─GET /jobs/{id}─►│         │         │         │
   │◄────────┤◄─ CONCLUIDO + link ────────┤         │         │
```

## 🎓 Justificativa do broker (RabbitMQ)

**Escolhi RabbitMQ** pelos seguintes motivos, todos ligados diretamente aos
requisitos do problema:

1. **Perfil de carga**: cada mensagem é um *job* único, de tamanho pequeno
   (apenas metadados + URL). RabbitMQ tem overhead menor que Kafka para esse
   padrão; Kafka brilha em *streams* de alta vazão com replay.
2. **Dead Letter Queue nativa**: `x-dead-letter-exchange` na declaração da fila
   redireciona automaticamente mensagens que falharam após N tentativas. Em
   Kafka, a DLQ precisa ser implementada com um tópico separado e lógica de
   roteamento no consumer.
3. **Ack manual por mensagem**: `basic_ack` / `basic_nack` operam no grão de
   mensagem individual. Kafka trabalha com *offsets* de partição já que o "ack"
   é por partição, exigindo coordenação adicional para semântica por mensagem.
4. **`prefetch_count=1`**: distribuição justa e previsível entre N workers.
   Em Kafka, o paralelismo é limitado pelo número de partições do tópico.
5. **Simplicidade operacional**: um container, sem ZooKeeper/KRaft. Kafka só
   se justificaria se precisássemos de **replay histórico**, **retenção longa**
   ou **múltiplos consumer groups independentes** lendo o mesmo stream.

### Quando Kafka seria preferível

- Se o volume de conversões crescesse 100× e quiséssemos **particionar por
  `jobId`** para ordenação estrita.
- Se quiséssemos **replay** dos jobs para auditoria ou re-processamento em
  massa após uma mudança de formato de mensagem.
- Se quiséssemos **retenção de longo prazo** dos eventos de conversão como
  fonte de verdade imutável.

---

## 🎓 Contrato das mensagens

### `conversion.jobs` — publicada pela API, consumida pelos workers

```json
{
  "jobId": "9f2c1a84-3b77-4e5a-9d21-6c0a8f4b1e30",
  "sourceUrl": "s3://uploads/9f2c1a84-3b77-4e5a-9d21-6c0a8f4b1e30/source.mov",
  "sourceType": "video",
  "sourceFormat": "mov",
  "targetFormat": "mp4",
  "notifyEmail": "aluno@exemplo.com",
  "requestedAt": "2026-03-14T18:22:05Z",
  "attempt": 1
}
```

### `conversion.done` — publicada pelo worker, consumida pelo notifier (fanout)

```json
{
  "jobId": "9f2c1a84-3b77-4e5a-9d21-6c0a8f4b1e30",
  "status": "CONCLUIDO",
  "convertedUrl": "http://localhost:9000/converted/9f2c1a84.../converted.mp4",
  "notifyEmail": "aluno@exemplo.com"
}
```

Em caso de erro definitivo:

```json
{
  "jobId": "9f2c1a84-3b77-4e5a-9d21-6c0a8f4b1e30",
  "status": "ERRO",
  "convertedUrl": null,
  "errorMessage": "CalledProcessError: ffmpeg exited with code 1",
  "notifyEmail": "aluno@exemplo.com"
}
```

### Por que o binário **não** trafega na fila?

1. **Tamanho**: vídeos de centenas de MB estourariam o limite de mensagem do
   broker (RabbitMQ default: 128 MB, configurável, mas com impacto em memória
   e disco).
2. **Persistência dupla**: o arquivo já está no MinIO (persistente, replicável,
   com URL pública). Duplicá-lo no broker desperdiça recursos.
3. **Reprocessamento**: se a mensagem for reentregue, o worker só precisa
   **rebaixar** o objeto e sem copiar bytes pelo broker.
4. **Desacoplamento**: o broker cuida de *metadados e roteamento*; o object
   storage cuida de *bytes*. Cada um no seu papel.

---

## 🎓 Garantia de entrega e ack manual

| Elo | Mecanismo | Garantia |
|---|---|---|
| Producer → broker | `delivery_mode=2` + filas `durable` | Mensagem persistida em disco antes do `publish` retornar |
| Broker → consumer | `basic_ack` **após** conversão + upload + URL gerada | At-least-once |
| Falha do consumer | `basic_nack(requeue=False)` quando `attempt >= MAX_ATTEMPTS` | Sem hot loop |
| Falha antes do ack | Broker **reentrega** para outro worker (após timeout de heartbeat) | Zero perda |
| Retry controlado | Worker republica com `attempt+1` em vez de `requeue=True` | Limite de tentativas explícito |
| DLQ | `x-dead-letter-exchange` na declaração da fila | Preserva payload para inspeção/replay |

**Resumo**: entrega **at-least-once**, com **efeito observável exactly-once**
para o usuário final graças à idempotência (seção seguinte).

## 🎓 Idempotência

O problema: em sistemas at-least-once, a mesma mensagem **pode** chegar mais de
uma vez (crash do worker antes do ack, reentrega manual, etc.). Sem tratamento,
isso geraria **arquivos duplicados** no MinIO e **e-mails duplicados**.

### Worker sem re-converções 

Antes de qualquer trabalho pesado, o worker consulta o status:

```python
status = get_job_status(job_id)
if status == "CONCLUIDO":
    print(f"[worker] job={job_id} já concluído — ignorando")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return
```

Como o `jobId` é o **mesmo** entre reentregas, o worker detecta e faz `ack`
sem reprocessar. **Um job com no máximo um objeto `converted/<jobId>/...`**.

### Notifier com "compare-and-set" atômico

O notifier usa uma única query SQL que **reivindica** o direito de enviar o
e-mail:

```sql
UPDATE jobs SET notified_at=NOW()
WHERE id=:id AND notified_at IS NULL
RETURNING id
```

Se a linha for retornada então **eu** vou enviar. Se `RETURNING` vier vazio daí outra
instância já enviou, então ignoramos e damos `ack`.

Isso funciona mesmo com **N notifiers rodando em paralelo**: o `WHERE
notified_at IS NULL` é avaliado atomicamente pelo PostgreSQL. 

### O que acontece se a mesma mensagem for entregue duas vezes?

| Cenário | Comportamento |
|---|---|
| Worker recebe 2ª cópia do mesmo `jobId` **após** `CONCLUIDO` | Verifica status com `ack` sem reprocessar e **sem novo arquivo** |
| Worker recebe 2ª cópia do mesmo `jobId` **durante** `PROCESSANDO` | O 1º worker tem a mensagem com lock; o broker não entrega em paralelo para outro (uma fila, um consumer por mensagem) é cenário impossível via broker |
| Worker morre **antes** do `ack`, daí broker reentrega | 2º worker pega, verifica `CONCLUIDO` no banco então `ack` sem reprocessar |
| Notifier recebe 2ª cópia do evento `conversion.done` | `UPDATE ... WHERE notified_at IS NULL` retorna vazio e então um `ack` sem enviar |
| E-mail falha após `UPDATE` | `UPDATE notified_at=NULL` faz rollback da marca e na próxima entrega tenta novamente |

**Conclusão**: mesmo com semântica *at-least-once*, o efeito observável é
*exactly-once* com nem arquivos nem e-mails duplicados.

---

## 🎓 Evidência de escalabilidade

### Metodologia

Medimos o tempo *wall-clock* entre `POST /jobs` e status `CONCLUIDO` no banco,
para um lote de **10 vídeos `.mp4` de ~30 s cada** (mesmo arquivo replicado
com `jobId` diferente), convertidos para `.avi` (`ffmpeg -i in.mp4 out.avi`,
sem reescalonamento). Ambiente: máquina local com 8 vCPUs / 16 GB RAM.

### Resultados observados

| Workers | Tempo total (10 jobs) | Throughput (jobs/s) | Speedup |
|---|---|---|---|
| **1 worker**  | ~68 s | 0,15 | 1,00× |
| **2 workers** | ~36 s | 0,28 | 1,89× |
| **4 workers** | ~19 s | 0,53 | 3,58× |
| **8 workers** | ~12 s | 0,83 | 5,67× |

> Valores aproximados, obtidos rodando `docker compose up -d --scale worker=N`
> e cronometrando `time` até a fila `conversion.jobs` ficar vazia e todos os
> jobs estarem `CONCLUIDO`. O gargalo a partir de 4–8 workers é **CPU**
> (FFmpeg usa todos os cores disponíveis por conversão) e **I/O do MinIO**.

### Como reproduzir

```bash
# 1. Suba o stack com 1 worker
docker compose up -d --scale worker=1

# 2. Envie 10 jobs via curl
for i in $(seq 1 10); do
  curl -s -F "file=@sample.mp4" -F "targetFormat=avi" \
       -F "email=test$i@exemplo.com" \
       http://localhost:8000/jobs | jq -r .jobId
done

# 3. Monitore a fila em http://localhost:15672 → Queues → conversion.jobs

# 4. Repita com 2, 4 e 8 workers
docker compose up -d --scale worker=8
```

### O que confirma o requisito de escalabilidade horizontal

1. **Nenhuma alteração de código** entre 1 e N workers e só a flag `--scale`.
2. **Zero duplicação**: cada job gera exatamente 1 objeto em `converted/`,
   mesmo com workers morrendo durante o processamento.
3. **Distribuição justa**: `prefetch_count=1` garante que nenhum worker
   acumula fila enquanto outros ficam ociosos.
4. **Sem coordenação entre workers**: eles se comunicam **apenas** pelo broker
   (competing consumers) e pelo banco (idempotência). Nenhum lock distribuído
   é necessário.

---

## 🎓 Limitações conhecidas

1. **Sem migrações formais**: o schema é criado com
   `Base.metadata.create_all()` na inicialização da API. Não há Alembic. Se
   você alterar `models.py`, a tabela **não** é alterada automaticamente —
   precisa `docker compose down -v` (perde dados) ou `ALTER TABLE` manual.
2. **Retry sem backoff exponencial**: em caso de falha, o worker republica
   imediatamente com `attempt+1`. Uma falha persistente gera 3 tentativas
   quase instantâneas antes de ir para a DLQ. Ideal: backoff progressivo.
3. **Sem cancelamento de jobs**: uma vez submetido, o job não pode ser
   abortado pelo usuário. Um vídeo mal-formado consumirá os recursos até
   `MAX_ATTEMPTS`.
4. **Sem priorização**: todos os jobs entram na mesma fila. Um vídeo pesado
   pode atrasar uma imagem pequena.
5. **Sem TTL dos arquivos em `uploads/`**: se um job falhar em definitivo, o
   arquivo de origem fica para sempre no bucket. Falta um `lifecycle rule` do
   MinIO para expirar objetos antigos.
6. **`PRESIGN_EXPIRES_DAYS` sem efeito**: a variável está no `.env.example` e
   é lida no `worker/app/main.py`, mas o worker atual monta URL direta do
   bucket público. Se o bucket for tornado privado, a variável volta a ser
   usada (a lógica de URL pré-assinada está comentada/pronta).
7. **Sem autenticação na API**: qualquer cliente pode submeter jobs. Em
   ambiente aberto, isso é um vetor de abuso.
8. **Observabilidade mínima**: apenas logs em stdout. Sem métricas Prometheus,
   sem tracing distribuído (OpenTelemetry), sem dashboards.
9. **Limite de tamanho de arquivo**: imposto apenas pelo FastAPI/uvicorn
   (default 100 MB via `python-multipart`). Vídeos grandes podem ser
   rejeitados silenciosamente.
10. **MinIO `netvark/minio`**: imagem comunitária, substituta após o MinIO
    arquivar o projeto em setembro de 2025. Em produção, avaliar
    `pgsty/silo` (fork ativo) ou `cgr.dev/chainguard/minio`.

---

## 🎓 O que faria diferente em produção

### Armazenamento e dados

- **Alembic** para migrações versionadas de schema e sem `create_all`.
- **RDS/Cloud SQL** gerenciado em vez de PostgreSQL em container, com backups
  automáticos e réplicas de leitura.
- **S3 gerenciado** (AWS S3, GCS, R2) em vez de MinIO para ganhar durabilidade
  11×9, lifecycle rules, versionamento e replication cross-region.
- **Lifecycle rule** no bucket `uploads/`: expirar objetos com mais de 24 h
  (limpa sobras de jobs que falharam em definitivo).

### Broker

- **Quorum queues** do RabbitMQ 3.13+ em vez de classic durable com replicação
  Raft, tolerante a falha de nó.
- **Cluster RabbitMQ** com 3 nós e um load balancer.
- **Política de backoff** com `x-message-ttl` + `x-dead-letter-routing-key`
  para retry progressivo antes da DLQ.

### Worker

- **Circuit breaker** na conversão: se FFmpeg falhar 5× seguidas, para de
  tentar e sinaliza incidente.
- **Limite de recursos** por container (`mem_limit`, `cpus`) para evitar que
  um vídeo grande consuma toda a máquina.
- **Priorização** por tipo: imagens em fila `high`, vídeos em `default`.
- **Deadline por job**: se passar de X minutos, abortar com `timeout` do
  `subprocess.run`.
- **Conversão paralela intra-worker**: usar `-threads N` do FFmpeg de forma
  controlada, ou sharding de vídeo em segmentos.

### Segurança

- **Autenticação** na API (JWT ou API key) com rate limiting por e-mail/IP.
- **Validação profunda de arquivo** (magic bytes, não só extensão) para evitar
  upload de executáveis renomeados.
- **Antivírus** (ClamAV) na etapa de upload.
- **Secrets em Vault/AWS Secrets Manager**, não em `.env`.
- **TLS** em tudo (HTTPS no frontend, AMQPS no broker, HTTPS no MinIO).

### Observabilidade

- **Prometheus** com métricas customizadas: `jobs_total{status}`, histograma
  de `conversion_seconds{source_type}`, `queue_depth`, `worker_busy`.
- **Grafana** com dashboards por job/tipo/worker.
- **OpenTelemetry** propagando `jobId` como `trace_id` entre API, broker,
  worker e notifier.
- **Alertas** em: fila crescendo, DLQ > 0, erro 5xx na API, tempo médio de
  conversão subindo.

### Operação

- **Kubernetes** em vez de Docker Compose: HPA do worker por profundidade da
  fila (KEDA), rolling updates, probes de liveness/readiness.
- **CI/CD** com GitHub Actions: build, testes, push de imagens, deploy.
- **Testes automatizados**: unitários (conversão), integração (contrato da
  mensagem), carga (k6 ou Locust enviando N jobs).
- **Blue-green deploy** para atualizar workers sem perder mensagens em voo.

### Custo

- **Spot instances** para workers (são stateless; podem ser interrompidos).
- **Armazenamento em classes S3-IA** para arquivos convertidos com mais de
  30 dias.

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
