# Conversor Escalável de Mídia (Vídeo / Áudio / Imagem)

Sistema distribuído de conversão assíncrona de mídia com broker de mensagens,
object storage compatível com S3, banco relacional para estado do job e
notificação por e-mail.

## Arquitetura

```
Frontend ──HTTP──► API (Producer) ──► MinIO (uploads)
                        │
                        └──► RabbitMQ (conversion.jobs) ──► Worker 1..N
                                                              │
                                                              ├──► MinIO (converted)
                                                              └──► RabbitMQ
                                                                   (conversion.done)
                                                                       │
                                                                       ▼
                                                                  Notifier
                                                                  (SMTP / MailHog)
```

## Como subir

```bash
docker compose up --build -d
```

Endpoints úteis:

| Serviço         | URL                                            |
|-----------------|------------------------------------------------|
| Frontend        | http://localhost:8000                          |
| API (OpenAPI)   | http://localhost:8000/docs                     |
| RabbitMQ (mgmt) | http://localhost:15672  (guest/guest)          |
| MinIO Console   | http://localhost:9001   (minioadmin/minioadmin)|
| MailHog UI      | http://localhost:8025                          |

## Escalando workers horizontalmente

```bash
docker compose up -d --scale worker=5
```

O RabbitMQ distribui as mensagens por *competing consumers* e `prefetch_count=1`
garante que cada worker recebe no máximo 1 job por vez. Nenhuma alteração de
código é necessária.

## Justificativa — RabbitMQ em vez de Kafka

Escolhemos **RabbitMQ** porque:

1. **Carga por mensagem é heterogênea e relativamente pequena em volume**:
   cada mensagem é um job. RabbitMQ tem overhead menor para esse padrão.
2. **Dead Letter Queue nativa**: a `x-dead-letter-exchange` permite que
   mensagens que excederam N tentativas vão automaticamente para a DLQ sem
   código de roteamento manual.
3. **Ack manual por mensagem**: ideal para garantir "processou → ack" com
   tolerância a falhas (se o worker morre, o broker reentrega).
4. **Simplicidade operacional** para a escala do problema. Kafka seria
   preferível se precisássemos de replay, streaming ou retenção longa.

## Garantias de entrega adotadas

- **Producer → broker**: `delivery_mode=2` (persistente) + filas `durable`.
- **Broker → consumer**: `basic_ack` **somente após** o resultado estar no MinIO
  e o status atualizado em banco. `basic_nack(requeue=False)` envia para a DLQ
  quando estoura `MAX_ATTEMPTS`.
- **Idempotência**:
  - Worker: ignora mensagens cujo job já está `CONCLUIDO`.
  - Notifier: usa `UPDATE ... WHERE notified_at IS NULL RETURNING id` como
    *compare-and-set* atômico, garantindo **no máximo um e-mail por job** mesmo
    com N instâncias do notifier rodando em paralelo.
- **At-least-once**: entrega é ao menos uma vez; a idempotência acima torna o
  efeito observável **exactly-once** para o usuário.

## Ciclo de vida dos arquivos no MinIO

1. API grava origem em `uploads/<jobId>/source.<ext>`.
2. Mensagem na fila carrega apenas `s3://uploads/<jobId>/source.<ext>` — nunca bytes.
3. Worker baixa, converte, sobe para `converted/<jobId>/converted.<ext>`.
4. Gera URL pré-assinada (7 dias) apontando para o endpoint público do MinIO.
5. **Só após sucesso** remove o objeto de origem.
6. Em falha definitiva, a origem é preservada para diagnóstico e o job vira `ERRO`.

## Contrato da mensagem

`conversion.jobs` (routing key `job`):

```json
{
  "jobId": "9f2c1a84-3b77-4e5a-9d21-6c0a8f4b1e30",
  "sourceUrl": "s3://uploads/9f2c1a84.../source.mov",
  "sourceType": "video",
  "sourceFormat": "mov",
  "targetFormat": "mp4",
  "notifyEmail": "aluno@exemplo.com",
  "requestedAt": "2026-03-14T18:22:05Z",
  "attempt": 1
}
```

`conversion.done` (fanout):

```json
{
  "jobId": "...",
  "status": "CONCLUIDO",
  "convertedUrl": "http://localhost:9000/converted/.../converted.mp4?...",
  "notifyEmail": "aluno@exemplo.com"
}
```

## Testando manualmente

1. Abra http://localhost:8000.
2. Selecione um arquivo `.mp4`, escolha `AVI` como destino e informe um e-mail.
3. O sistema responde imediatamente com `jobId` e a tela faz polling em `/jobs/{id}`.
4. Veja o e-mail no MailHog: http://localhost:8025.
5. Veja o link público e a conversão via MinIO Console (bucket `converted`).
6. Veja a fila de mensagens em http://localhost:15672.
7. Simule falha: derrube um worker no meio do processamento
   (`docker compose kill worker`) e observe a reentrega via outro worker.

## Estrutura de componentes

| Componente     | Papel                                                              |
|----------------|--------------------------------------------------------------------|
| `api`          | Producer: upload, persistência do job, publicação da mensagem      |
| `worker`       | Consumer: baixa, converte (FFmpeg/Pillow), sobe, limpa origem      |
| `notifier`     | Segundo consumer: reage ao evento de conclusão, envia e-mail       |
| `rabbitmq`     | Broker — filas `conversion.jobs` + `conversion.done` + DLQ         |
| `minio`        | Object storage — buckets `uploads` e `converted`                   |
| `postgres`     | Estado do job (`PENDENTE/PROCESSANDO/CONCLUIDO/ERRO`)              |
| `mailhog`      | Simulador SMTP para desenvolvimento                                |
