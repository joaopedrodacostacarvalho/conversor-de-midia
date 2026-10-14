import json
import os
import subprocess
import tempfile
import time

import pika
from minio import Minio
from PIL import Image
from sqlalchemy import create_engine, text

# ------------------------- config -------------------------
DATABASE_URL = os.environ["DATABASE_URL"]
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
UPLOADS_BUCKET = os.getenv("UPLOADS_BUCKET", "uploads")
CONVERTED_BUCKET = os.getenv("CONVERTED_BUCKET", "converted")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
PRESIGN_EXPIRES_DAYS = int(os.getenv("PRESIGN_EXPIRES_DAYS", "7"))

EXCHANGE = "conversion"
QUEUE = "conversion.jobs"
DLX = "conversion.dlx"
DLQ = "conversion.jobs.dlq"
DONE_EXCHANGE = "conversion.done"
DONE_QUEUE = "conversion.done.notify"

# ------------------------- clients -------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def _build_minio(endpoint: str) -> Minio:
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https://")
    return Minio(
        host, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=secure
    )


internal_minio = _build_minio(MINIO_ENDPOINT)


# ------------------------- helpers -------------------------
def wait_for_db(max_wait=60):
    for _ in range(max_wait):
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1 FROM jobs LIMIT 1"))
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Tabela 'jobs' não disponível")


def update_job(job_id: str, **fields):
    if not fields:
        return
    sets = ", ".join(f"{k}=:{k}" for k in fields)
    with engine.begin() as c:
        c.execute(
            text(f"UPDATE jobs SET {sets}, updated_at=NOW() WHERE id=:job_id"),
            {**fields, "job_id": job_id},
        )


def get_job_status(job_id: str):
    with engine.connect() as c:
        row = c.execute(
            text("SELECT status FROM jobs WHERE id=:id"), {"id": job_id}
        ).fetchone()
        return row[0] if row else None


def convert_media(src: str, dst: str, source_type: str, target_format: str) -> None:
    if source_type in ("video", "audio"):
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-i", src, dst],
            check=True,
            capture_output=True,
        )
    elif source_type == "image":
        img = Image.open(src)
        fmt = "JPEG" if target_format in (
            "jpg", "jpeg") else target_format.upper()
        if fmt == "JPEG" and img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        img.save(dst, format=fmt)
    else:
        raise ValueError(f"Tipo de mídia não suportado: {source_type}")


# ------------------------- broker -------------------------
def _declare_topology(ch):
    ch.exchange_declare(EXCHANGE, exchange_type="direct", durable=True)
    ch.exchange_declare(DLX, exchange_type="direct", durable=True)
    ch.queue_declare(
        QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": "dlq",
        },
    )
    ch.queue_bind(QUEUE, EXCHANGE, routing_key="job")
    ch.queue_declare(DLQ, durable=True)
    ch.queue_bind(DLQ, DLX, routing_key="dlq")
    ch.exchange_declare(DONE_EXCHANGE, exchange_type="fanout", durable=True)
    ch.queue_declare(DONE_QUEUE, durable=True)
    ch.queue_bind(DONE_QUEUE, DONE_EXCHANGE)


def _publish(exchange: str, routing_key: str, payload: dict):
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    try:
        ch = conn.channel()
        _declare_topology(ch)
        ch.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload).encode(),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json"
            ),
        )
    finally:
        conn.close()


def publish_retry(msg: dict):
    _publish(EXCHANGE, "job", msg)


def publish_done(event: dict):
    _publish(DONE_EXCHANGE, "", event)


# ------------------------- consumer -------------------------
def process_message(ch, method, _properties, body):
    try:
        msg = json.loads(body)
    except Exception:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    job_id = msg["jobId"]
    attempt = int(msg.get("attempt", 1))
    print(f"[worker] recebido job={job_id} attempt={attempt}", flush=True)

    try:
        # Idempotência: se já concluído, ignora
        status = get_job_status(job_id)
        if status == "CONCLUIDO":
            print(f"[worker] job={
                  job_id} já concluído — ignorando", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        update_job(job_id, status="PROCESSANDO", error_message=None)

        url = msg["sourceUrl"]
        if not url.startswith("s3://"):
            raise ValueError(f"sourceUrl inválido: {url}")
        bucket, key = url[5:].split("/", 1)

        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, f"source.{msg['sourceFormat']}")
            dst = os.path.join(tmp, f"converted.{msg['targetFormat']}")

            internal_minio.fget_object(bucket, key, src)
            convert_media(src, dst, msg["sourceType"], msg["targetFormat"])

            out_key = f"{job_id}/converted.{msg['targetFormat']}"
            internal_minio.fput_object(CONVERTED_BUCKET, out_key, dst)

            public_url = (
                f"{MINIO_PUBLIC_ENDPOINT.rstrip(
                    '/')}/{CONVERTED_BUCKET}/{out_key}"
            )

            update_job(job_id, status="CONCLUIDO", converted_url=public_url)

            # Limpeza da origem só após sucesso comprovado
            try:
                internal_minio.remove_object(bucket, key)
            except Exception as e:
                print(f"[worker] aviso: falha ao remover origem: {
                      e}", flush=True)

            publish_done(
                {
                    "jobId": job_id,
                    "status": "CONCLUIDO",
                    "convertedUrl": public_url,
                    "notifyEmail": msg["notifyEmail"],
                }
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[worker] job={job_id} concluído", flush=True)

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        print(f"[worker] erro job={job_id}: {err}", flush=True)

        if attempt < MAX_ATTEMPTS:
            msg["attempt"] = attempt + 1
            publish_retry(msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            update_job(job_id, status="ERRO", error_message=err[:1000])
            publish_done(
                {
                    "jobId": job_id,
                    "status": "ERRO",
                    "convertedUrl": None,
                    "errorMessage": err,
                    "notifyEmail": msg.get("notifyEmail"),
                }
            )
            # Vai para a DLQ — não fica re-tentando indefinidamente
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    wait_for_db()
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 60

    while True:
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            _declare_topology(ch)
            # 1 job por vez por worker → distribuição justa
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE, on_message_callback=process_message)
            print("[worker] aguardando mensagens...", flush=True)
            ch.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"[worker] conexão AMQP perdida ({e}). Reconectando em 5s", flush=True
            )
            time.sleep(5)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
