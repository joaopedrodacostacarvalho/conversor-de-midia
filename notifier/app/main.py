import json
import os
import smtplib
import time
from email.mime.text import MIMEText

import pika
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
SMTP_HOST = os.getenv("SMTP_HOST", "mailhog")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
SMTP_FROM = os.getenv("SMTP_FROM", "noreply@converter.local")

DONE_EXCHANGE = "conversion.done"
DONE_QUEUE = "conversion.done.notify"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def send_email(to: str, subject: str, html: str) -> None:
    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as s:
        s.send_message(msg)


def render_email(event: dict) -> tuple[str, str]:
    job_id = event["jobId"]
    if event["status"] == "CONCLUIDO":
        subject = f"[Conversor] Conversão concluída — {job_id[:8]}"
        html = f"""
            <h2>Conversão concluída ✅</h2>
            <p>Job: <code>{job_id}</code></p>
            <p>Seu arquivo está disponível em:</p>
            <p><a href="{event["convertedUrl"]}">{event["convertedUrl"]}</a></p>
        """
    else:
        subject = f"[Conversor] Falha na conversão — {job_id[:8]}"
        html = f"""
            <h2>Falha na conversão ❌</h2>
            <p>Job: <code>{job_id}</code></p>
            <p>Erro: {event.get("errorMessage", "desconhecido")}</p>
        """
    return subject, html


def process_message(ch, method, _properties, body):
    try:
        event = json.loads(body)
        job_id = event["jobId"]
        email = event.get("notifyEmail")
        if not email:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Reivindicação idempotente — só envia se ainda não foi notificado
        with engine.begin() as conn:
            row = conn.execute(
                text("""
                    UPDATE jobs SET notified_at=NOW()
                    WHERE id=:id AND notified_at IS NULL
                    RETURNING id
                """),
                {"id": job_id},
            ).fetchone()
        if not row:
            print(f"[notifier] job={
                  job_id} já notificado — ignorando", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        subject, html = render_email(event)
        try:
            send_email(email, subject, html)
        except Exception:
            # Rollback da marca para permitir nova tentativa
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE jobs SET notified_at=NULL WHERE id=:id"),
                    {"id": job_id},
                )
            raise

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(
            f"[notifier] e-mail enviado para {email} (job={job_id})", flush=True)

    except Exception as e:
        print(f"[notifier] erro: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    for _ in range(60):
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1 FROM jobs LIMIT 1"))
            break
        except Exception:
            time.sleep(1)

    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 60
    while True:
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.exchange_declare(
                DONE_EXCHANGE, exchange_type="fanout", durable=True)
            ch.queue_declare(DONE_QUEUE, durable=True)
            ch.queue_bind(DONE_QUEUE, DONE_EXCHANGE)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=DONE_QUEUE,
                             on_message_callback=process_message)
            print("[notifier] aguardando eventos de conclusão...", flush=True)
            ch.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(5)


if __name__ == "__main__":
    main()
