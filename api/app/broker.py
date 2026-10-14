import json
import pika
from .config import settings

EXCHANGE = "conversion"
QUEUE = "conversion.jobs"
DLX = "conversion.dlx"
DLQ = "conversion.jobs.dlq"
DONE_EXCHANGE = "conversion.done"
DONE_QUEUE = "conversion.done.notify"


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


def publish_job(message: dict) -> None:
    conn = pika.BlockingConnection(pika.URLParameters(settings.RABBITMQ_URL))
    try:
        ch = conn.channel()
        _declare_topology(ch)
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key="job",
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json"
            ),
        )
    finally:
        conn.close()
