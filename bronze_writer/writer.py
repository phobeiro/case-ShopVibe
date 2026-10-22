"""
ShopVibe - Writer da camada bronze (raw).

Lê os eventos de pedidos do Kafka (publicados pelo Debezium) e grava, SEM transformar,
arquivos JSON Lines no bucket S3 (SeaweedFS), particionados por tabela e data:

    s3://bronze/<tabela>/dt=AAAA-MM-DD/<tabela>-p<partição>-<offset_inicial>-<offset_final>.jsonl

Garantias:
  - At-least-once: o offset só é confirmado (commit) DEPOIS que o arquivo foi gravado no S3.
  - Reprocessamento seguro: o nome do arquivo é determinístico (partição + faixa de offsets);
    se o writer cair entre gravar e confirmar, o mesmo lote é regravado no mesmo caminho.
"""
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException, TopicPartition

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", r"^shopvibe\.shopvibe\.(orders|order_items)$")
GROUP_ID = os.getenv("GROUP_ID", "bronze-writer")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_BUCKET = os.getenv("S3_BUCKET", "bronze")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "lab")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "lab")

FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "2"))      # latência máxima até a bronze
FLUSH_MAX_RECORDS = int(os.getenv("FLUSH_MAX_RECORDS", "500"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bronze-writer")

running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 5, "mode": "standard"}),
    )


def ensure_bucket(s3):
    """Espera o S3 responder e garante que o bucket exista."""
    while running:
        try:
            try:
                s3.head_bucket(Bucket=S3_BUCKET)
            except ClientError as e:
                if e.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
                    s3.create_bucket(Bucket=S3_BUCKET)
                    log.info("bucket '%s' criado", S3_BUCKET)
                else:
                    raise
            log.info("bucket '%s' pronto em %s", S3_BUCKET, S3_ENDPOINT)
            return
        except Exception as e:  # noqa: BLE001
            log.warning("aguardando o S3 (%s): %s", S3_ENDPOINT, e)
            time.sleep(3)


def table_of(topic: str) -> str:
    """'shopvibe.shopvibe.orders' -> 'orders'"""
    return topic.rsplit(".", 1)[-1]


def to_line(msg) -> str:
    """Uma linha JSON por evento: metadados do Kafka + chave e valor originais."""
    ts_type, ts_ms = msg.timestamp()
    raw_value = msg.value()
    raw_key = msg.key()
    try:
        value = json.loads(raw_value) if raw_value is not None else None
    except json.JSONDecodeError:
        value = {"_unparsed": raw_value.decode("utf-8", "replace")}
    try:
        key = json.loads(raw_key) if raw_key is not None else None
    except json.JSONDecodeError:
        key = raw_key.decode("utf-8", "replace")
    record = {
        "_kafka_topic": msg.topic(),
        "_kafka_partition": msg.partition(),
        "_kafka_offset": msg.offset(),
        "_kafka_timestamp_ms": ts_ms,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "key": key,
        "value": value,
    }
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def date_of(msgs) -> str:
    """Partição dt= pela data (UTC) do primeiro evento do lote."""
    ts_ms = msgs[0].timestamp()[1]
    if ts_ms and ts_ms > 0:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def flush(s3, consumer, buffer):
    """Grava cada (tópico, partição) em um arquivo e, só depois, confirma os offsets."""
    if not buffer:
        return
    offsets_to_commit = []
    total = 0
    for (topic, partition), msgs in buffer.items():
        first, last = msgs[0].offset(), msgs[-1].offset()
        table = table_of(topic)
        key = f"{table}/dt={date_of(msgs)}/{table}-p{partition}-{first:012d}-{last:012d}.jsonl"
        body = ("\n".join(to_line(m) for m in msgs) + "\n").encode("utf-8")
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body,
                      ContentType="application/x-ndjson")
        log.info("gravado s3://%s/%s (%d eventos)", S3_BUCKET, key, len(msgs))
        offsets_to_commit.append(msgs[-1])
        total += len(msgs)
    # commit só após TODOS os PUTs terem dado certo (at-least-once)
    consumer.commit(offsets=[
        TopicPartition(m.topic(), m.partition(), m.offset() + 1)
        for m in offsets_to_commit
    ], asynchronous=False)
    buffer.clear()
    log.info("offsets confirmados (%d eventos no lote)", total)


# ---------------------------------------------------------------------- main
def main():
    s3 = make_s3()
    ensure_bucket(s3)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,                  # commit manual, depois do S3
        "auto.offset.reset": "earliest",              # lê todo o histórico na 1ª vez
        "topic.metadata.refresh.interval.ms": 5000,   # descobre tópicos criados depois
        "session.timeout.ms": 15000,
    })
    consumer.subscribe([TOPIC_PATTERN])
    log.info("consumindo %s em %s (grupo '%s')", TOPIC_PATTERN, KAFKA_BOOTSTRAP, GROUP_ID)

    buffer = defaultdict(list)
    count = 0
    last_flush = time.monotonic()

    try:
        while running:
            msg = consumer.poll(0.5)
            if msg is not None:
                if msg.error():
                    # tópico ainda inexistente (conector não registrado) não é erro fatal
                    log.warning("kafka: %s", msg.error())
                else:
                    buffer[(msg.topic(), msg.partition())].append(msg)
                    count += 1

            due = (time.monotonic() - last_flush) >= FLUSH_SECONDS
            if buffer and (due or count >= FLUSH_MAX_RECORDS):
                flush(s3, consumer, buffer)
                count = 0
                last_flush = time.monotonic()
            elif not buffer and due:
                last_flush = time.monotonic()
    except KafkaException as e:
        log.error("erro do Kafka: %s", e)
        sys.exit(1)
    finally:
        try:
            flush(s3, consumer, buffer)   # grava o que sobrou ao encerrar
        finally:
            consumer.close()
            log.info("encerrado")


if __name__ == "__main__":
    main()