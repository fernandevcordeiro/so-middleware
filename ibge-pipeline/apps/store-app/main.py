#!/usr/bin/env python3
"""
Store App - Consome dados do Kafka e grava Parquet no MinIO S3 (com batching).
Adaptado para usar OpenTelemetry OTLP (Tempo) configurável via env var.
"""
import os, io, json, time, logging
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource


logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
logger = logging.getLogger("store-app")

# --- OpenTelemetry ---
otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo:4317")
service_name = os.getenv("OTEL_SERVICE_NAME", "store-app")
resource_attrs = {"service.name": service_name}
resource = Resource.create(resource_attrs)
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
otlp_exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","ibge-enderecos")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID","store-app-group")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT","minio.storage.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET","ibge-data")

BATCH_SIZE = int(os.getenv("BATCH_SIZE","200"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT","30"))

def connect_kafka():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=BATCH_TIMEOUT*1000
        )
        logger.info(f"Conectado ao Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        return consumer
    except Exception as e:
        logger.exception(f"Erro Kafka: {e}")
        return None

def connect_minio():
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logger.info(f"Bucket criado: {MINIO_BUCKET}")
        logger.info(f"Conectado ao MinIO: {MINIO_ENDPOINT}")
        return client
    except Exception as e:
        logger.exception(f"Erro MinIO: {e}")
        return None

def save_to_parquet(minio_client, records, batch_id):
    with tracer.start_as_current_span("save_to_parquet") as span:
        span.set_attribute("batch.id", batch_id)
        span.set_attribute("records.count", len(records))
        data_list = []
        for rec in records:
            data = rec.get("data", {})
            data["source_file"] = rec.get("source_file", "unknown")
            data["ingestion_timestamp"] = rec.get("timestamp", time.time())
            data_list.append(data)
        if not data_list:
            return False
        df = pd.DataFrame(data_list)
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        object_name = f"enderecos/batch_{batch_id}_{ts}.parquet"
        minio_client.put_object(MINIO_BUCKET, object_name, buf, length=len(buf.getvalue()), content_type="application/octet-stream")
        logger.info(f"Salvo no MinIO: {object_name} ({len(records)} registros)")
        span.set_attribute("object.name", object_name)
        return True

def main():
    logger.info("Iniciando store-app")
    consumer = connect_kafka()
    if not consumer:
        logger.error("Kafka indisponível. Encerrando.")
        return
    minio_client = connect_minio()
    if not minio_client:
        logger.error("MinIO indisponível. Encerrando.")
        return

    batch, batch_id, last = [], 1, time.time()
    total = 0
    try:
        for msg in consumer:
            batch.append(msg.value)
            now = time.time()
            if len(batch) >= BATCH_SIZE or (now - last) >= BATCH_TIMEOUT:
                if save_to_parquet(minio_client, batch, batch_id):
                    total += len(batch)
                batch, batch_id, last = [], batch_id+1, now
    except KeyboardInterrupt:
        logger.info("Interrompido. Gravando batch final...")
    finally:
        if batch:
            save_to_parquet(minio_client, batch, batch_id)
            total += len(batch)
        consumer.close()
        logger.info(f"Concluído. Total processado: {total}")

if __name__ == "__main__":
    main()
