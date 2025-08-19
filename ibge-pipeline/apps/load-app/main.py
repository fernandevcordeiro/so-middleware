#!/usr/bin/env python3
"""
Load App - Carrega dados CSV do IBGE e envia para Kafka (multi-arquivos)
Adaptado para usar OpenTelemetry OTLP (Tempo) configurável via env var.
"""
import json, os, time, logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource


logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
logger = logging.getLogger("load-app")

# --- OpenTelemetry ---
otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo:4317")
service_name = os.getenv("OTEL_SERVICE_NAME", "load-app")
resource_attrs = {"service.name": service_name}
resource = Resource.create(resource_attrs)
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
otlp_exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","ibge-enderecos")
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY","/data")

def connect_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=5,
            retry_backoff_ms=1000
        )
        logger.info(f"Conectado ao Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.exception(f"Erro ao conectar ao Kafka: {e}")
        return None

def load_csv_file(file_path):
    with tracer.start_as_current_span("load_csv_file") as span:
        span.set_attribute("file.path", file_path)
        df = pd.read_csv(file_path)
        span.set_attribute("records.count", len(df))
        return df.to_dict("records")

def send_to_kafka(producer, data, source_file):
    with tracer.start_as_current_span("send_to_kafka") as span:
        span.set_attribute("source.file", source_file)
        sent = 0
        for record in data:
            enriched = {"source_file": source_file, "timestamp": time.time(), "data": record}
            key = str(record.get("MUNICIPIO","unknown"))
            try:
                fut = producer.send(KAFKA_TOPIC, key=key, value=enriched)
                fut.get(timeout=10)
                sent += 1
            except KafkaError as e:
                logger.error(f"Falha ao enviar msg: {e}")
                span.record_exception(e)
        span.set_attribute("records.sent", sent)
        logger.info(f"Arquivo {source_file}: {sent} registros enviados")
        return sent

def main():
    logger.info("Iniciando load-app")
    producer = connect_kafka()
    if not producer:
        logger.error("Kafka indisponível. Encerrando.")
        return

    if not os.path.exists(DATA_DIRECTORY):
        logger.error(f"Diretório de dados não encontrado: {DATA_DIRECTORY}")
        return

    files = [f for f in os.listdir(DATA_DIRECTORY) if f.endswith(".csv")]
    if not files:
        logger.warning(f"Nenhum CSV em {DATA_DIRECTORY}")
        return

    total = 0
    for f in files:
        path = os.path.join(DATA_DIRECTORY, f)
        try:
            rows = load_csv_file(path)
            total += send_to_kafka(producer, rows, f)
            time.sleep(0.5)
        except Exception as e:
            logger.exception(f"Erro processando {f}: {e}")
    producer.flush()
    producer.close()
    logger.info(f"Concluído. Total de mensagens enviadas: {total}")

if __name__ == "__main__":
    main()
