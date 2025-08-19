FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV OTEL_SERVICE_NAME=store-app \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=ibge-enderecos \
    KAFKA_GROUP_ID=store-app-group \
    MINIO_ENDPOINT=minio.storage.svc.cluster.local:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin \
    MINIO_BUCKET=ibge-data \
    BATCH_SIZE=200 \
    BATCH_TIMEOUT=30
CMD ["python", "main.py"]
