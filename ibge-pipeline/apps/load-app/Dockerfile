FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV OTEL_SERVICE_NAME=load-app \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=ibge-enderecos \
    DATA_DIRECTORY=/data
CMD ["python", "main.py"]
