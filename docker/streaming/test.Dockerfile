FROM python:3.9-slim

WORKDIR /app

COPY test_kafka_connection.py /app/

CMD ["python", "test_kafka_connection.py"] 