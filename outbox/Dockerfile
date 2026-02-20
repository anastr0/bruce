# Build from repo root: docker build -f outbox/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY outbox/ ./outbox/

ENV PYTHONUNBUFFERED=1

CMD ["python", "outbox/publisher.py"]

