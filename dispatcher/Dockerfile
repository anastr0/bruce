# Build from repo root: docker build -f dispatcher/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY dispatcher/ ./dispatcher/

ENV PYTHONUNBUFFERED=1

CMD ["python", "dispatcher/worker.py"]
