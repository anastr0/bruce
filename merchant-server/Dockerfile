# Build from repo root: docker build -f merchant-server/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY merchant-server/ ./merchant-server/

ENV PYTHONUNBUFFERED=1
ENV PORT=5001

EXPOSE 5001

CMD ["python", "merchant-server/main.py"]
