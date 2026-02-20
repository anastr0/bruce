FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy application code
COPY ingestion-api/ ./ingestion-api/
COPY .env.example .env

# Expose port
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=ingestion-api/main.py
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "ingestion-api/main.py"]
