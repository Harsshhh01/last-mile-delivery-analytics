# Runs the full pipeline anywhere Docker runs (recommended on Windows: no Java/Hadoop setup).
#   docker build -t delivery-pipeline .
#   docker run --rm -v "%cd%/output:/app/output" delivery-pipeline           (Windows cmd)
#   docker run --rm -v "$(pwd)/output:/app/output" delivery-pipeline         (macOS/Linux)
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
RUN pip install --no-cache-dir ".[dev]"
COPY tests ./tests
COPY jobs ./jobs
COPY infra ./infra

ENTRYPOINT ["python", "-m", "delivery_pipeline.run_pipeline", \
            "--lake", "output/lake", "--warehouse", "output/warehouse/delivery.duckdb", \
            "--reports", "output/reports"]
CMD ["--packages", "200000", "--fresh"]
