FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app

COPY pyproject.toml .
RUN uv pip install --no-cache -e .

COPY example_db/ /app/example_db/
COPY pi/ /app/pi/

ENTRYPOINT ["uv", "run", "sqlmesh"]
