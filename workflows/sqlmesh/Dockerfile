FROM debian:trixie AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV UV_PYTHON_INSTALL_DIR=/opt/python

WORKDIR /app

COPY .python-version pyproject.toml ./

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && uv python install "$(cat .python-version)" \
    && uv venv --python "$(cat .python-version)" --managed-python --relocatable /app/.venv \
    && UV_LINK_MODE=copy uv sync --no-dev --no-install-project

FROM debian:trixie

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=/app/.venv/bin:$PATH
ENV PYSPARK_PYTHON=/app/.venv/bin/python

WORKDIR /app/pipelines

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /opt/python /opt/python
COPY pipelines/ /app/pipelines/

ENTRYPOINT ["sqlmesh"]
