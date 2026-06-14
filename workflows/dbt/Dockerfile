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
    && rm -rf /var/lib/apt/lists/*

ENV PATH=/app/.venv/bin:$PATH

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /opt/python /opt/python
COPY dbt_runner/ /app/dbt_runner/
COPY example_db/ /app/example_db/
COPY pi/ /app/pi/

# python -m runner runs `dbt build` against Trino (homelab) or DuckDB (local).
ENTRYPOINT ["python", "-m", "dbt_runner"]
