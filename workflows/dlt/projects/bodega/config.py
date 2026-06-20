"""Pipeline-specific configuration for the bodega dlt pipelines.

Shared infrastructure (Trino, Polaris, DuckDB paths, S3, target validation) lives in
``dlt_runner.config`` and is re-exported here so callers use a single import.
"""

from __future__ import annotations

import os

from dlt_runner.config import VALID_TARGETS as VALID_TARGETS
from dlt_runner.config import configure_iceberg_env as configure_iceberg_env
from dlt_runner.config import duckdb_path as duckdb_path
from dlt_runner.config import env as env
from dlt_runner.config import polaris_uri as polaris_uri
from dlt_runner.config import s3_credentials as s3_credentials
from dlt_runner.config import s3_endpoint as s3_endpoint
from dlt_runner.config import bronze_bucket as bronze_bucket
from dlt_runner.config import temp_bucket as temp_bucket
from dlt_runner.config import trino_url as trino_url
from dlt_runner.config import validate_target as validate_target

KAFKA_BOOTSTRAP_SERVERS_DEFAULT = (
    "datahub-local-core-data-redpanda-0.datahub-local-core-data-redpanda.data.svc.cluster.local:9093,"
    "datahub-local-core-data-redpanda-1.datahub-local-core-data-redpanda.data.svc.cluster.local:9093,"
    "datahub-local-core-data-redpanda-2.datahub-local-core-data-redpanda.data.svc.cluster.local:9093"
)


def kafka_bootstrap_servers() -> str:
    return os.environ["KAFKA_BOOTSTRAP_SERVERS"]


def kafka_topic() -> str:
    return env("KAFKA_TOPIC_BODEGA", "bodega_invoices")


def llm_provider() -> str:
    """LLM provider to use for enrichment. Values: 'openrouter' or 'ollama' (default)."""
    return env("LLM_PROVIDER", "ollama")


def openrouter_api_key() -> str:
    return os.environ["OPENROUTER_API_KEY"]


def openrouter_model() -> str:
    return env("OPENROUTER_MODEL", "deepseek/deepseek-v4-flash")


def ollama_base_url() -> str:
    return env("OLLAMA_BASE_URL", "http://datahub-local-core-data-ollama:11434/v1")


def ollama_model() -> str:
    return env("OLLAMA_MODEL", "lfm2.5-thinking:1.2b")


def llm_settings() -> tuple[str, str, str]:
    """Return (base_url, api_key, model_id) for the configured LLM provider."""
    provider = llm_provider()
    if provider == "ollama":
        return ollama_base_url(), "", ollama_model()
    return "https://openrouter.ai/api/v1", openrouter_api_key(), openrouter_model()
