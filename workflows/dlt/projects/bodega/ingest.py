"""Ingest step: drain the bodega_invoices Kafka topic into the bronze medallion layer.

dlt streams parsed invoice JSON from Kafka into ``bronze.bodega.raw_invoices``, which the
dbt bodega project then reads as a source. Raw JSON arrays (items, taxes) are kept as
strings in Bronze; explosion into rows happens in Silver.

- ``local``   → DuckDB file (the same ``bronze.duckdb`` the dbt local target reads).
- ``homelab`` → Iceberg table via Apache Polaris REST + S3, staged in the temp bucket.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import dlt
from confluent_kafka import Consumer, KafkaError

from . import config

PIPELINE_NAME = "bodega_ingest"
DATASET_NAME = "bodega"
TABLE_NAME = "raw_invoices"


@dlt.resource(
    name=TABLE_NAME,
    write_disposition="merge",
    primary_key=["invoice_number"],
    columns={"invoice_date": {"data_type": "text"}},
)
def raw_invoices(bootstrap_servers: str, topic: str, group_id: str = "bodega-dlt-bronze"):
    """Micro-batch Kafka consumer: drain until 3 consecutive empty polls, then return."""
    kafka_config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
    }
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    ingested_at = datetime.now(timezone.utc).isoformat()
    idle_rounds, max_idle, poll_timeout = 0, 3, 10.0
    try:
        while idle_rounds < max_idle:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                idle_rounds += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle_rounds = max_idle
                else:
                    raise RuntimeError(f"Kafka error: {msg.error()}")
                continue
            idle_rounds = 0
            data = json.loads(msg.value().decode("utf-8"))
            yield {
                "invoice_number":     data["invoice"]["number"],
                "invoice_date":       data["invoice"]["date"],
                "operator_id":        data["invoice"]["operator"],
                "store_name":         data["store"]["name"],
                "store_vat_id":       data["store"]["vat_id"],
                "store_address":      data["store"]["address"],
                "store_phone":        data["store"]["phone"],
                "total_amount":       float(data["totals"]["amount"]),
                "payment_method":     data["totals"]["payment_method"],
                "card_type":          data["totals"]["card_type"],
                "card_number_masked": data["totals"]["card_number"],
                "items_json":         json.dumps(data["items"]),
                "taxes_json":         json.dumps(data["taxes"]),
                "supermarket":        "MERCADONA",
                "_kafka_offset":      msg.offset(),
                "_ingested_at":       ingested_at,
            }
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


def run(target: str):
    config.validate_target(target)

    resource = raw_invoices(
        bootstrap_servers=config.kafka_bootstrap_servers(),
        topic=config.kafka_topic(),
    )

    if target == "local":
        destination = dlt.destinations.duckdb(config.duckdb_path("bronze"))
    else:
        config.configure_iceberg_env("bronze")
        destination = dlt.destinations.filesystem(
            bucket_url=f"s3://{config.bronze_bucket()}/{DATASET_NAME}",
            credentials=config.s3_credentials(),
        )
        resource.apply_hints(table_format="iceberg")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=destination,
        dataset_name=DATASET_NAME,
    )
    return pipeline.run(resource)
