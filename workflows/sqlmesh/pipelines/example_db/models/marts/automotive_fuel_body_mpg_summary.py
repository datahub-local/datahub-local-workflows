import os
import typing as t

from sqlmesh import ExecutionContext, model


@model(
    "example_db.automotive_fuel_body_mpg_summary",
    kind="FULL",
    dialect="spark",
    description="MPG and price summary by fuel type and body style, written to Spark warehouse and external JDBC target.",
    depends_on=["example_db.automotive_snapshot"],
    columns={
        "fuel_type": "STRING",
        "body_style": "STRING",
        "vehicle_count": "BIGINT",
        "avg_city_mpg": "DOUBLE",
        "avg_highway_mpg": "DOUBLE",
        "avg_price": "DOUBLE",
        "updated_date": "TIMESTAMP",
        "created_date": "TIMESTAMP",
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> t.Any:
    snapshot = context.resolve_table("example_db.automotive_snapshot")
    sql = f"""
        SELECT
          fuel_type,
          body_style,
          COUNT(*)                       AS vehicle_count,
          ROUND(AVG(city_mpg), 2)        AS avg_city_mpg,
          ROUND(AVG(highway_mpg), 2)     AS avg_highway_mpg,
          ROUND(AVG(price), 2)           AS avg_price,
          MAX(updated_date)              AS updated_date
        FROM {snapshot}
        WHERE city_mpg IS NOT NULL
          AND highway_mpg IS NOT NULL
        GROUP BY fuel_type, body_style
        """

    from pyspark.sql import functions as F  # type: ignore[import]

    df = context.spark.sql(sql).withColumn("created_date", F.current_timestamp())
    _write_jdbc(df, table="automotive_fuel_body_mpg_summary")
    return df


def _write_jdbc(df: t.Any, table: str) -> None:
    db_url = os.environ["EXAMPLE_DB_URL"]
    schema = os.environ.get("EXAMPLE_DB_SCHEMA", "public")
    user = os.environ["EXAMPLE_DB_USER"]
    password = os.environ.get("EXAMPLE_DB_PASSWORD", "")
    driver = os.environ.get("EXAMPLE_DB_DRIVER", "org.h2.Driver")

    df.write.format("jdbc").options(
        url=db_url,
        dbtable=f"{schema}.{table}",
        user=user,
        password=password,
        driver=driver,
    ).mode("overwrite").save()
