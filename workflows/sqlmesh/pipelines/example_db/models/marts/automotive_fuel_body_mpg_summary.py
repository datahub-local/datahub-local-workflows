import typing as t

from sqlmesh import ExecutionContext, model

from models.marts.jdbc import write_jdbc


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
    write_jdbc(df, table="automotive_fuel_body_mpg_summary")
    return df
