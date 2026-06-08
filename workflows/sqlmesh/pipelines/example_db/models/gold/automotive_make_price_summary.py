import typing as t

from sqlmesh import ExecutionContext, model

from models.jdbc import write_jdbc


@model(
    "gold.example_db.automotive_make_price_summary",
    kind="FULL",
    dialect="spark",
    description="Price and horsepower summary per make, written to Spark warehouse and external JDBC target.",
    depends_on=["example_db.automotive_snapshot"],
    columns={
        "make": "STRING",
        "vehicle_count": "BIGINT",
        "avg_price": "DOUBLE",
        "max_price": "DOUBLE",
        "avg_horsepower": "DOUBLE",
        "updated_date": "TIMESTAMP",
        "created_date": "TIMESTAMP",
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> t.Any:
    snapshot = context.resolve_table("example_db.automotive_snapshot")
    sql = f"""
        SELECT
          make,
          COUNT(*)                       AS vehicle_count,
          ROUND(AVG(price), 2)           AS avg_price,
          ROUND(MAX(price), 2)           AS max_price,
          ROUND(AVG(horsepower), 2)      AS avg_horsepower,
          MAX(updated_date)              AS updated_date
        FROM {snapshot}
        WHERE price IS NOT NULL
        GROUP BY make
        """

    from pyspark.sql import functions as F  # type: ignore[import]

    df = context.spark.sql(sql).withColumn("created_date", F.current_timestamp())
    write_jdbc(df, table="automotive_make_price_summary")
    return df
