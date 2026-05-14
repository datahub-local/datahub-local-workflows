import os
import typing as t

from sqlmesh import ExecutionContext, model


@model(
    "example_db.automotive_raw",
    kind="FULL",
    dialect="spark",
    description="Full automotive dataset written to both Spark warehouse and external JDBC target.",
    depends_on=["example_db.automotive_snapshot"],
    columns={
        "symboling": "INT",
        "normalized_losses": "INT",
        "make": "STRING",
        "fuel_type": "STRING",
        "aspiration": "STRING",
        "num_of_doors": "STRING",
        "body_style": "STRING",
        "drive_wheels": "STRING",
        "engine_location": "STRING",
        "wheel_base": "DOUBLE",
        "length": "DOUBLE",
        "width": "DOUBLE",
        "height": "DOUBLE",
        "curb_weight": "INT",
        "engine_type": "STRING",
        "num_of_cylinders": "STRING",
        "engine_size": "INT",
        "fuel_system": "STRING",
        "bore": "DOUBLE",
        "stroke": "DOUBLE",
        "compression_ratio": "DOUBLE",
        "horsepower": "INT",
        "peak_rpm": "INT",
        "city_mpg": "INT",
        "highway_mpg": "INT",
        "price": "DOUBLE",
        "updated_date": "TIMESTAMP",
        "created_date": "TIMESTAMP",
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> t.Any:
    snapshot = context.table("example_db.automotive_snapshot")
    sql = f"""
        SELECT
          symboling,
          normalized_losses,
          make,
          fuel_type,
          aspiration,
          num_of_doors,
          body_style,
          drive_wheels,
          engine_location,
          wheel_base,
          length,
          width,
          height,
          curb_weight,
          engine_type,
          num_of_cylinders,
          engine_size,
          fuel_system,
          bore,
          stroke,
          compression_ratio,
          horsepower,
          peak_rpm,
          city_mpg,
          highway_mpg,
          price,
          updated_date
        FROM {snapshot}
        """

    from pyspark.sql import functions as F  # type: ignore[import]

    df = context.spark.sql(sql).withColumn("created_date", F.current_timestamp())
    _write_jdbc(df, table="automotive_raw")
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
