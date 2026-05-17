import os
import typing as t


def write_jdbc(df: t.Any, table: str) -> None:
    db_url = os.environ["EXAMPLE_DB_URL"]
    schema = os.environ.get("EXAMPLE_DB_SCHEMA", "public")
    user = os.environ["EXAMPLE_DB_USER"]
    password = os.environ.get("EXAMPLE_DB_PASSWORD", "")
    driver = os.environ.get("EXAMPLE_DB_DRIVER", "org.h2.Driver")
    jdbc_options = {
        "url": db_url,
        "dbtable": f"{schema}.{table}",
        "user": user,
        "password": password,
        "driver": driver,
    }

    is_h2_connection = "h2" in driver.casefold() or db_url.casefold().startswith(
        "jdbc:h2:"
    )
    if not is_h2_connection:
        jdbc_options["prepareQuery"] = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    df.write.format("jdbc").options(**jdbc_options).mode("overwrite").save()