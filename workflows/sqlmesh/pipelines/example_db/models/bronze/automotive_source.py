import os
import tempfile
import typing as t
import urllib.request

from sqlmesh import ExecutionContext, model


@model(
    "example_db.automotive_source",
    kind="FULL",
    dialect="spark",
    description="Raw CSV ingestion of automotive dataset. All columns remain as strings with original hyphenated names.",
    columns={
        "symboling": "STRING",
        "normalized-losses": "STRING",
        "make": "STRING",
        "fuel-type": "STRING",
        "aspiration": "STRING",
        "num-of-doors": "STRING",
        "body-style": "STRING",
        "drive-wheels": "STRING",
        "engine-location": "STRING",
        "wheel-base": "STRING",
        "length": "STRING",
        "width": "STRING",
        "height": "STRING",
        "curb-weight": "STRING",
        "engine-type": "STRING",
        "num-of-cylinders": "STRING",
        "engine-size": "STRING",
        "fuel-system": "STRING",
        "bore": "STRING",
        "stroke": "STRING",
        "compression-ratio": "STRING",
        "horsepower": "STRING",
        "peak-rpm": "STRING",
        "city-mpg": "STRING",
        "highway-mpg": "STRING",
        "price": "STRING",
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> t.Any:
    source_url = os.environ.get(
        "EXAMPLE_DB_SOURCE_URL",
        "https://raw.githubusercontent.com/Opensourcefordatascience/Data-sets/refs/heads/master/automotive_data.csv",
    )
    
    # Download CSV to temporary file in system temp directory
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
        tmp_path = tmp_file.name
        with urllib.request.urlopen(source_url) as response:
            tmp_file.write(response.read())
    
    # Read from local temporary file using Spark
    return (
        context.spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(tmp_path)
    )

