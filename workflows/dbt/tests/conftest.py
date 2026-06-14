import os
import sys
import tempfile
from pathlib import Path

# Project root (workflows/dbt) holds the `dbt_runner` package and the dbt project dirs.
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# DuckDB warehouse files for the local target (one file per medallion catalog). A shared
# temp dir keeps dbt parse/build and the dlt local ingest pointing at the same catalogs.
_WAREHOUSE = Path(tempfile.gettempdir()) / "datahub-local-duckdb"
_WAREHOUSE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("DBT_DUCKDB_PATH", str(_WAREHOUSE / "bronze.duckdb"))
os.environ.setdefault("DBT_DUCKDB_SILVER_PATH", str(_WAREHOUSE / "silver.duckdb"))
os.environ.setdefault("DBT_DUCKDB_GOLD_PATH", str(_WAREHOUSE / "gold.duckdb"))
os.environ.setdefault("DBT_DUCKDB_PI_PATH", str(_WAREHOUSE / "test.duckdb"))
