import os
import sys
from pathlib import Path

# Add pipeline directory to path so model imports resolve correctly
PIPELINE_DIR = Path(__file__).parent.parent.parent / "pipelines" / "example_db"
sys.path.insert(0, str(PIPELINE_DIR))

# Defaults for JDBC env vars expected by write_jdbc
os.environ.setdefault("EXAMPLE_DB_URL", "jdbc:h2:mem:testdb")
os.environ.setdefault("EXAMPLE_DB_USER", "sa")
os.environ.setdefault("EXAMPLE_DB_PASSWORD", "")
