"""Pure-Python task implementations for tests and DAG usage.

Keeping these functions in a separate module lets us run lightweight tests
without requiring Airflow to be installed.
"""


def fetch_sample(context: dict | None = None) -> str:
    """Simulate fetching sample data. Returns a message for testing."""
    msg = "Fetching sample data..."
    print(msg)
    return msg


def process_sample(context: dict | None = None) -> str:
    """Simulate processing sample data. Returns a message for testing."""
    msg = "Processing sample data..."
    print(msg)
    return msg
