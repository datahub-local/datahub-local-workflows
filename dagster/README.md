# datahub-local-dagster

This folder contains the Dagster project for orchestrating data workflows in the DataHub Local Workflows repository.

## Setup

1. Install dependencies using uv:
   ```bash
   pip install uv
   uv pip install -e .
   ```
2. (Optional) Install development dependencies:
   ```bash
   uv pip install -e .[dev]
   ```
3. Start Dagster webserver (if using dev group):
   ```bash
   uv pip install dagster-webserver
   dagster-webserver
   ```
4. Configure and run pipelines using the provided YAML files.

## Requirements

- Python 3.12
- See `pyproject.toml` for all required Dagster packages and plugins.

## License
This project is licensed under the Apache License. See `../LICENSE.txt` for details.
