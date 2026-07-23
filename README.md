# datahub-local-ai

This repository serves as a centralized collection of workflow definitions for use with Datahub.local and related tools. It is designed to organize, version, and share workflows that automate or orchestrate data processes in local or development environments.

## Repository Structure

Each subdirectory (e.g., `n8n/`) contains workflows for a specific tool or platform.

- `n8n/` includes n8n workflow definitions and related files.
- `superset/` includes Superset dashboard definitions per project and a Helm release that deploys them as ConfigMaps.
- `workflows/` is the main directory for all workflow definitions, organized by tool or format. For example:
- `workflows/airflow/` is an Airflow project to orchestrate data workflows.
- `workflows/dbt/` is a dbt Core project (Trino / DuckDB, Iceberg + Apache Polaris) for the data transformation pipelines.
- `workflows/dlt/` is a [dlt](https://dlthub.com) project for the ingest and reverse-ETL export pipelines that run around dbt.

Additional directories may be added in the future to support other workflow engines or formats.

## Usage

1. Browse the relevant subdirectory for your workflow engine (e.g., `n8n/`).
2. Follow the instructions in each subdirectory (if available) to import or use the workflows.
3. Contribute new workflows by adding them to the appropriate directory or creating a new one for a different tool.

## Contributing

- Please structure new workflows in clearly named directories by tool or format.
- Include documentation or usage notes as needed.
- Open a pull request for review.

## License

See [LICENSE](LICENSE) for details.
