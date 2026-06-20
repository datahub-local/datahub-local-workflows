from __future__ import annotations

import time
from dataclasses import dataclass

import requests
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from utils.k8s import get_secret_entries

N8N_BASE_URL = "http://datahub-local-core-automation-n8n.automation.svc.cluster.local:80"
N8N_WEBHOOK_BASE_URL = "http://datahub-local-core-automation-n8n-webhook.automation.svc.cluster.local:80"
DEFAULT_POKE_INTERVAL = 30
DEFAULT_TIMEOUT = 3600


@dataclass(frozen=True)
class N8nTaskConfig:
    task_id: str
    workflow_name: str
    poke_interval: int = DEFAULT_POKE_INTERVAL
    timeout: int = DEFAULT_TIMEOUT
    params: dict | None = None


def _run_n8n_workflow(
    workflow_name: str,
    poke_interval: int,
    timeout: int,
    params: dict | None = None,
) -> None:
    entries = get_secret_entries("n8n-secrets", ["N8N_API_KEY"])
    if not entries:
        raise AirflowException("Secret n8n-secrets/N8N_API_KEY not found")
    api_key = entries[0].value
    api_headers = {"Content-Type": "application/json", "X-N8N-API-KEY": api_key}

    # Resolve workflow name → ID
    response = requests.get(f"{N8N_BASE_URL}/api/v1/workflows", headers=api_headers)
    response.raise_for_status()
    workflows = response.json().get("data", [])
    workflow = next((wf for wf in workflows if wf.get("name") == workflow_name), None)
    if workflow is None:
        raise AirflowException(f"n8n workflow not found: {workflow_name!r}")
    workflow_id = workflow["id"]

    # Resolve workflow → webhook path
    response = requests.get(f"{N8N_BASE_URL}/api/v1/workflows/{workflow_id}", headers=api_headers)
    response.raise_for_status()
    nodes = response.json().get("nodes", [])
    webhook_node = next(
        (n for n in nodes if n.get("type") == "n8n-nodes-base.webhook"),
        None,
    )
    if webhook_node is None:
        raise AirflowException(
            f"Workflow {workflow_name!r} has no webhook trigger node; "
            "add an 'n8n-nodes-base.webhook' node with httpMethod=POST"
        )
    webhook_path = webhook_node.get("parameters", {}).get("path")

    # Trigger execution via webhook
    response = requests.post(
        f"{N8N_WEBHOOK_BASE_URL}/webhook/{webhook_path}",
        headers=api_headers,
        json=params or {},
    )
    response.raise_for_status()

    # Retrieve execution ID — newer n8n versions return it in the body; otherwise query
    body = response.json() if response.content else {}
    execution_id = body.get("executionId") or body.get("data", {}).get("executionId")
    if not execution_id:
        time.sleep(2)
        list_resp = requests.get(
            f"{N8N_BASE_URL}/api/v1/executions",
            headers=api_headers,
            params={"workflowId": workflow_id, "limit": 1},
        )
        list_resp.raise_for_status()
        execs = list_resp.json().get("data", [])
        execution_id = execs[0]["id"] if execs else None
    if not execution_id:
        raise AirflowException(f"n8n did not return an execution ID for workflow {workflow_name!r}")

    # Poll until completion
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = requests.get(
            f"{N8N_BASE_URL}/api/v1/executions/{execution_id}", headers=api_headers
        )
        response.raise_for_status()
        status = response.json().get("status")
        if status == "success":
            return
        if status in ("error", "crashed"):
            raise AirflowException(f"n8n execution {execution_id} failed: status={status}")
        time.sleep(poke_interval)

    raise AirflowException(f"n8n execution {execution_id} timed out after {timeout}s")


def create_n8n_task(config: N8nTaskConfig) -> PythonOperator:
    return PythonOperator(
        task_id=config.task_id,
        python_callable=_run_n8n_workflow,
        op_kwargs={
            "workflow_name": config.workflow_name,
            "poke_interval": config.poke_interval,
            "timeout": config.timeout,
            "params": config.params,
        },
    )
