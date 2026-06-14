from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import V1EnvVar, V1PodSecurityContext, V1ResourceRequirements

from tasks.dbt_utils import (
    DEFAULT_STARTUP_TIMEOUT_SECONDS,
    DEFAULT_TARGET,
    NAMESPACE,
    ConfigMapEnvVarRef,
    SecretEnvVarRef,
    _validate_name,
    build_pod_env_vars,
    build_pod_resources,
)

DLT_IMAGE = "ghcr.io/datahub-local/datahub-local-workflows-dlt:main"
DLT_IMAGE_PULL_POLICY = "Always"
VALID_PIPELINES = ("ingest", "export")


@dataclass(frozen=True)
class DltTaskConfig:
    task_id: str
    project: str
    pipeline: str
    target: str = DEFAULT_TARGET
    env_vars: Mapping[str, Any] = field(default_factory=dict)
    secret_env_vars: tuple[SecretEnvVarRef, ...] = field(default_factory=tuple)
    configmap_env_vars: tuple[ConfigMapEnvVarRef, ...] = field(default_factory=tuple)
    cpu: str | None = None
    memory: str | None = None
    startup_timeout_seconds: int = DEFAULT_STARTUP_TIMEOUT_SECONDS


def validate_task_config(task_config: DltTaskConfig) -> None:
    _validate_name("project", task_config.project)
    _validate_name("target", task_config.target)
    if task_config.pipeline not in VALID_PIPELINES:
        raise AirflowException(
            f"pipeline must be one of {', '.join(VALID_PIPELINES)}: {task_config.pipeline}"
        )


def build_dlt_arguments(task_config: DltTaskConfig) -> list[str]:
    validate_task_config(task_config)
    return [
        "--pipeline", task_config.pipeline,
        "--project", task_config.project,
        "--target", task_config.target,
    ]


def build_dlt_env_vars(task_config: DltTaskConfig) -> list[V1EnvVar]:
    validate_task_config(task_config)
    return build_pod_env_vars(
        task_config.env_vars, task_config.secret_env_vars, task_config.configmap_env_vars
    )


def build_dlt_container_resources(task_config: DltTaskConfig) -> V1ResourceRequirements | None:
    validate_task_config(task_config)
    return build_pod_resources(task_config.cpu, task_config.memory)


def create_dlt_task(task_config: DltTaskConfig) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_config.task_id,
        name=task_config.task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=DLT_IMAGE,
        image_pull_policy=DLT_IMAGE_PULL_POLICY,
        arguments=build_dlt_arguments(task_config),
        env_vars=build_dlt_env_vars(task_config),
        container_resources=build_dlt_container_resources(task_config),
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False,
        service_account_name="datahub-local-core-data-airflow",
        security_context=V1PodSecurityContext(run_as_non_root=False),
        startup_timeout_seconds=task_config.startup_timeout_seconds,
    )
