from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import (
    V1ConfigMapKeySelector,
    V1EnvVar,
    V1EnvVarSource,
    V1PodSecurityContext,
    V1ResourceRequirements,
    V1SecretKeySelector,
)

DBT_IMAGE = "ghcr.io/datahub-local/datahub-local-workflows-dbt:main"
DBT_IMAGE_PULL_POLICY = "Always"
NAMESPACE = "data"
DEFAULT_TARGET = "homelab"
DEFAULT_STARTUP_TIMEOUT_SECONDS = 300

_NAME_RE = re.compile(r"[A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class SecretEnvVarRef:
    secret_name: str
    secret_key: str
    env_name: str | None = None


@dataclass(frozen=True)
class ConfigMapEnvVarRef:
    configmap_name: str
    configmap_key: str
    env_name: str | None = None


@dataclass(frozen=True)
class DbtTaskConfig:
    task_id: str
    project: str
    target: str = DEFAULT_TARGET
    select_model: str = ""
    full_refresh: bool = False
    env_vars: Mapping[str, Any] = field(default_factory=dict)
    secret_env_vars: tuple[SecretEnvVarRef, ...] = field(default_factory=tuple)
    configmap_env_vars: tuple[ConfigMapEnvVarRef, ...] = field(default_factory=tuple)
    cpu: str | None = None
    memory: str | None = None
    startup_timeout_seconds: int = DEFAULT_STARTUP_TIMEOUT_SECONDS


def _stringify_env_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _validate_name(label: str, value: str) -> None:
    if not _NAME_RE.fullmatch(value):
        raise AirflowException(
            f"{label} must contain only letters, numbers, dots, underscores, and hyphens"
        )


def build_pod_env_vars(
    env_vars: Mapping[str, Any],
    secret_env_vars: tuple[SecretEnvVarRef, ...],
    configmap_env_vars: tuple[ConfigMapEnvVarRef, ...],
) -> list[V1EnvVar]:
    """Build the K8s env var list shared by the dbt and dlt pod tasks."""
    result = [
        V1EnvVar(name=key, value=_stringify_env_value(value))
        for key, value in sorted(env_vars.items())
        if value not in (None, "")
    ]
    result.extend(
        V1EnvVar(
            name=ref.env_name or ref.secret_key,
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(name=ref.secret_name, key=ref.secret_key)
            ),
        )
        for ref in secret_env_vars
    )
    result.extend(
        V1EnvVar(
            name=ref.env_name or ref.configmap_key,
            value_from=V1EnvVarSource(
                config_map_key_ref=V1ConfigMapKeySelector(
                    name=ref.configmap_name, key=ref.configmap_key
                )
            ),
        )
        for ref in configmap_env_vars
    )
    return result


def build_pod_resources(cpu: str | None, memory: str | None) -> V1ResourceRequirements | None:
    requests: dict[str, str] = {}
    if cpu:
        requests["cpu"] = cpu
    if memory:
        requests["memory"] = memory
    if not requests:
        return None
    return V1ResourceRequirements(requests=requests, limits=dict(requests))


def validate_task_config(task_config: DbtTaskConfig) -> None:
    _validate_name("project", task_config.project)
    _validate_name("target", task_config.target)


def build_dbt_arguments(task_config: DbtTaskConfig) -> list[str]:
    validate_task_config(task_config)
    arguments = ["--project", task_config.project, "--target", task_config.target]
    if task_config.select_model:
        arguments.extend(["--select", task_config.select_model])
    if task_config.full_refresh:
        arguments.append("--full-refresh")
    return arguments


def build_dbt_env_vars(task_config: DbtTaskConfig) -> list[V1EnvVar]:
    validate_task_config(task_config)
    return build_pod_env_vars(
        task_config.env_vars, task_config.secret_env_vars, task_config.configmap_env_vars
    )


def build_dbt_container_resources(task_config: DbtTaskConfig) -> V1ResourceRequirements | None:
    validate_task_config(task_config)
    return build_pod_resources(task_config.cpu, task_config.memory)


def create_dbt_task(task_config: DbtTaskConfig) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_config.task_id,
        name=task_config.task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        image_pull_policy=DBT_IMAGE_PULL_POLICY,
        arguments=build_dbt_arguments(task_config),
        env_vars=build_dbt_env_vars(task_config),
        container_resources=build_dbt_container_resources(task_config),
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False,
        service_account_name="datahub-local-core-data-airflow",
        security_context=V1PodSecurityContext(run_as_non_root=False),
        startup_timeout_seconds=task_config.startup_timeout_seconds,
    )
