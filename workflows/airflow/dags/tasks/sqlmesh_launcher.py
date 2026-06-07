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
    V1ObjectFieldSelector,
    V1PodSecurityContext,
    V1ResourceRequirements,
    V1SecretKeySelector,
)

SQLMESH_IMAGE = "ghcr.io/datahub-local/datahub-local-workflows-sqlmesh:main"
SQLMESH_IMAGE_PULL_POLICY = "Always"
NAMESPACE = "data"
DEFAULT_GATEWAY = "homelab"
DEFAULT_ENVIRONMENT = "prod"


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
class SQLMeshTaskConfig:
    task_id: str
    pipeline_name: str
    select_model: str = ""
    gateway: str = DEFAULT_GATEWAY
    environment: str = DEFAULT_ENVIRONMENT
    env_vars: Mapping[str, Any] = field(default_factory=dict)
    sqlmesh_vars: Mapping[str, Any] = field(default_factory=dict)
    secret_env_vars: tuple[SecretEnvVarRef, ...] = field(default_factory=tuple)
    configmap_env_vars: tuple[ConfigMapEnvVarRef, ...] = field(default_factory=tuple)
    executor_instances: int | None = None
    executor_memory: str | None = None
    driver_cores: int | None = None
    driver_memory: str | None = None
    command: str = "plan"
    run_migrate: bool = False


def _stringify_env_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _build_pod_metadata_env_vars(existing_names: set[str]) -> list[V1EnvVar]:
    pod_metadata_fields = {
        "SPARK_NAMESPACE": "metadata.namespace",
        "SPARK_DRIVER_POD_NAME": "metadata.name",
        "SPARK_DRIVER_POD_IP": "status.podIP",
    }
    return [
        V1EnvVar(
            name=name,
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(field_path=field_path)
            ),
        )
        for name, field_path in pod_metadata_fields.items()
        if name not in existing_names
    ]


def validate_task_config(task_config: SQLMeshTaskConfig) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", task_config.pipeline_name):
        raise AirflowException(
            "pipeline_name must contain only letters, numbers, dots, underscores, and hyphens"
        )
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", task_config.environment):
        raise AirflowException(
            "environment must contain only letters, numbers, dots, underscores, and hyphens"
        )

    invalid_sqlmesh_vars = sorted(
        str(key)
        for key in task_config.sqlmesh_vars
        if not str(key).startswith("SQLMESH_VAR_")
    )
    if invalid_sqlmesh_vars:
        raise AirflowException(
            "sqlmesh_vars keys must start with SQLMESH_VAR_: "
            f"{', '.join(invalid_sqlmesh_vars)}"
        )


def build_sqlmesh_arguments(task_config: SQLMeshTaskConfig) -> list[str]:
    validate_task_config(task_config)

    # Run migrate if requested (skip other commands)
    if task_config.run_migrate:
        return [
            "-p",
            f"/app/pipelines/{task_config.pipeline_name}",
            "--gateway",
            task_config.gateway,
            "migrate",
        ]

    # Regular command (plan or run)
    arguments = [
        "-p",
        f"/app/pipelines/{task_config.pipeline_name}",
        "--gateway",
        task_config.gateway,
        task_config.command,
        "--auto-apply",
        "--no-prompts",
    ]

    if task_config.select_model:
        arguments.extend(["--select-model", task_config.select_model])
    if task_config.environment:
        arguments.append(task_config.environment)
    return arguments


def build_sqlmesh_env_vars(task_config: SQLMeshTaskConfig) -> list[V1EnvVar]:
    validate_task_config(task_config)
    env_vars = [
        V1EnvVar(name=key, value=_stringify_env_value(value))
        for key, value in sorted(task_config.env_vars.items())
        if value not in (None, "")
    ]
    env_vars.extend(
        V1EnvVar(
            name=env_var_ref.env_name or env_var_ref.secret_key,
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name=env_var_ref.secret_name,
                    key=env_var_ref.secret_key,
                )
            ),
        )
        for env_var_ref in task_config.secret_env_vars
    )
    env_vars.extend(
        V1EnvVar(
            name=env_var_ref.env_name or env_var_ref.configmap_key,
            value_from=V1EnvVarSource(
                config_map_key_ref=V1ConfigMapKeySelector(
                    name=env_var_ref.configmap_name,
                    key=env_var_ref.configmap_key,
                )
            ),
        )
        for env_var_ref in task_config.configmap_env_vars
    )
    env_vars.extend(_build_pod_metadata_env_vars({env_var.name for env_var in env_vars}))

    if task_config.executor_instances is not None:
        env_vars.append(
            V1EnvVar(
                name="SPARK_EXECUTOR_INSTANCES",
                value=str(task_config.executor_instances),
            )
        )
    if task_config.executor_memory:
        env_vars.append(
            V1EnvVar(name="SPARK_EXECUTOR_MEMORY", value=task_config.executor_memory)
        )
    if task_config.driver_cores is not None:
        env_vars.append(
            V1EnvVar(name="SPARK_DRIVER_CORES", value=str(task_config.driver_cores))
        )
    if task_config.driver_memory:
        env_vars.append(
            V1EnvVar(name="SPARK_DRIVER_MEMORY", value=task_config.driver_memory)
        )

    env_vars.extend(
        V1EnvVar(name=key, value=_stringify_env_value(value))
        for key, value in sorted(task_config.sqlmesh_vars.items())
        if value not in (None, "")
    )
    return env_vars


def build_sqlmesh_container_resources(
    task_config: SQLMeshTaskConfig,
) -> V1ResourceRequirements | None:
    validate_task_config(task_config)
    requests: dict[str, str] = {}
    limits: dict[str, str] = {}

    if task_config.driver_cores is not None:
        cpu = str(task_config.driver_cores)
        requests["cpu"] = cpu
        limits["cpu"] = cpu
    if task_config.driver_memory:
        requests["memory"] = task_config.driver_memory
        limits["memory"] = task_config.driver_memory

    if not requests and not limits:
        return None

    return V1ResourceRequirements(requests=requests, limits=limits)


def create_sqlmesh_task(task_config: SQLMeshTaskConfig) -> KubernetesPodOperator:
    validate_task_config(task_config)
    return KubernetesPodOperator(
        task_id=task_config.task_id,
        name=task_config.task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=SQLMESH_IMAGE,
        image_pull_policy=SQLMESH_IMAGE_PULL_POLICY,
        arguments=build_sqlmesh_arguments(task_config),
        env_vars=build_sqlmesh_env_vars(task_config),
        container_resources=build_sqlmesh_container_resources(task_config),
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False,
        service_account_name="datahub-local-core-data-airflow-spark",
        security_context=V1PodSecurityContext(
            run_as_non_root=False,  # Required for Spark to function
        ),
    )