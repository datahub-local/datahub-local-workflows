"""Shared utilities for Airflow Kubernetes pod tasks."""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from kubernetes.client import (
    V1ConfigMapKeySelector,
    V1EnvVar,
    V1EnvVarSource,
    V1ResourceRequirements,
    V1SecretKeySelector,
)

# ==============
# Shared Classes
# ==============


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


# ==============
# Shared Functions
# ==============

_NAME_RE = re.compile(r"[A-Za-z0-9_.-]+")


def _stringify_env_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _validate_name(label: str, value: str) -> None:
    if not _NAME_RE.fullmatch(value):
        from airflow.exceptions import AirflowException

        raise AirflowException(
            f"{label} must contain only letters, numbers, dots, underscores, and hyphens"
        )


def build_pod_env_vars(
    env_vars: Mapping[str, Any],
    secret_env_vars: tuple[SecretEnvVarRef, ...],
    configmap_env_vars: tuple[ConfigMapEnvVarRef, ...],
) -> list[V1EnvVar]:
    """Build the K8s env var list shared by the dbt, dlt, and others pod tasks."""
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
