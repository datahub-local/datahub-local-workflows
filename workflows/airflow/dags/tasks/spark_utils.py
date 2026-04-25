"""Helper utilities for managing Kubernetes SparkApplications via Airflow.

This module provides functions to clone and monitor SparkApplication resources
in a Kubernetes cluster, with support for random naming and status polling.
"""

import random
import string
import time
from typing import Optional

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException


def generate_random_name(base_name: str = "spark-job", length: int = 8) -> str:
    """Generate a random name for a SparkApplication resource.

    Args:
        base_name: Prefix for the generated name (default: "spark-job")
        length: Length of random suffix (default: 8)

    Returns:
        A unique name suitable for Kubernetes resource naming (e.g., spark-job-a1b2c3d4)
    """
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return f"{base_name}-{suffix}"


def load_k8s_client(
    in_cluster: bool = True,
) -> tuple[client.CustomObjectsApi, str]:
    """Load Kubernetes client and determine the namespace.

    Args:
        in_cluster: If True, load config from within a cluster (default: True).
                   If False, load from kubeconfig file.

    Returns:
        A tuple of (CustomObjectsApi client, namespace string)

    Raises:
        ConfigException: If unable to load Kubernetes configuration.
    """
    try:
        if in_cluster:
            config.load_incluster_config()
            namespace = "default"
            try:
                with open(
                    "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
                ) as f:
                    namespace = f.read().strip()
            except FileNotFoundError:
                pass
        else:
            config.load_kube_config()
            namespace = "default"
    except config.ConfigException as e:
        raise config.ConfigException(
            f"Unable to load Kubernetes configuration: {e}"
        ) from e

    api = client.CustomObjectsApi()
    return api, namespace


def clone_spark_app(
    source_app_name: str,
    source_namespace: str = "default",
    target_namespace: str = "default",
    new_app_name: Optional[str] = None,
    in_cluster: bool = True,
) -> str:
    """Clone a SparkApplication resource with a new name.

    Args:
        source_app_name: Name of the source SparkApplication to clone
        source_namespace: Namespace containing the source SparkApplication
        target_namespace: Namespace where the cloned app will be created
        new_app_name: Name for the cloned app (auto-generated if None)
        in_cluster: If True, run from within a cluster (default: True)

    Returns:
        The name of the newly created SparkApplication

    Raises:
        ApiException: If unable to read/create the resource in Kubernetes
    """
    api, _ = load_k8s_client(in_cluster=in_cluster)

    # Generate name if not provided
    if new_app_name is None:
        new_app_name = generate_random_name(base_name=source_app_name)

    group = "sparkoperator.k8s.io"
    version = "v1beta2"
    plural = "sparkapplications"

    try:
        # Retrieve the source SparkApplication
        source_app = api.get_namespaced_custom_object(
            group=group,
            version=version,
            namespace=source_namespace,
            plural=plural,
            name=source_app_name,
        )
    except ApiException as e:
        raise ApiException(
            f"Failed to get SparkApplication '{source_app_name}' from namespace "
            f"'{source_namespace}': {e}"
        ) from e

    # Clone the application
    cloned_app = {
        "apiVersion": source_app["apiVersion"],
        "kind": source_app["kind"],
        "metadata": {
            "name": new_app_name,
            "namespace": target_namespace,
        },
        "spec": source_app["spec"].copy(),
    }

    try:
        created_app = api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=target_namespace,
            plural=plural,
            body=cloned_app,
        )
        print(
            f"Successfully created SparkApplication '{new_app_name}' in namespace '{target_namespace}'"
        )
        return created_app.metadata.name
    except ApiException as e:
        raise ApiException(
            f"Failed to create cloned SparkApplication '{new_app_name}': {e}"
        ) from e


def wait_for_spark_app_completion(
    app_name: str,
    namespace: str = "default",
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 10,
    in_cluster: bool = True,
) -> bool:
    """Wait for a SparkApplication to complete (successfully or with failure).

    Args:
        app_name: Name of the SparkApplication to monitor
        namespace: Namespace containing the SparkApplication
        timeout_seconds: Maximum time to wait before raising timeout error (default: 3600)
        poll_interval_seconds: Time between status checks in seconds (default: 10)
        in_cluster: If True, run from within a cluster (default: True)

    Returns:
        True if the application completed successfully, False if it failed

    Raises:
        TimeoutError: If the application does not complete within timeout_seconds
        ApiException: If unable to query the SparkApplication status
    """
    api, _ = load_k8s_client(in_cluster=in_cluster)

    group = "sparkoperator.k8s.io"
    version = "v1beta2"
    plural = "sparkapplications"

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"SparkApplication '{app_name}' did not complete within {timeout_seconds} seconds"
            )

        try:
            app = api.get_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=app_name,
            )
        except ApiException as e:
            raise ApiException(
                f"Failed to query SparkApplication '{app_name}': {e}"
            ) from e

        status = app.get("status", {})
        app_state = status.get("applicationState", {}).get("state", "UNKNOWN")

        print(f"[{int(elapsed)}s] SparkApplication '{app_name}' state: {app_state}")

        if app_state == "COMPLETED":
            print(f"✓ SparkApplication '{app_name}' completed successfully")
            return True
        elif app_state == "FAILED":
            error_msg = status.get("applicationState", {}).get(
                "errorMessage", "Unknown error"
            )
            print(f"✗ SparkApplication '{app_name}' failed: {error_msg}")
            return False
        elif app_state == "UNKNOWN":
            print(f"⚠ SparkApplication '{app_name}' state is unknown, waiting...")

        time.sleep(poll_interval_seconds)


def clone_and_wait_for_spark_app(
    source_app_name: str,
    source_namespace: str = "default",
    target_namespace: str = "default",
    new_app_name: Optional[str] = None,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 10,
    in_cluster: bool = True,
) -> tuple[str, bool]:
    """Clone a SparkApplication and wait for it to complete.

    This is the main helper function that orchestrates cloning and monitoring.

    Args:
        source_app_name: Name of the source SparkApplication to clone
        source_namespace: Namespace containing the source SparkApplication
        target_namespace: Namespace where the cloned app will be created
        new_app_name: Name for the cloned app (auto-generated if None)
        timeout_seconds: Maximum time to wait for completion (default: 3600)
        poll_interval_seconds: Time between status checks (default: 10)
        in_cluster: If True, run from within a cluster (default: True)

    Returns:
        A tuple of (created_app_name, success_status) where:
        - created_app_name: The name of the cloned SparkApplication
        - success_status: True if successful, False if failed

    Raises:
        ApiException: If unable to interact with Kubernetes
        TimeoutError: If the application doesn't complete in time
    """
    # Clone the SparkApplication
    created_name = clone_spark_app(
        source_app_name=source_app_name,
        source_namespace=source_namespace,
        target_namespace=target_namespace,
        new_app_name=new_app_name,
        in_cluster=in_cluster,
    )

    # Wait for completion
    success = wait_for_spark_app_completion(
        app_name=created_name,
        namespace=target_namespace,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        in_cluster=in_cluster,
    )

    return created_name, success
