"""Helper utilities for managing Kubernetes SparkApplications via Airflow.

This module provides functions to clone and monitor SparkApplication resources
in a Kubernetes cluster, with support for random naming and status polling.
"""

import random
import string
import time
from copy import deepcopy
from typing import Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from .. import SPARK_JOB_PREFIX


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


def update_spark_app_arguments(arguments: list, parameters: dict) -> list:
    """Update SparkApplication arguments with provided parameters.

    Replaces existing argument values or adds new --key value pairs.

    Args:
        arguments: Existing arguments list (e.g., ["--partitions", "500"])
        parameters: Dict of key-value pairs to set (e.g., {"partitions": "1000"})

    Returns:
        Updated arguments list with parameters applied
    """
    if not parameters:
        return arguments

    # Create a dict from existing arguments for easy lookup
    args_dict = {}
    i = 0
    while i < len(arguments):
        if arguments[i].startswith("--"):
            key = arguments[i][2:]  # Remove -- prefix
            if i + 1 < len(arguments) and not arguments[i + 1].startswith("--"):
                args_dict[key] = arguments[i + 1]
                i += 2
            else:
                args_dict[key] = None
                i += 1
        else:
            i += 1

    # Update with provided parameters
    for key, value in parameters.items():
        args_dict[key] = str(value)

    # Convert back to arguments list
    result = []
    for key, value in args_dict.items():
        result.append(f"--{key}")
        if value is not None:
            result.append(value)

    return result


def get_current_namespace(in_cluster: bool = True) -> str:
    """Get the namespace where the current pod is running.

    Args:
        in_cluster: If True, read from in-cluster service account (default: True).

    Returns:
        The current namespace (defaults to "default" if unable to determine)
    """
    namespace = "default"
    if in_cluster:
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
                namespace = f.read().strip()
        except FileNotFoundError:
            pass
    return namespace


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
        else:
            config.load_kube_config()
    except config.ConfigException as e:
        raise config.ConfigException(
            f"Unable to load Kubernetes configuration: {e}"
        ) from e

    namespace = get_current_namespace(in_cluster=in_cluster)
    api = client.CustomObjectsApi()
    return api, namespace


def clone_spark_app(
    source_app_name: str,
    source_namespace: Optional[str] = None,
    target_namespace: Optional[str] = None,
    new_app_name: Optional[str] = None,
    parameters: Optional[dict] = None,
    in_cluster: bool = True,
) -> str:
    """Clone a SparkApplication resource with a new name.

    Clones the spec, sets suspend=false, updates arguments with parameters,
    and sets timeToLiveSeconds to 1 hour for automatic cleanup.

    Args:
        source_app_name: Name of the source SparkApplication to clone
        source_namespace: Namespace containing the source SparkApplication.
                         If None, uses the current pod's namespace.
        target_namespace: Namespace where the cloned app will be created.
                         If None, uses the current pod's namespace.
        new_app_name: Name for the cloned app (auto-generated if None)
        parameters: Dict of parameters to set as arguments (e.g., {"partitions": "1000"}).
                   Replaces existing values if they already exist.
        in_cluster: If True, run from within a cluster (default: True)

    Returns:
        The name of the newly created SparkApplication

    Raises:
        ApiException: If unable to read/create the resource in Kubernetes
    """
    api, current_ns = load_k8s_client(in_cluster=in_cluster)

    # Use current namespace if not specified
    if source_namespace is None:
        source_namespace = current_ns
    if target_namespace is None:
        target_namespace = current_ns

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

    # Deep copy the spec to avoid modifying the source
    spec = deepcopy(source_app["spec"])

    # Set suspend to false to execute immediately
    spec["suspend"] = False

    # Set timeToLiveSeconds to 1 hour (3600 seconds) for cleanup
    spec["timeToLiveSeconds"] = 3600

    # Update arguments with provided parameters
    if parameters:
        current_arguments = spec.get("arguments", [])
        spec["arguments"] = update_spark_app_arguments(current_arguments, parameters)
        print(f"Updated arguments: {spec['arguments']}")

    # Clone the application with updated spec
    cloned_app = {
        "apiVersion": source_app["apiVersion"],
        "kind": source_app["kind"],
        "metadata": {
            "name": new_app_name,
            "namespace": target_namespace,
        },
        "spec": spec,
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
        return created_app["metadata"]["name"]
    except ApiException as e:
        raise ApiException(
            f"Failed to create cloned SparkApplication '{new_app_name}': {e}"
        ) from e


def wait_for_spark_app_completion(
    app_name: str,
    namespace: Optional[str] = None,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 10,
    in_cluster: bool = True,
) -> bool:
    """Wait for a SparkApplication to complete (successfully or with failure).

    Args:
        app_name: Name of the SparkApplication to monitor
        namespace: Namespace containing the SparkApplication.
                  If None, uses the current pod's namespace.
        timeout_seconds: Maximum time to wait before raising timeout error (default: 3600)
        poll_interval_seconds: Time between status checks in seconds (default: 10)
        in_cluster: If True, run from within a cluster (default: True)

    Returns:
        True if the application completed successfully, False if it failed

    Raises:
        TimeoutError: If the application does not complete within timeout_seconds
        ApiException: If unable to query the SparkApplication status
    """
    api, current_ns = load_k8s_client(in_cluster=in_cluster)

    # Use current namespace if not specified
    if namespace is None:
        namespace = current_ns

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
    source_namespace: Optional[str] = None,
    target_namespace: Optional[str] = None,
    new_app_name: Optional[str] = None,
    parameters: Optional[dict] = None,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 10,
    in_cluster: bool = True,
) -> tuple[str, bool]:
    """Clone a SparkApplication and wait for it to complete.

    This is the main helper function that orchestrates cloning and monitoring.

    Args:
        source_app_name: Name of the source SparkApplication to clone
        source_namespace: Namespace containing the source SparkApplication.
                         If None, uses the current pod's namespace.
        target_namespace: Namespace where the cloned app will be created.
                         If None, uses the current pod's namespace.
        new_app_name: Name for the cloned app (auto-generated if None)
        parameters: Dict of parameters to set as arguments (e.g., {"partitions": "1000"}).
                   Replaces existing values if they already exist.
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
    final_source_app_name = f"{SPARK_JOB_PREFIX}{source_app_name}"

    # Clone the SparkApplication with parameters
    created_name = clone_spark_app(
        source_app_name=final_source_app_name,
        source_namespace=source_namespace,
        target_namespace=target_namespace,
        new_app_name=new_app_name,
        parameters=parameters,
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
