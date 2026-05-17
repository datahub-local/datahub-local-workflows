import pytest
from airflow.exceptions import AirflowException

from tasks.sqlmesh_launcher import (
    ConfigMapEnvVarRef,
    SQLMeshTaskConfig,
    SecretEnvVarRef,
    build_sqlmesh_arguments,
    build_sqlmesh_container_resources,
    build_sqlmesh_env_vars,
    create_sqlmesh_task,
)


def test_build_sqlmesh_arguments_includes_select_model():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_pi",
        pipeline_name="pi",
        select_model="pi.pi_estimate",
    )

    assert build_sqlmesh_arguments(task_config) == [
        "-p",
        "/app/pipelines/pi",
        "--gateway",
        "homelab",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "--select-model",
        "pi.pi_estimate",
        "prod",
    ]


def test_build_sqlmesh_arguments_supports_custom_environment():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_pi",
        pipeline_name="pi",
        environment="dev_ci",
    )

    assert build_sqlmesh_arguments(task_config) == [
        "-p",
        "/app/pipelines/pi",
        "--gateway",
        "homelab",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "dev_ci",
    ]


def test_build_sqlmesh_env_vars_include_shared_pipeline_and_resource_settings():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_example_db",
        pipeline_name="example_db",
        env_vars={"NESSIE_REF": "main"},
        secret_env_vars=(
            SecretEnvVarRef(secret_name="sqlmesh-state-secret", secret_key="SQLMESH_STATE_HOST"),
            SecretEnvVarRef(secret_name="example-db-secret", secret_key="EXAMPLE_DB_URL"),
        ),
        sqlmesh_vars={"SQLMESH_VAR_BATCH_ID": 42},
        executor_instances=3,
        executor_memory="4G",
        driver_cores=2,
        driver_memory="4G",
    )

    env_vars = build_sqlmesh_env_vars(task_config)
    env_var_map = {env_var.name: env_var for env_var in env_vars}

    assert env_var_map["NESSIE_REF"].value == "main"
    assert env_var_map["SQLMESH_STATE_HOST"].value_from.secret_key_ref.name == "sqlmesh-state-secret"
    assert env_var_map["EXAMPLE_DB_URL"].value_from.secret_key_ref.name == "example-db-secret"
    assert env_var_map["SPARK_EXECUTOR_INSTANCES"].value == "3"
    assert env_var_map["SPARK_EXECUTOR_MEMORY"].value == "4G"
    assert env_var_map["SPARK_DRIVER_CORES"].value == "2"
    assert env_var_map["SPARK_DRIVER_MEMORY"].value == "4G"
    assert env_var_map["SPARK_NAMESPACE"].value_from.field_ref.field_path == "metadata.namespace"
    assert env_var_map["SPARK_DRIVER_POD_NAME"].value_from.field_ref.field_path == "metadata.name"
    assert env_var_map["SPARK_DRIVER_POD_IP"].value_from.field_ref.field_path == "status.podIP"
    assert env_var_map["SQLMESH_VAR_BATCH_ID"].value == "42"


def test_build_sqlmesh_env_vars_only_adds_explicit_value_env_vars():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_example_db",
        pipeline_name="example_db",
    )

    env_var_names = {env_var.name for env_var in build_sqlmesh_env_vars(task_config)}

    assert "SPARK_NAMESPACE" in env_var_names
    assert "SPARK_DRIVER_POD_NAME" in env_var_names
    assert "SPARK_DRIVER_POD_IP" in env_var_names
    assert "NESSIE_REF" not in env_var_names
    assert "SQLMESH_STATE_HOST" not in env_var_names
    assert "EXAMPLE_DB_URL" not in env_var_names


def test_build_sqlmesh_env_vars_support_secret_and_configmap_value_from_with_renamed_env_vars():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_example_db",
        pipeline_name="example_db",
        secret_env_vars=(
            SecretEnvVarRef(
                secret_name="nessie-secret",
                secret_key="NESSIE_URI",
                env_name="SQLMESH_NESSIE_URI",
            ),
        ),
        configmap_env_vars=(
            ConfigMapEnvVarRef(
                configmap_name="sqlmesh-config",
                configmap_key="SPARK_MASTER",
                env_name="SPARK_MASTER_OVERRIDE",
            ),
        ),
    )

    env_var_map = {env_var.name: env_var for env_var in build_sqlmesh_env_vars(task_config)}

    assert env_var_map["SQLMESH_NESSIE_URI"].value_from.secret_key_ref.name == "nessie-secret"
    assert env_var_map["SQLMESH_NESSIE_URI"].value_from.secret_key_ref.key == "NESSIE_URI"
    assert env_var_map["SPARK_MASTER_OVERRIDE"].value_from.config_map_key_ref.name == "sqlmesh-config"
    assert env_var_map["SPARK_MASTER_OVERRIDE"].value_from.config_map_key_ref.key == "SPARK_MASTER"


def test_build_sqlmesh_container_resources_use_driver_settings():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_pi",
        pipeline_name="pi",
        driver_cores=2,
        driver_memory="4G",
    )

    resources = build_sqlmesh_container_resources(task_config)

    assert resources is not None
    assert resources.requests == {"cpu": "2", "memory": "4G"}
    assert resources.limits == {"cpu": "2", "memory": "4G"}


def test_create_sqlmesh_task_applies_mounts_resources_and_arguments():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_pi",
        pipeline_name="pi",
        env_vars={"NESSIE_REF": "main"},
        secret_env_vars=(
            SecretEnvVarRef(secret_name="nessie-secret", secret_key="NESSIE_URI"),
            SecretEnvVarRef(secret_name="sqlmesh-state-secret", secret_key="SQLMESH_STATE_HOST"),
            SecretEnvVarRef(secret_name="postgres-db-secret", secret_key="SQLMESH_STATE_DB"),
        ),
        executor_instances=2,
        executor_memory="2G",
        driver_cores=1,
        driver_memory="2G",
    )

    task = create_sqlmesh_task(task_config)
    env_var_map = {env_var.name: env_var for env_var in task.env_vars}

    assert task.arguments == [
        "-p",
        "/app/pipelines/pi",
        "--gateway",
        "homelab",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "prod",
    ]
    assert env_var_map["NESSIE_URI"].value_from.secret_key_ref.name == "nessie-secret"
    assert env_var_map["SQLMESH_STATE_HOST"].value_from.secret_key_ref.name == "sqlmesh-state-secret"
    assert env_var_map["SQLMESH_STATE_DB"].value_from.secret_key_ref.name == "postgres-db-secret"
    assert env_var_map["SPARK_NAMESPACE"].value_from.field_ref.field_path == "metadata.namespace"
    assert env_var_map["SPARK_DRIVER_POD_NAME"].value_from.field_ref.field_path == "metadata.name"
    assert env_var_map["SPARK_DRIVER_POD_IP"].value_from.field_ref.field_path == "status.podIP"
    assert not task.env_from
    assert not task.volumes
    assert not task.volume_mounts
    assert task.image_pull_policy == "Always"
    assert task.container_resources.requests == {"cpu": "1", "memory": "2G"}


def test_validate_task_config_does_not_require_local_sqlmesh_pipeline_files():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_missing",
        pipeline_name="missing-pipeline",
    )

    assert build_sqlmesh_arguments(task_config) == [
        "-p",
        "/app/pipelines/missing-pipeline",
        "--gateway",
        "homelab",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "prod",
    ]


def test_validate_task_config_rejects_non_prefixed_sqlmesh_vars():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_pi",
        pipeline_name="pi",
        sqlmesh_vars={"BATCH_ID": 42},
    )

    with pytest.raises(AirflowException, match="SQLMESH_VAR_"):
        build_sqlmesh_env_vars(task_config)


def test_validate_task_config_rejects_invalid_environment_name():
    task_config = SQLMeshTaskConfig(
        task_id="sqlmesh_pi",
        pipeline_name="pi",
        environment="prod/main",
    )

    with pytest.raises(AirflowException, match="environment"):
        build_sqlmesh_arguments(task_config)