"""Unit tests for spark_utils module.

Tests cover:
- Random name generation
- Kubernetes namespace detection
- SparkApplication cloning
- SparkApplication status monitoring
- End-to-end orchestration
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest
from kubernetes.client.rest import ApiException

from dags.tasks.spark_utils import (
    K8S_API_REQUEST_TIMEOUT,
    clone_and_wait_for_spark_app,
    clone_spark_app,
    generate_random_name,
    get_current_namespace,
    load_k8s_client,
    update_spark_app_arguments,
    wait_for_spark_app_completion,
)


class TestGenerateRandomName:
    """Test random name generation."""

    def test_default_name_format(self):
        """Test that generated name follows spark-job-{suffix} format."""
        name = generate_random_name()
        assert name.startswith("spark-job-")
        assert len(name) == len("spark-job-") + 8

    def test_custom_base_name(self):
        """Test custom base name."""
        name = generate_random_name(base_name="custom-app")
        assert name.startswith("custom-app-")
        assert len(name) == len("custom-app-") + 8

    def test_custom_length(self):
        """Test custom suffix length."""
        name = generate_random_name(length=5)
        assert name.startswith("spark-job-")
        assert len(name) == len("spark-job-") + 5

    def test_uniqueness(self):
        """Test that generated names are unique."""
        names = [generate_random_name() for _ in range(100)]
        assert len(set(names)) == 100, "Generated names should be unique"

    def test_valid_characters(self):
        """Test that generated names contain only valid Kubernetes characters."""
        name = generate_random_name()
        # Kubernetes names must contain only lowercase letters, numbers, and hyphens
        assert all(c.isalnum() or c == "-" for c in name)
        assert all(c.islower() or c.isdigit() or c == "-" for c in name)


class TestGetCurrentNamespace:
    """Test current namespace detection."""

    @patch("builtins.open", new_callable=mock_open, read_data="test-namespace\n")
    def test_in_cluster_reads_from_file(self, mock_file):
        """Test that in-cluster mode reads namespace from service account file."""
        namespace = get_current_namespace(in_cluster=True)
        assert namespace == "test-namespace"
        mock_file.assert_called_once_with(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        )

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_in_cluster_defaults_when_file_missing(self, mock_file):
        """Test that in-cluster mode defaults to 'default' when file is missing."""
        namespace = get_current_namespace(in_cluster=True)
        assert namespace == "default"

    def test_out_of_cluster_defaults_to_default(self):
        """Test that out-of-cluster mode defaults to 'default'."""
        namespace = get_current_namespace(in_cluster=False)
        assert namespace == "default"


class TestLoadK8sClient:
    """Test Kubernetes client loading."""

    @patch("dags.tasks.spark_utils.get_current_namespace")
    @patch("dags.tasks.spark_utils.config.load_incluster_config")
    @patch("dags.tasks.spark_utils.client.CustomObjectsApi")
    def test_in_cluster_config_loading(
        self, mock_api, mock_incluster_config, mock_get_namespace
    ):
        """Test loading Kubernetes config in-cluster."""
        mock_get_namespace.return_value = "airflow"
        mock_api_instance = MagicMock()
        mock_api.return_value = mock_api_instance

        api, namespace = load_k8s_client(in_cluster=True)

        mock_incluster_config.assert_called_once()
        assert api == mock_api_instance
        assert namespace == "airflow"

    @patch("dags.tasks.spark_utils.get_current_namespace")
    @patch("dags.tasks.spark_utils.config.load_kube_config")
    @patch("dags.tasks.spark_utils.client.CustomObjectsApi")
    def test_out_of_cluster_config_loading(
        self, mock_api, mock_kube_config, mock_get_namespace
    ):
        """Test loading Kubernetes config from kubeconfig file."""
        mock_get_namespace.return_value = "default"
        mock_api_instance = MagicMock()
        mock_api.return_value = mock_api_instance

        api, namespace = load_k8s_client(in_cluster=False)

        mock_kube_config.assert_called_once()
        assert api == mock_api_instance
        assert namespace == "default"

    @patch("dags.tasks.spark_utils.config.load_incluster_config")
    def test_config_exception_handling(self, mock_incluster_config):
        """Test handling of configuration exceptions."""
        from kubernetes import config

        mock_incluster_config.side_effect = config.ConfigException("Config error")

        with pytest.raises(config.ConfigException):
            load_k8s_client(in_cluster=True)


class TestUpdateSparkAppArguments:
    """Test argument update functionality."""

    def test_empty_parameters(self):
        """Test with no parameters."""
        args = ["--partitions", "500"]
        result = update_spark_app_arguments(args, {})
        assert result == args

    def test_add_new_parameter(self):
        """Test adding a new parameter."""
        args = ["--partitions", "500"]
        result = update_spark_app_arguments(args, {"cores": "4"})
        assert "--cores" in result
        assert "4" in result
        assert "--partitions" in result

    def test_replace_existing_parameter(self):
        """Test replacing an existing parameter."""
        args = ["--partitions", "500"]
        result = update_spark_app_arguments(args, {"partitions": "1000"})

        # Find the partitions value
        idx = result.index("--partitions")
        assert result[idx + 1] == "1000"

    def test_multiple_parameter_replacement(self):
        """Test replacing multiple parameters."""
        args = ["--partitions", "500", "--executor-cores", "2"]
        result = update_spark_app_arguments(
            args, {"partitions": "1000", "executor-cores": "4"}
        )

        # Verify both are replaced
        assert "--partitions" in result
        assert "--executor-cores" in result
        assert result[result.index("--partitions") + 1] == "1000"
        assert result[result.index("--executor-cores") + 1] == "4"

    def test_empty_arguments_with_parameters(self):
        """Test adding parameters when no existing arguments."""
        args = []
        result = update_spark_app_arguments(args, {"partitions": "500"})
        assert "--partitions" in result
        assert "500" in result


class TestCloneSparkApp:
    """Test SparkApplication cloning functionality."""

    @patch("dags.tasks.spark_utils.load_k8s_client")
    @patch("dags.tasks.spark_utils.generate_random_name")
    def test_clone_with_auto_generated_name(self, mock_gen_name, mock_load_client):
        """Test cloning with auto-generated name."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")
        mock_gen_name.return_value = "python-py-abc123"

        # Mock the source SparkApplication
        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {"driver": {"cores": 1}},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app

        # Mock the created app response with proper dict structure
        created_app = {"metadata": {"name": "python-py-abc123"}}
        mock_api.create_namespaced_custom_object.return_value = created_app

        name = clone_spark_app(source_app_name="python-py")

        assert name == "python-py-abc123"
        mock_gen_name.assert_called_once_with(base_name="python-py")
        mock_api.get_namespaced_custom_object.assert_called_once()
        mock_api.create_namespaced_custom_object.assert_called_once()
        assert (
            mock_api.get_namespaced_custom_object.call_args.kwargs["_request_timeout"]
            == K8S_API_REQUEST_TIMEOUT
        )
        assert (
            mock_api.create_namespaced_custom_object.call_args.kwargs[
                "_request_timeout"
            ]
            == K8S_API_REQUEST_TIMEOUT
        )

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_with_explicit_name(self, mock_load_client):
        """Test cloning with explicitly provided name."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {"driver": {"cores": 1}},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app

        created_app = {"metadata": {"name": "my-custom-job"}}
        mock_api.create_namespaced_custom_object.return_value = created_app

        name = clone_spark_app(
            source_app_name="python-py", new_app_name="my-custom-job"
        )

        assert name == "my-custom-job"

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_uses_current_namespace_when_not_specified(self, mock_load_client):
        """Test that cloning uses current pod's namespace when not specified."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-app"}
        }

        clone_spark_app(source_app_name="python-py")

        # Verify the source namespace call uses 'airflow'
        calls = mock_api.get_namespaced_custom_object.call_args_list
        assert calls[0][1]["namespace"] == "airflow"

        # Verify the create namespace call uses 'airflow'
        calls = mock_api.create_namespaced_custom_object.call_args_list
        assert calls[0][1]["namespace"] == "airflow"

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_uses_explicit_namespace(self, mock_load_client):
        """Test that cloning uses explicitly provided namespaces."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-app"}
        }

        clone_spark_app(
            source_app_name="python-py",
            source_namespace="source-ns",
            target_namespace="target-ns",
        )

        # Verify source namespace
        get_calls = mock_api.get_namespaced_custom_object.call_args_list
        assert get_calls[0][1]["namespace"] == "source-ns"

        # Verify target namespace
        create_calls = mock_api.create_namespaced_custom_object.call_args_list
        assert create_calls[0][1]["namespace"] == "target-ns"

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_source_not_found(self, mock_load_client):
        """Test handling when source SparkApplication is not found."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        mock_api.get_namespaced_custom_object.side_effect = ApiException(
            404, "Not Found"
        )

        with pytest.raises(ApiException):
            clone_spark_app(source_app_name="nonexistent")

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_create_failure(self, mock_load_client):
        """Test handling when creation fails."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.side_effect = ApiException(
            400, "Bad Request"
        )

        with pytest.raises(ApiException):
            clone_spark_app(source_app_name="python-py")

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_sets_suspend_false(self, mock_load_client):
        """Test that cloning sets suspend=false."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {"suspend": True, "driver": {"cores": 1}},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-app"}
        }

        clone_spark_app(source_app_name="python-py")

        # Verify the created spec has suspend=false
        create_call = mock_api.create_namespaced_custom_object.call_args
        created_spec = create_call[1]["body"]["spec"]
        assert created_spec["suspend"] is False

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_sets_time_to_live_seconds(self, mock_load_client):
        """Test that cloning sets timeToLiveSeconds to 1 hour."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {"driver": {"cores": 1}},
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-app"}
        }

        clone_spark_app(source_app_name="python-py")

        # Verify the created spec has timeToLiveSeconds=3600
        create_call = mock_api.create_namespaced_custom_object.call_args
        created_spec = create_call[1]["body"]["spec"]
        assert created_spec["timeToLiveSeconds"] == 3600

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_with_parameters(self, mock_load_client):
        """Test cloning with custom parameters."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {
                "arguments": ["--partitions", "500"],
                "driver": {"cores": 1},
            },
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-app"}
        }

        clone_spark_app(
            source_app_name="python-py",
            parameters={"partitions": "1000"},
        )

        # Verify the arguments were updated
        create_call = mock_api.create_namespaced_custom_object.call_args
        created_spec = create_call[1]["body"]["spec"]
        args = created_spec["arguments"]

        assert "--partitions" in args
        idx = args.index("--partitions")
        assert args[idx + 1] == "1000"

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_clone_preserves_other_spec_fields(self, mock_load_client):
        """Test that cloning preserves other spec fields."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        source_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": {
                "image": "apache/spark:4.1.1",
                "imagePullPolicy": "IfNotPresent",
                "mode": "cluster",
                "type": "Python",
                "driver": {"cores": 1, "memory": "512m"},
                "executor": {"cores": 1, "memory": "512m", "instances": 2},
            },
        }
        mock_api.get_namespaced_custom_object.return_value = source_app
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-app"}
        }

        clone_spark_app(source_app_name="python-py")

        # Verify the created spec preserves important fields
        create_call = mock_api.create_namespaced_custom_object.call_args
        created_spec = create_call[1]["body"]["spec"]

        assert created_spec["image"] == "apache/spark:4.1.1"
        assert created_spec["mode"] == "cluster"
        assert created_spec["type"] == "Python"
        assert created_spec["driver"]["cores"] == 1


class TestWaitForSparkAppCompletion:
    """Test SparkApplication status monitoring."""

    @patch("dags.tasks.spark_utils.client.CoreV1Api")
    @patch("dags.tasks.spark_utils.time.sleep")
    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_for_success(self, mock_load_client, mock_sleep, mock_core_v1_api):
        """Test waiting for successful completion."""
        mock_api = MagicMock()
        mock_pod_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")
        mock_core_v1_api.return_value = mock_pod_api

        # App completes successfully on first check
        app_response = {
            "status": {
                "applicationState": {
                    "state": "COMPLETED",
                },
                "driverInfo": {"podName": "test-app-driver"},
            }
        }
        mock_api.get_namespaced_custom_object.return_value = app_response
        mock_pod_api.read_namespaced_pod_log.return_value = "driver log"

        success = wait_for_spark_app_completion(app_name="test-app")

        assert success is True
        mock_api.get_namespaced_custom_object.assert_called_once()
        assert (
            mock_api.get_namespaced_custom_object.call_args.kwargs["_request_timeout"]
            == K8S_API_REQUEST_TIMEOUT
        )
        mock_pod_api.read_namespaced_pod_log.assert_called_once_with(
            name="test-app-driver",
            namespace="airflow",
            _request_timeout=K8S_API_REQUEST_TIMEOUT,
        )
        mock_sleep.assert_not_called()

    @patch("dags.tasks.spark_utils.client.CoreV1Api")
    @patch("dags.tasks.spark_utils.time.sleep")
    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_for_failure(self, mock_load_client, mock_sleep, mock_core_v1_api):
        """Test waiting for application failure."""
        mock_api = MagicMock()
        mock_pod_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")
        mock_core_v1_api.return_value = mock_pod_api

        app_response = {
            "status": {
                "applicationState": {
                    "state": "FAILED",
                    "errorMessage": "Out of memory",
                },
                "driverInfo": {"podName": "test-app-driver"},
            }
        }
        mock_api.get_namespaced_custom_object.return_value = app_response
        mock_pod_api.read_namespaced_pod_log.return_value = "driver failed"

        success = wait_for_spark_app_completion(app_name="test-app")

        assert success is False
        mock_pod_api.read_namespaced_pod_log.assert_called_once_with(
            name="test-app-driver",
            namespace="airflow",
            _request_timeout=K8S_API_REQUEST_TIMEOUT,
        )

    @patch("dags.tasks.spark_utils.client.CoreV1Api")
    @patch("dags.tasks.spark_utils.time.sleep")
    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_logs_driver_log_read_error_without_failing(
        self, mock_load_client, mock_sleep, mock_core_v1_api, capsys
    ):
        """Test driver log fetch failures are logged but do not change the result."""
        mock_api = MagicMock()
        mock_pod_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")
        mock_core_v1_api.return_value = mock_pod_api

        app_response = {
            "status": {
                "applicationState": {
                    "state": "FAILED",
                    "errorMessage": "Out of memory",
                },
                "driverInfo": {"podName": "test-app-driver"},
            }
        }
        mock_api.get_namespaced_custom_object.return_value = app_response
        mock_pod_api.read_namespaced_pod_log.side_effect = ApiException(
            status=404, reason="Not Found"
        )

        success = wait_for_spark_app_completion(app_name="test-app")

        assert success is False
        assert "Unable to read driver logs" in capsys.readouterr().out

    @patch("dags.tasks.spark_utils.time.time")
    @patch("dags.tasks.spark_utils.time.sleep")
    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_transitions_from_running_to_completed(
        self, mock_load_client, mock_sleep, mock_time
    ):
        """Test waiting through state transitions."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        # Simulate state transitions: RUNNING -> COMPLETED
        responses = [
            {"status": {"applicationState": {"state": "RUNNING"}}},
            {"status": {"applicationState": {"state": "COMPLETED"}}},
        ]
        mock_api.get_namespaced_custom_object.side_effect = responses

        # Mock time to prevent timeout
        mock_time.side_effect = [0, 5, 10]

        success = wait_for_spark_app_completion(
            app_name="test-app", poll_interval_seconds=5
        )

        assert success is True
        assert mock_api.get_namespaced_custom_object.call_count == 2
        mock_sleep.assert_called_once_with(5)

    @patch("dags.tasks.spark_utils.time.time")
    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_timeout(self, mock_load_client, mock_time):
        """Test timeout when app doesn't complete."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        app_response = {
            "status": {
                "applicationState": {
                    "state": "RUNNING",
                }
            }
        }
        mock_api.get_namespaced_custom_object.return_value = app_response

        # Mock time to simulate timeout
        mock_time.side_effect = [0, 4000]

        with pytest.raises(TimeoutError):
            wait_for_spark_app_completion(app_name="test-app", timeout_seconds=3600)

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_api_error(self, mock_load_client):
        """Test handling of API errors during monitoring."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        mock_api.get_namespaced_custom_object.side_effect = ApiException(
            500, "Internal Server Error"
        )

        with pytest.raises(ApiException):
            wait_for_spark_app_completion(app_name="test-app")

    @patch("dags.tasks.spark_utils.load_k8s_client")
    def test_wait_uses_current_namespace_when_not_specified(self, mock_load_client):
        """Test that wait uses current pod's namespace when not specified."""
        mock_api = MagicMock()
        mock_load_client.return_value = (mock_api, "airflow")

        app_response = {
            "status": {
                "applicationState": {
                    "state": "COMPLETED",
                }
            }
        }
        mock_api.get_namespaced_custom_object.return_value = app_response

        wait_for_spark_app_completion(app_name="test-app")

        # Verify namespace is 'airflow'
        calls = mock_api.get_namespaced_custom_object.call_args_list
        assert calls[0][1]["namespace"] == "airflow"


class TestCloneAndWaitForSparkApp:
    """Test end-to-end orchestration of cloning and waiting."""

    @patch("dags.tasks.spark_utils.wait_for_spark_app_completion")
    @patch("dags.tasks.spark_utils.clone_spark_app")
    def test_successful_orchestration(self, mock_clone, mock_wait):
        """Test successful end-to-end orchestration."""
        mock_clone.return_value = "python-py-abc123"
        mock_wait.return_value = True

        app_name, success = clone_and_wait_for_spark_app(source_app_name="python-py")

        assert app_name == "python-py-abc123"
        assert success is True
        mock_clone.assert_called_once()
        mock_wait.assert_called_once()

    @patch("dags.tasks.spark_utils.wait_for_spark_app_completion")
    @patch("dags.tasks.spark_utils.clone_spark_app")
    def test_failed_orchestration(self, mock_clone, mock_wait):
        """Test orchestration when app fails."""
        mock_clone.return_value = "python-py-abc123"
        mock_wait.return_value = False

        app_name, success = clone_and_wait_for_spark_app(source_app_name="python-py")

        assert app_name == "python-py-abc123"
        assert success is False

    @patch("dags.tasks.spark_utils.wait_for_spark_app_completion")
    @patch("dags.tasks.spark_utils.clone_spark_app")
    def test_orchestration_with_custom_namespaces(self, mock_clone, mock_wait):
        """Test orchestration with custom namespaces."""
        mock_clone.return_value = "python-py-abc123"
        mock_wait.return_value = True

        clone_and_wait_for_spark_app(
            source_app_name="python-py",
            source_namespace="source-ns",
            target_namespace="target-ns",
        )

        # Verify namespaces are passed through
        clone_call = mock_clone.call_args
        assert clone_call[1]["source_namespace"] == "source-ns"
        assert clone_call[1]["target_namespace"] == "target-ns"

        wait_call = mock_wait.call_args
        assert wait_call[1]["namespace"] == "target-ns"

    @patch("dags.tasks.spark_utils.wait_for_spark_app_completion")
    @patch("dags.tasks.spark_utils.clone_spark_app")
    def test_orchestration_with_parameters(self, mock_clone, mock_wait):
        """Test orchestration with parameters."""
        mock_clone.return_value = "python-py-abc123"
        mock_wait.return_value = True

        clone_and_wait_for_spark_app(
            source_app_name="python-py",
            parameters={"partitions": "1000", "executor-cores": "4"},
        )

        # Verify parameters are passed through
        clone_call = mock_clone.call_args
        assert clone_call[1]["parameters"] == {
            "partitions": "1000",
            "executor-cores": "4",
        }
