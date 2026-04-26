# Spark DAG Unit Tests

## Overview

Comprehensive unit test suite for the Spark job Airflow DAG and utilities. All tests are passing.

**Test Results: 38/38 PASSED ✓**

## Test Files

### 1. `tests/lib/test_spark_utils.py` (26 tests)
Tests for the Kubernetes SparkApplication helper utilities.

#### Test Classes:

**TestGenerateRandomName** (5 tests)
- `test_default_name_format`: Validates random name format (spark-job-{8chars})
- `test_custom_base_name`: Tests custom base name prefix
- `test_custom_length`: Tests customizable suffix length
- `test_uniqueness`: Ensures 100 generated names are unique
- `test_valid_characters`: Validates Kubernetes naming constraints

**TestGetCurrentNamespace** (3 tests)
- `test_in_cluster_reads_from_file`: Reads namespace from service account file in-cluster
- `test_in_cluster_defaults_when_file_missing`: Defaults to "default" when file missing
- `test_out_of_cluster_defaults_to_default`: Out-of-cluster mode defaults to "default"

**TestLoadK8sClient** (3 tests)
- `test_in_cluster_config_loading`: Tests loading config from within cluster
- `test_out_of_cluster_config_loading`: Tests loading config from kubeconfig file
- `test_config_exception_handling`: Tests exception handling for config errors

**TestCloneSparkApp** (7 tests)
- `test_clone_with_auto_generated_name`: Clones with auto-generated name
- `test_clone_with_explicit_name`: Clones with explicit name
- `test_clone_uses_current_namespace_when_not_specified`: Uses current pod's namespace by default
- `test_clone_uses_explicit_namespace`: Uses explicitly provided namespaces
- `test_clone_source_not_found`: Handles missing source SparkApplication
- `test_clone_create_failure`: Handles creation failures
- Tests namespace auto-detection from current pod

**TestWaitForSparkAppCompletion** (7 tests)
- `test_wait_for_success`: Monitors until successful completion
- `test_wait_for_failure`: Detects application failure
- `test_wait_transitions_from_running_to_completed`: Tracks state transitions
- `test_wait_timeout`: Raises TimeoutError when timeout exceeded
- `test_wait_api_error`: Handles API errors during monitoring
- `test_wait_uses_current_namespace_when_not_specified`: Uses current pod's namespace
- Tests polling interval and timeout configurations

**TestCloneAndWaitForSparkApp** (3 tests)
- `test_successful_orchestration`: Tests end-to-end success
- `test_failed_orchestration`: Tests end-to-end failure
- `test_orchestration_with_custom_namespaces`: Tests namespace pass-through

### 2. `tests/test_spark_py_dag.py` (12 tests)
Tests for the Spark job DAG structure and configuration.

#### Test Functions:

**DAG Configuration** (4 tests)
- `test_dag_importable`: DAG module imports and exposes dag object
- `test_dag_configuration`: Validates DAG name, description, owner, schedule, catchup
- `test_dag_default_args`: Validates default arguments (owner, retries, etc.)
- `test_dag_has_no_dependencies`: Verifies single-task DAG with no dependencies

**Task Structure** (4 tests)
- `test_task_exists`: Required task exists in DAG
- `test_single_task_dag`: DAG contains exactly one task
- `test_task_type`: Task is properly configured TaskFlow task
- `test_run_spark_job_uses_python_py_app`: DAG runs correct SparkApplication

**DAG Logic** (4 tests)
- `test_run_spark_job_logic_success`: Tests logic when SparkApp succeeds
- `test_run_spark_job_logic_failure`: Tests logic when SparkApp fails
- `test_run_spark_job_timeout_configured`: Verifies 1-hour timeout setting
- `test_run_spark_job_in_cluster_mode`: Verifies in-cluster configuration

## Coverage Summary

### Kubernetes Utilities
- ✓ Random name generation with uniqueness guarantee
- ✓ Namespace detection from service account
- ✓ In-cluster and out-of-cluster config loading
- ✓ SparkApplication cloning with metadata copying
- ✓ Status monitoring with configurable polling
- ✓ State transition detection (RUNNING → COMPLETED/FAILED)
- ✓ Timeout handling
- ✓ API error handling
- ✓ Namespace auto-detection and explicit override
- ✓ End-to-end orchestration

### DAG Structure & Configuration
- ✓ DAG importability and metadata
- ✓ Task definition and type validation
- ✓ Default arguments inheritance
- ✓ Manual trigger (no schedule)
- ✓ Single task structure
- ✓ Correct SparkApplication target
- ✓ Retry configuration
- ✓ Namespace auto-detection in tasks

## Running the Tests

### All tests:
```bash
cd workflows/airflow
uv run -- pytest tests/lib/test_spark_utils.py tests/test_spark_py_dag.py -v
```

### Specific test file:
```bash
uv run -- pytest tests/lib/test_spark_utils.py -v
uv run -- pytest tests/test_spark_py_dag.py -v
```

### Specific test class:
```bash
uv run -- pytest tests/lib/test_spark_utils.py::TestCloneSparkApp -v
```

### Specific test:
```bash
uv run -- pytest tests/lib/test_spark_utils.py::TestGenerateRandomName::test_uniqueness -v
```

### With coverage:
```bash
uv run -- pytest tests/lib/test_spark_utils.py tests/test_spark_py_dag.py --cov=dags.tasks.spark_utils --cov=dags.spark_py_dag -v
```

## Mocking Strategy

All tests use `unittest.mock` to:
- Mock Kubernetes API calls (no cluster required)
- Mock namespace file reads
- Mock config loading
- Isolate function behavior without external dependencies

## Key Test Patterns

1. **Utility Testing**: Direct function calls with mocked dependencies
2. **DAG Validation**: Structure verification without task execution
3. **Error Handling**: Tests for all exception paths
4. **State Machine**: Monitors task state transitions
5. **Configuration**: Verifies all parameters are passed correctly

## Notes

- Tests do not require a Kubernetes cluster
- Tests do not require Airflow scheduler to be running
- All external dependencies are mocked
- Tests verify both success and failure paths
- Comprehensive namespace handling validation
