"""Unit tests for sample_tasks module.

Tests cover:
- Basic task execution
- Return value types and content
- Context parameter handling
- Edge cases
"""

from unittest.mock import MagicMock, patch


from dags.tasks.sample_tasks import fetch_sample, process_sample


class TestFetchSample:
    """Test fetch_sample task."""

    def test_fetch_sample_returns_string(self):
        """Test that fetch_sample returns a string."""
        result = fetch_sample()
        assert isinstance(result, str)

    def test_fetch_sample_contains_expected_message(self):
        """Test that fetch_sample returns expected message."""
        result = fetch_sample()
        assert "Fetching" in result
        assert "sample data" in result

    def test_fetch_sample_exact_message(self):
        """Test exact message content."""
        result = fetch_sample()
        assert result == "Fetching sample data..."

    def test_fetch_sample_with_none_context(self):
        """Test fetch_sample with None context parameter."""
        result = fetch_sample(context=None)
        assert isinstance(result, str)
        assert "Fetching" in result

    def test_fetch_sample_with_dict_context(self):
        """Test fetch_sample with dict context parameter."""
        context = {"task_id": "fetch_sample", "run_id": "test"}
        result = fetch_sample(context=context)
        assert isinstance(result, str)
        assert "Fetching" in result

    def test_fetch_sample_with_empty_dict_context(self):
        """Test fetch_sample with empty dict context."""
        result = fetch_sample(context={})
        assert isinstance(result, str)

    @patch("builtins.print")
    def test_fetch_sample_prints_message(self, mock_print):
        """Test that fetch_sample prints the message."""
        fetch_sample()
        mock_print.assert_called_once_with("Fetching sample data...")

    def test_fetch_sample_is_idempotent(self):
        """Test that calling fetch_sample multiple times produces same result."""
        result1 = fetch_sample()
        result2 = fetch_sample()
        result3 = fetch_sample()
        assert result1 == result2 == result3

    def test_fetch_sample_non_empty_string(self):
        """Test that fetch_sample returns non-empty string."""
        result = fetch_sample()
        assert len(result) > 0


class TestProcessSample:
    """Test process_sample task."""

    def test_process_sample_returns_string(self):
        """Test that process_sample returns a string."""
        result = process_sample()
        assert isinstance(result, str)

    def test_process_sample_contains_expected_message(self):
        """Test that process_sample returns expected message."""
        result = process_sample()
        assert "Processing" in result
        assert "sample data" in result

    def test_process_sample_exact_message(self):
        """Test exact message content."""
        result = process_sample()
        assert result == "Processing sample data..."

    def test_process_sample_with_none_context(self):
        """Test process_sample with None context parameter."""
        result = process_sample(context=None)
        assert isinstance(result, str)
        assert "Processing" in result

    def test_process_sample_with_dict_context(self):
        """Test process_sample with dict context parameter."""
        context = {"task_id": "process_sample", "run_id": "test"}
        result = process_sample(context=context)
        assert isinstance(result, str)
        assert "Processing" in result

    def test_process_sample_with_empty_dict_context(self):
        """Test process_sample with empty dict context."""
        result = process_sample(context={})
        assert isinstance(result, str)

    @patch("builtins.print")
    def test_process_sample_prints_message(self, mock_print):
        """Test that process_sample prints the message."""
        process_sample()
        mock_print.assert_called_once_with("Processing sample data...")

    def test_process_sample_is_idempotent(self):
        """Test that calling process_sample multiple times produces same result."""
        result1 = process_sample()
        result2 = process_sample()
        result3 = process_sample()
        assert result1 == result2 == result3

    def test_process_sample_non_empty_string(self):
        """Test that process_sample returns non-empty string."""
        result = process_sample()
        assert len(result) > 0


class TestTasksIntegration:
    """Test task integration and sequencing."""

    def test_fetch_then_process_sequence(self):
        """Test typical fetch-then-process sequence."""
        fetch_result = fetch_sample()
        assert fetch_result is not None

        process_result = process_sample()
        assert process_result is not None

        # Both should return non-empty strings
        assert len(fetch_result) > 0
        assert len(process_result) > 0

    def test_fetch_and_process_different_messages(self):
        """Test that fetch and process return different messages."""
        fetch_result = fetch_sample()
        process_result = process_sample()

        assert fetch_result != process_result
        assert "Fetching" in fetch_result
        assert "Processing" in process_result

    def test_tasks_accept_airflow_context(self):
        """Test that tasks accept Airflow context dict."""
        mock_context = {
            "task_id": "test_task",
            "run_id": "test_run",
            "dag_id": "test_dag",
            "execution_date": "2025-01-01",
        }

        fetch_result = fetch_sample(context=mock_context)
        assert fetch_result is not None

        process_result = process_sample(context=mock_context)
        assert process_result is not None

    def test_tasks_with_mock_context_object(self):
        """Test that tasks accept mock context objects."""
        mock_context = MagicMock()
        mock_context.task_id = "test_task"
        mock_context.run_id = "test_run"

        # Should not raise exceptions
        fetch_result = fetch_sample(context=mock_context)
        assert fetch_result is not None

        process_result = process_sample(context=mock_context)
        assert process_result is not None
