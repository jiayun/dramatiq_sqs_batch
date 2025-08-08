"""
Basic tests for BatchSQSBroker functionality.

These tests focus on the core batching logic and don't require actual AWS SQS connections.
"""
import time
import unittest.mock
from unittest.mock import MagicMock, patch

import pytest
from dramatiq import Message

from batch_sqs_broker import BatchSQSBroker, FailedMessage


class TestBatchSQSBroker:
    """Test suite for BatchSQSBroker core functionality."""

    def test_broker_initialization(self):
        """Test broker initializes with correct default values."""
        broker = BatchSQSBroker(namespace="test-")
        
        assert broker.default_batch_interval == 1.0
        assert broker.default_idle_timeout == 0.1
        assert broker.batch_size == 10
        assert broker.max_buffer_size_per_queue == 5000
        assert broker.max_retry_attempts == 3
        assert broker._running is True
        assert isinstance(broker.buffer, dict)
        assert isinstance(broker.failed_messages, dict)

    def test_broker_initialization_with_custom_values(self):
        """Test broker initializes with custom configuration values."""
        broker = BatchSQSBroker(
            namespace="custom-",
            default_batch_interval=2.0,
            default_idle_timeout=0.5,
            batch_size=5,
            max_buffer_size_per_queue=1000,
            max_retry_attempts=5,
            group_batch_intervals={"high": 0, "low": 10.0},
            group_idle_timeouts={"high": 0, "low": 1.0},
        )
        
        assert broker.default_batch_interval == 2.0
        assert broker.default_idle_timeout == 0.5
        assert broker.batch_size == 5
        assert broker.max_buffer_size_per_queue == 1000
        assert broker.max_retry_attempts == 5
        assert broker.group_batch_intervals == {"high": 0, "low": 10.0}
        assert broker.group_idle_timeouts == {"high": 0, "low": 1.0}

    def test_batch_size_limited_to_10(self):
        """Test that batch_size is limited to SQS maximum of 10."""
        broker = BatchSQSBroker(batch_size=20)
        assert broker.batch_size == 10

    def test_check_message_size(self):
        """Test message size validation."""
        broker = BatchSQSBroker()
        
        # Small message (should pass)
        small_entry = {"Id": "test", "MessageBody": "x" * 1000}
        assert broker._check_message_size(small_entry) is True
        
        # Large message (should fail)
        large_entry = {"Id": "test", "MessageBody": "x" * (256 * 1024 + 1)}
        assert broker._check_message_size(large_entry) is False

    def test_failed_message_dataclass(self):
        """Test FailedMessage dataclass functionality."""
        entry = {"Id": "test", "MessageBody": "test"}
        failed_msg = FailedMessage(entry=entry)
        
        assert failed_msg.entry == entry
        assert failed_msg.retry_count == 0
        assert isinstance(failed_msg.first_failure_time, float)
        assert isinstance(failed_msg.last_failure_time, float)

    def test_split_oversized_batch_basic(self):
        """Test basic batch splitting functionality."""
        broker = BatchSQSBroker()
        
        # Create entries that together exceed 256KB
        entries = []
        for i in range(15):  # 15 messages, each ~20KB
            message_body = "x" * (20 * 1024)
            entries.append({"Id": f"msg-{i}", "MessageBody": message_body})
        
        sendable_batches, oversized_messages = broker._split_oversized_batch(entries, "test_queue")
        
        # Should split into multiple batches
        assert len(sendable_batches) > 1
        assert len(oversized_messages) == 0  # No individual oversized messages
        
        # Each batch should respect limits
        for batch in sendable_batches:
            assert len(batch) <= 10  # Message count limit
            total_size = sum(len(entry["MessageBody"].encode("utf-8")) for entry in batch)
            assert total_size <= 256 * 1024  # Size limit

    def test_split_oversized_batch_with_huge_messages(self):
        """Test batch splitting with messages that individually exceed 256KB."""
        broker = BatchSQSBroker()
        
        entries = [
            {"Id": "normal", "MessageBody": "x" * 1000},  # Normal message
            {"Id": "huge", "MessageBody": "x" * (256 * 1024 + 1)},  # Oversized message
        ]
        
        sendable_batches, oversized_messages = broker._split_oversized_batch(entries, "test_queue")
        
        assert len(sendable_batches) == 1
        assert len(sendable_batches[0]) == 1
        assert sendable_batches[0][0]["Id"] == "normal"
        
        assert len(oversized_messages) == 1
        assert oversized_messages[0]["Id"] == "huge"

    @patch('batch_sqs_broker.broker.get_logger')
    def test_get_metrics(self, mock_logger):
        """Test metrics collection functionality."""
        broker = BatchSQSBroker()
        
        # Add some test data
        broker.buffer["test_queue"] = [{"Id": "1", "MessageBody": "test"}]
        broker.failed_messages["test_queue"] = [
            FailedMessage(entry={"Id": "2", "MessageBody": "failed"})
        ]
        broker.metrics["messages_sent"]["test_queue"] = 10
        broker.metrics["messages_failed"]["test_queue"] = 2
        
        metrics = broker.get_metrics()
        
        assert metrics["buffer_sizes"]["test_queue"] == 1
        assert metrics["failed_message_counts"]["test_queue"] == 1
        assert metrics["metrics"]["messages_sent"]["test_queue"] == 10
        assert metrics["metrics"]["messages_failed"]["test_queue"] == 2
        assert metrics["max_buffer_size_per_queue"] == 5000
        assert metrics["max_retry_attempts"] == 3

    @patch('batch_sqs_broker.broker.get_logger')
    def test_get_queue_status(self, mock_logger):
        """Test queue status functionality."""
        broker = BatchSQSBroker(
            group_batch_intervals={"test_queue": 2.0},
            group_idle_timeouts={"test_queue": 0.5}
        )
        
        # Add test data
        broker.buffer["test_queue"] = [{"Id": "1", "MessageBody": "test"}]
        broker.failed_messages["test_queue"] = [
            FailedMessage(entry={"Id": "2", "MessageBody": "failed"})
        ]
        
        status = broker.get_queue_status("test_queue")
        
        assert status["queue_name"] == "test_queue"
        assert status["buffer_size"] == 1
        assert status["failed_message_count"] == 1
        assert status["batch_interval"] == 2.0
        assert status["idle_timeout"] == 0.5

    @patch('batch_sqs_broker.broker.get_logger')
    def test_clear_queue_buffer(self, mock_logger):
        """Test queue buffer clearing functionality."""
        broker = BatchSQSBroker()
        
        # Add test data
        broker.buffer["test_queue"] = [
            {"Id": "1", "MessageBody": "test1"},
            {"Id": "2", "MessageBody": "test2"}
        ]
        broker.failed_messages["test_queue"] = [
            FailedMessage(entry={"Id": "3", "MessageBody": "failed"})
        ]
        
        cleared_count = broker.clear_queue_buffer("test_queue")
        
        assert cleared_count == 3  # 2 buffered + 1 failed
        assert len(broker.buffer["test_queue"]) == 0
        assert len(broker.failed_messages["test_queue"]) == 0

    @patch('batch_sqs_broker.broker.get_logger')
    def test_close_broker(self, mock_logger):
        """Test broker cleanup functionality."""
        broker = BatchSQSBroker()
        
        # Mock the background thread
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        broker._background_thread = mock_thread
        
        # Mock flush_all to avoid SQS calls
        broker.flush_all = MagicMock()
        
        broker.close()
        
        assert broker._running is False
        mock_thread.join.assert_called_once_with(timeout=5.0)
        broker.flush_all.assert_called_once()


class TestFailedMessageRetry:
    """Test suite for failed message retry logic."""

    @patch('batch_sqs_broker.broker.get_logger')
    def test_retry_logic_with_exponential_backoff(self, mock_logger):
        """Test retry logic respects exponential backoff timing."""
        broker = BatchSQSBroker(max_retry_attempts=3)
        
        # Create a failed message
        entry = {"Id": "test", "MessageBody": "test"}
        failed_msg = FailedMessage(entry=entry)
        failed_msg.retry_count = 1
        failed_msg.last_failure_time = time.time() - 3  # 3 seconds ago
        
        broker.failed_messages["test_queue"] = [failed_msg]
        broker.buffer["test_queue"] = []
        
        # Should retry because enough time has passed (2^1 = 2 seconds)
        broker._retry_failed_messages("test_queue")
        
        assert len(broker.buffer["test_queue"]) == 1
        assert len(broker.failed_messages["test_queue"]) == 0

    @patch('batch_sqs_broker.broker.get_logger')
    def test_retry_logic_respects_max_attempts(self, mock_logger):
        """Test retry logic drops messages after max attempts."""
        broker = BatchSQSBroker(max_retry_attempts=2)
        
        # Create a failed message that has exceeded retry limit
        entry = {"Id": "test", "MessageBody": "test"}
        failed_msg = FailedMessage(entry=entry)
        failed_msg.retry_count = 2  # At max retry attempts
        
        broker.failed_messages["test_queue"] = [failed_msg]
        broker.buffer["test_queue"] = []
        
        broker._retry_failed_messages("test_queue")
        
        # Message should be dropped
        assert len(broker.buffer["test_queue"]) == 0
        assert len(broker.failed_messages["test_queue"]) == 0
        assert broker.metrics["retry_exhausted_count"]["test_queue"] == 1

    @patch('batch_sqs_broker.broker.get_logger')
    def test_retry_logic_waits_for_backoff_time(self, mock_logger):
        """Test retry logic waits for proper backoff time."""
        broker = BatchSQSBroker()
        
        # Create a failed message that hasn't waited long enough
        entry = {"Id": "test", "MessageBody": "test"}
        failed_msg = FailedMessage(entry=entry)
        failed_msg.retry_count = 2
        failed_msg.last_failure_time = time.time() - 2  # Only 2 seconds ago, need 2^2=4
        
        broker.failed_messages["test_queue"] = [failed_msg]
        broker.buffer["test_queue"] = []
        
        broker._retry_failed_messages("test_queue")
        
        # Should not retry yet
        assert len(broker.buffer["test_queue"]) == 0
        assert len(broker.failed_messages["test_queue"]) == 1  # Still in failed queue


if __name__ == "__main__":
    pytest.main([__file__])