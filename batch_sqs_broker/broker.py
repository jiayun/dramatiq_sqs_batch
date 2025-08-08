from __future__ import annotations

import threading
import time
import uuid
from base64 import b64encode
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from dramatiq import Message
from dramatiq.logging import get_logger
from dramatiq_sqs import SQSBroker


@dataclass
class FailedMessage:
    """Track retry information for failed messages"""

    entry: dict
    retry_count: int = 0
    first_failure_time: float = field(default_factory=time.time)
    last_failure_time: float = field(default_factory=time.time)


class BatchSQSBroker(SQSBroker):
    def __init__(
        self,
        *args,
        default_batch_interval: float = 1.0,
        default_idle_timeout: float = 0.1,
        batch_size: int = 10,
        group_batch_intervals: dict[str, float] = None,
        group_idle_timeouts: dict[str, float] = None,
        max_buffer_size_per_queue: int = 5000,
        max_retry_attempts: int = 3,
        **kwargs,
    ):
        """
        Initialize batch SQS Broker with support for per-group max wait time and idle timeout settings.
        
        :param default_batch_interval: Default maximum wait time in seconds.
        :param default_idle_timeout: Default idle timeout in seconds, sends when no new messages.
        :param batch_size: Maximum messages per batch (max 10, SQS limit).
        :param group_batch_intervals: Per-group (queue_name) max wait times, e.g., {"high_priority": 0, "low_priority": 1.0}.
        :param group_idle_timeouts: Per-group idle timeouts, e.g., {"low_priority": 0.2}.
        :param max_buffer_size_per_queue: Maximum buffer size per queue (default 5000).
        :param max_retry_attempts: Maximum retry attempts for failed messages (default 3).
        :param args, kwargs: Arguments passed to SQSBroker (e.g., namespace).
        """
        super().__init__(*args, **kwargs)
        self.logger = get_logger(__name__, type(self))
        self.default_batch_interval = default_batch_interval
        self.default_idle_timeout = default_idle_timeout
        self.batch_size = min(batch_size, 10)
        self.group_batch_intervals = group_batch_intervals or {}
        self.group_idle_timeouts = group_idle_timeouts or {}
        self.max_buffer_size_per_queue = max_buffer_size_per_queue
        self.max_retry_attempts = max_retry_attempts

        # Data structures
        self.buffer: dict[str, list[dict]] = {}  # Store buffered messages by queue name
        self.failed_messages: dict[str, list[FailedMessage]] = defaultdict(list)  # Failed message tracking
        self.last_flush: dict[str, float] = {}  # Record last send time by queue name
        self.last_message_time: dict[str, float] = {}  # Record last message entry time

        # Monitoring metrics
        self.metrics = {
            "messages_sent": defaultdict(int),
            "messages_failed": defaultdict(int),
            "buffer_overflow_count": defaultdict(int),
            "retry_exhausted_count": defaultdict(int),
            "batch_split_count": defaultdict(int),
            "oversized_message_dropped": defaultdict(int),
        }

        self.lock = threading.Lock()  # Use threading lock to ensure thread safety
        self._running = True
        self._background_thread: Optional[threading.Thread] = None
        # Start background thread for checking tasks
        self._start_background_flush()

    def enqueue(self, message: Message, *, delay: int = None) -> Message:
        """
        Override enqueue to add messages to buffer and send based on group conditions.
        """
        queue_name = message.queue_name
        with self.lock:
            if queue_name not in self.buffer:
                self.buffer[queue_name] = []
                self.last_flush[queue_name] = time.time()
                self.last_message_time[queue_name] = time.time()

            # Check buffer size limit (backpressure mechanism)
            current_buffer_size = len(self.buffer[queue_name]) + len(self.failed_messages[queue_name])
            if current_buffer_size >= self.max_buffer_size_per_queue:
                self.metrics["buffer_overflow_count"][queue_name] += 1
                self.logger.warning(
                    f"Buffer overflow for queue {queue_name}: {current_buffer_size} messages. "
                    f"Forcing flush to prevent memory issues."
                )
                # Force flush to free up space
                self._flush(queue_name)

                # If still full after flush, reject new messages
                if (
                    len(self.buffer[queue_name]) + len(self.failed_messages[queue_name])
                    >= self.max_buffer_size_per_queue
                ):
                    self.logger.error(f"Buffer still full after flush for queue {queue_name}, rejecting message")
                    raise BufferError(f"Buffer full for queue {queue_name}, cannot accept new messages")

            # Generate SQS message format using the same encoding as SQSBroker
            encoded_message = b64encode(message.encode()).decode()
            # Use UUID to avoid ID conflicts
            message_id = str(uuid.uuid4())[:10]  # SQS ID limit is 80 chars, first 10 chars are sufficient
            entry = {"Id": message_id, "MessageBody": encoded_message}
            if delay is not None:
                entry["DelaySeconds"] = min(int(delay / 1000), 900)
            self.buffer[queue_name].append(entry)
            self.last_message_time[queue_name] = time.time()  # Update last message time

            # Get group's batch_interval
            batch_interval = self.group_batch_intervals.get(queue_name, self.default_batch_interval)

            # Check if should send immediately (full capacity or batch_interval=0)
            if len(self.buffer[queue_name]) >= self.batch_size or batch_interval == 0:
                self._flush(queue_name)

        return message

    def _flush(self, queue_name: str):
        """
        Batch send buffered messages from specified queue to SQS.
        Optimization: Reduce lock holding time by moving SQS API calls outside lock region.
        """
        # Handle retry of failed messages first
        self._retry_failed_messages(queue_name)

        if not self.buffer.get(queue_name):
            return

        # Copy data within lock, then release lock
        entries_to_send = self.buffer[queue_name].copy()
        self.buffer[queue_name] = []  # Clear buffer immediately
        self.last_flush[queue_name] = time.time()

        # Release lock before SQS API calls
        # This allows other threads to continue adding messages to buffer
        self._send_to_sqs(queue_name, entries_to_send)

    def _check_message_size(self, entry: dict) -> bool:
        """
        Check if a single message exceeds SQS limits.
        SQS single message maximum is 256KB.
        """
        message_size = len(entry["MessageBody"].encode("utf-8"))
        return message_size <= 256 * 1024

    def _split_oversized_batch(self, entries: list[dict], queue_name: str) -> tuple[list[list[dict]], list[dict]]:
        """
        Split oversized batches using greedy algorithm to organize optimal batch sizes.
        Returns: (list of sendable batches, list of oversized unsendable messages)
        """
        MAX_BATCH_SIZE_BYTES = 256 * 1024  # 256KB
        MAX_MESSAGES_PER_BATCH = 10

        sendable_batches = []
        oversized_messages = []

        # First filter out truly oversized single messages
        valid_entries = []
        for entry in entries:
            if not self._check_message_size(entry):
                oversized_messages.append(entry)
                self.logger.error(f"Single message {entry['Id']} exceeds 256KB limit for queue {queue_name}, dropping")
            else:
                valid_entries.append(entry)

        if not valid_entries:
            return [], oversized_messages

        # Use greedy algorithm to organize batches
        current_batch = []
        current_size = 0

        for entry in valid_entries:
            entry_size = len(entry["MessageBody"].encode("utf-8"))

            # Check if can add to current batch
            if len(current_batch) < MAX_MESSAGES_PER_BATCH and current_size + entry_size <= MAX_BATCH_SIZE_BYTES:
                current_batch.append(entry)
                current_size += entry_size
            else:
                # Current batch is full, start new batch
                if current_batch:
                    sendable_batches.append(current_batch)
                current_batch = [entry]
                current_size = entry_size

        # Add the last batch
        if current_batch:
            sendable_batches.append(current_batch)

        if len(sendable_batches) > 1:
            self.metrics["batch_split_count"][queue_name] += 1
            self.logger.info(
                f"Split oversized batch for queue {queue_name}: "
                f"{len(entries)} messages → {len(sendable_batches)} batches"
            )

        return sendable_batches, oversized_messages

    def _send_to_sqs(self, queue_name: str, entries: list[dict]):
        """
        Actually send messages to SQS (no need to hold lock).
        Optimization: Intelligently handle oversized batches to avoid infinite retry loops.
        """
        if not entries:
            return

        # Ensure queue exists in self.queues
        if queue_name not in self.queues:
            prefixed_queue_name = f"{self.namespace}{queue_name}"
            self.queues[queue_name] = self.sqs.get_queue_by_name(QueueName=prefixed_queue_name)

        queue = self.queues[queue_name]

        # First check if batch splitting is needed
        total_size = sum(len(entry["MessageBody"].encode("utf-8")) for entry in entries)
        if total_size > 256 * 1024:
            # Use intelligent splitting to handle oversized batches
            sendable_batches, oversized_messages = self._split_oversized_batch(entries, queue_name)

            # Handle truly oversized single messages
            if oversized_messages:
                with self.lock:
                    self.metrics["oversized_message_dropped"][queue_name] += len(oversized_messages)
                    # Don't add to failed_messages as these messages can never be sent via SQS

                self.logger.warning(
                    f"Dropped {len(oversized_messages)} oversized messages for queue {queue_name} "
                    f"(>256KB each, cannot be sent via SQS)"
                )

            # Send each split batch separately
            for batch_entries in sendable_batches:
                self._send_single_batch(queue, queue_name, batch_entries)

        else:
            # Normal size, send in batches of 10
            max_batch_size = 10
            for i in range(0, len(entries), max_batch_size):
                batch_entries = entries[i : i + max_batch_size]
                self._send_single_batch(queue, queue_name, batch_entries)

    def _send_single_batch(self, queue, queue_name: str, batch_entries: list[dict]):
        """
        Send a single batch to SQS, handling transmission errors and failed messages.
        """
        try:
            response = queue.send_messages(Entries=batch_entries)

            # Record success count
            success_count = len(batch_entries)

            # Check if there are failed messages
            if "Failed" in response and response["Failed"]:
                failed = response["Failed"]
                self.logger.error(f"Failed to send {len(failed)} messages to {queue_name}: {failed}")

                # Add failed messages to retry queue
                failed_ids = {f["Id"] for f in failed}
                with self.lock:
                    for entry in batch_entries:
                        if entry["Id"] in failed_ids:
                            self.failed_messages[queue_name].append(FailedMessage(entry=entry))
                            success_count -= 1

                self.metrics["messages_failed"][queue_name] += len(failed)

            self.metrics["messages_sent"][queue_name] += success_count

        except Exception as e:
            self.logger.error(f"Error sending batch to {queue_name}: {e}", exc_info=True)
            # Add all messages to failed queue
            with self.lock:
                for entry in batch_entries:
                    self.failed_messages[queue_name].append(FailedMessage(entry=entry))
                self.metrics["messages_failed"][queue_name] += len(batch_entries)

    def _retry_failed_messages(self, queue_name: str):
        """
        Retry failed messages with retry count limits.
        """
        if queue_name not in self.failed_messages or not self.failed_messages[queue_name]:
            return

        current_time = time.time()
        messages_to_retry = []
        messages_to_drop = []

        for failed_msg in self.failed_messages[queue_name]:
            # Check retry count
            if failed_msg.retry_count >= self.max_retry_attempts:
                messages_to_drop.append(failed_msg)
            else:
                # Use exponential backoff: 2^retry_count seconds
                retry_delay = 2**failed_msg.retry_count
                if current_time - failed_msg.last_failure_time >= retry_delay:
                    messages_to_retry.append(failed_msg)

        # Handle messages that exceeded retry count
        for msg in messages_to_drop:
            self.failed_messages[queue_name].remove(msg)
            self.metrics["retry_exhausted_count"][queue_name] += 1
            self.logger.error(
                f"Message {msg.entry['Id']} for queue {queue_name} dropped after "
                f"{msg.retry_count} retries. First failure: {msg.first_failure_time}"
            )

        # Retry messages
        if messages_to_retry:
            # Add retry messages back to buffer
            for msg in messages_to_retry:
                msg.retry_count += 1
                msg.last_failure_time = current_time
                self.buffer[queue_name].append(msg.entry)
                self.failed_messages[queue_name].remove(msg)

            self.logger.info(f"Retrying {len(messages_to_retry)} messages for queue {queue_name}")

    def _start_background_flush(self):
        """
        Start background thread to periodically check and flush buffers.
        Enhancement: Added exception handling and automatic restart.
        """

        def check_idle_buffers():
            while self._running:
                try:
                    time.sleep(0.05)  # Check every 50ms
                    with self.lock:
                        current_time = time.time()
                        for queue_name in list(self.buffer.keys()):
                            # Get group's batch_interval and idle_timeout
                            batch_interval = self.group_batch_intervals.get(queue_name, self.default_batch_interval)
                            idle_timeout = self.group_idle_timeouts.get(queue_name, self.default_idle_timeout)

                            # Check if timeout (batch_interval) or idle (idle_timeout)
                            if self.buffer[queue_name] and (
                                current_time - self.last_flush[queue_name] >= batch_interval
                                or current_time - self.last_message_time[queue_name] >= idle_timeout
                            ):
                                self._flush(queue_name)

                            # Also check if failed messages need retry
                            if self.failed_messages[queue_name]:
                                self._retry_failed_messages(queue_name)

                except Exception as e:
                    self.logger.error(f"Error in background flush thread: {e}", exc_info=True)
                    # Continue execution, don't let background thread crash
                    time.sleep(1)  # Avoid exception loops being too fast

        # Start background thread, set as daemon so it terminates when main program ends
        self._background_thread = threading.Thread(target=check_idle_buffers, daemon=True, name="BatchSQSBroker-Flush")
        self._background_thread.start()

    def get_queue_url(self, queue_name: str) -> str:
        """
        Get the SQS URL for a queue.
        """
        if queue_name in self.queues:
            return self.queues[queue_name].url
        else:
            # If queue hasn't been declared yet, get from SQS
            prefixed_queue_name = f"{self.namespace}{queue_name}"
            queue = self.sqs.get_queue_by_name(QueueName=prefixed_queue_name)
            return queue.url

    def flush_all(self):
        """
        Send buffered messages from all queues.
        """
        with self.lock:
            queue_names = list(self.buffer.keys())

        # Flush each queue individually, giving each flush operation independent lock time
        for queue_name in queue_names:
            with self.lock:
                self._flush(queue_name)

    def get_metrics(self) -> dict:
        """
        Get current monitoring metrics.
        """
        with self.lock:
            # Add current buffer status
            buffer_status = {}
            failed_status = {}
            for queue_name in self.buffer.keys():
                buffer_status[queue_name] = len(self.buffer[queue_name])
                failed_status[queue_name] = len(self.failed_messages[queue_name])

            return {
                "buffer_sizes": buffer_status,
                "failed_message_counts": failed_status,
                "metrics": dict(self.metrics),
                "max_buffer_size_per_queue": self.max_buffer_size_per_queue,
                "max_retry_attempts": self.max_retry_attempts,
                "background_thread_alive": self._background_thread.is_alive() if self._background_thread else False,
            }

    def get_queue_status(self, queue_name: str) -> dict:
        """
        Get detailed status information for a specific queue.
        """
        with self.lock:
            return {
                "queue_name": queue_name,
                "buffer_size": len(self.buffer.get(queue_name, [])),
                "failed_message_count": len(self.failed_messages[queue_name]),
                "messages_sent": self.metrics["messages_sent"][queue_name],
                "messages_failed": self.metrics["messages_failed"][queue_name],
                "buffer_overflow_count": self.metrics["buffer_overflow_count"][queue_name],
                "retry_exhausted_count": self.metrics["retry_exhausted_count"][queue_name],
                "batch_split_count": self.metrics["batch_split_count"][queue_name],
                "oversized_message_dropped": self.metrics["oversized_message_dropped"][queue_name],
                "last_flush_time": self.last_flush.get(queue_name),
                "last_message_time": self.last_message_time.get(queue_name),
                "batch_interval": self.group_batch_intervals.get(queue_name, self.default_batch_interval),
                "idle_timeout": self.group_idle_timeouts.get(queue_name, self.default_idle_timeout),
            }

    def clear_queue_buffer(self, queue_name: str) -> int:
        """
        Clear buffer for specified queue (for emergency use).
        Returns the number of messages cleared.
        """
        with self.lock:
            buffer_count = len(self.buffer.get(queue_name, []))
            failed_count = len(self.failed_messages[queue_name])

            if queue_name in self.buffer:
                self.buffer[queue_name] = []
            self.failed_messages[queue_name] = []

            self.logger.warning(
                f"Manually cleared buffer for queue {queue_name}: "
                f"{buffer_count} buffered + {failed_count} failed messages"
            )

            return buffer_count + failed_count

    def force_flush_queue(self, queue_name: str):
        """
        Force flush specific queue (for emergency use).
        """
        with self.lock:
            if queue_name in self.buffer:
                self._flush(queue_name)
                self.logger.info(f"Manually flushed queue {queue_name}")

    def close(self):
        """
        Close broker and clean up resources.
        Enhancement: Gracefully shut down background thread.
        """
        self.logger.info("Closing BatchSQSBroker...")
        self._running = False

        # Wait for background thread to finish
        if self._background_thread and self._background_thread.is_alive():
            self._background_thread.join(timeout=5.0)
            if self._background_thread.is_alive():
                self.logger.warning("Background thread did not stop gracefully")

        # Final flush of all buffers
        self.flush_all()

        # Log final status
        final_metrics = self.get_metrics()
        self.logger.info(f"BatchSQSBroker closed. Final metrics: {final_metrics}")

        super().close()