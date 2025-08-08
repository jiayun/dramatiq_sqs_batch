# BatchSQSBroker

A high-performance batch processing broker for AWS SQS with intelligent message splitting, retry mechanisms, and comprehensive monitoring. Optimized for handling high-volume task queues with automatic batching, exponential backoff, and thread-safe operations.

## Features

- **Intelligent Batch Processing**: Automatically batches messages up to SQS limits (10 messages, 256KB)
- **Smart Message Splitting**: Handles oversized batches with greedy algorithm optimization
- **Retry Mechanisms**: Exponential backoff with configurable retry limits
- **Thread-Safe Operations**: Concurrent message processing with proper locking
- **Comprehensive Monitoring**: Built-in metrics for observability
- **Configurable Timeouts**: Per-queue batch intervals and idle timeouts
- **Memory Management**: Buffer overflow protection with backpressure
- **Python 3.8+ Support**: Compatible with modern Python versions

## Installation

```bash
pip install dramatiq_sqs_batch
```

Or with Poetry:

```bash
poetry add dramatiq_sqs_batch
```

## Quick Start

```python
from batch_sqs_broker import BatchSQSBroker
import dramatiq

# Create broker
broker = BatchSQSBroker(
    namespace="my-app-",
    default_batch_interval=1.0,  # Max 1 second wait
    default_idle_timeout=0.1,    # 100ms idle timeout
)

# Set as default broker
dramatiq.set_broker(broker)

# Define a task
@dramatiq.actor
def my_task(data):
    print(f"Processing: {data}")

# Enqueue messages
for i in range(100):
    my_task.send(f"message-{i}")
```

## Configuration

### Basic Configuration

```python
broker = BatchSQSBroker(
    namespace="myapp-",
    default_batch_interval=2.0,    # Wait up to 2 seconds
    default_idle_timeout=0.5,      # Send after 500ms of no new messages
    batch_size=10,                 # Max 10 messages per batch (SQS limit)
    max_buffer_size_per_queue=1000, # Memory protection
    max_retry_attempts=3,          # Retry failed messages 3 times
)
```

### Per-Queue Configuration

```python
broker = BatchSQSBroker(
    group_batch_intervals={
        "high_priority": 0,      # Send immediately
        "low_priority": 5.0,     # Wait up to 5 seconds
    },
    group_idle_timeouts={
        "high_priority": 0,      # No idle timeout
        "low_priority": 1.0,     # Send after 1s idle
    }
)
```

## Monitoring

```python
# Get overall metrics
metrics = broker.get_metrics()
print(metrics)

# Get specific queue status
status = broker.get_queue_status("my_queue")
print(status)
```

Example output:
```python
{
    "buffer_sizes": {"my_queue": 5},
    "failed_message_counts": {"my_queue": 0},
    "metrics": {
        "messages_sent": {"my_queue": 100},
        "messages_failed": {"my_queue": 2},
        "batch_split_count": {"my_queue": 1},
        "oversized_message_dropped": {"my_queue": 0}
    },
    "max_buffer_size_per_queue": 5000,
    "max_retry_attempts": 3,
    "background_thread_alive": True
}
```

## Advanced Features

### Manual Queue Management

```python
# Force flush a specific queue
broker.force_flush_queue("urgent_queue")

# Clear queue buffer (emergency use)
cleared_count = broker.clear_queue_buffer("problematic_queue")

# Flush all queues
broker.flush_all()
```

### Graceful Shutdown

```python
# Proper cleanup
broker.close()
```

## How It Solves SQS Challenges

BatchSQSBroker addresses common AWS SQS limitations:

1. **256KB Batch Limit**: Intelligently splits oversized batches using greedy algorithm
2. **10 Message Limit**: Automatically chunks large message sets
3. **Infinite Retry Loops**: Distinguishes between batch-size and message-size issues
4. **Memory Leaks**: Implements buffer size limits with backpressure
5. **Thread Safety**: Uses proper locking for concurrent access
6. **Performance**: Reduces SQS API calls through intelligent batching

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Application   │────│  BatchSQSBroker  │────│   AWS SQS       │
│                 │    │                  │    │                 │
│ dramatiq.send() │    │ • Buffering      │    │ • Batch API     │
│                 │    │ • Batching       │    │ • Message Queue │
│                 │    │ • Retry Logic    │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                      ┌──────────────────┐
                      │ Background Thread │
                      │ • Timeout Check   │
                      │ • Auto Flush      │
                      │ • Retry Failed    │
                      └──────────────────┘
```

## API Reference

### BatchSQSBroker

The main broker class that extends dramatiq's SQSBroker with batching capabilities.

#### Parameters

- `default_batch_interval` (float): Maximum wait time before sending batch (default: 1.0s)
- `default_idle_timeout` (float): Send batch after this idle time (default: 0.1s)
- `batch_size` (int): Maximum messages per batch, up to 10 (SQS limit)
- `group_batch_intervals` (dict): Per-queue batch intervals
- `group_idle_timeouts` (dict): Per-queue idle timeouts
- `max_buffer_size_per_queue` (int): Buffer size limit per queue (default: 5000)
- `max_retry_attempts` (int): Maximum retry attempts for failed messages (default: 3)

#### Methods

- `get_metrics()`: Returns comprehensive metrics dictionary
- `get_queue_status(queue_name)`: Returns detailed status for specific queue
- `flush_all()`: Immediately flush all queue buffers
- `force_flush_queue(queue_name)`: Force flush specific queue
- `clear_queue_buffer(queue_name)`: Clear buffer for emergency use
- `close()`: Gracefully shut down broker

### FailedMessage

Dataclass for tracking failed message retry information.

#### Attributes

- `entry` (dict): The SQS message entry
- `retry_count` (int): Current retry count
- `first_failure_time` (float): Timestamp of first failure
- `last_failure_time` (float): Timestamp of last failure

## Best Practices

1. **Choose appropriate batch intervals**: Balance between latency and throughput
2. **Monitor buffer sizes**: Watch for queue buffer overflows
3. **Set reasonable retry limits**: Avoid infinite retry loops
4. **Use per-queue configuration**: Different queues may need different settings
5. **Implement proper shutdown**: Always call `broker.close()` for graceful cleanup

## Error Handling

BatchSQSBroker includes robust error handling:

- **Oversized Messages**: Messages >256KB are dropped with warnings
- **Buffer Overflow**: Automatic backpressure prevents memory issues  
- **Network Errors**: Exponential backoff retry with limits
- **Thread Safety**: Proper locking prevents race conditions

## Performance Tips

- Use `default_idle_timeout` for low-latency requirements
- Set `default_batch_interval=0` for immediate sending on high-priority queues
- Monitor `batch_split_count` metric to optimize message sizes
- Adjust `max_buffer_size_per_queue` based on memory constraints

## Requirements

- Python 3.8+
- dramatiq >= 1.12.0
- dramatiq-sqs >= 0.2.0
- boto3 >= 1.20.0

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### v1.0.0
- Initial release
- Intelligent batch splitting with greedy algorithm
- Retry mechanisms with exponential backoff
- Comprehensive monitoring and metrics
- Thread-safe operations
- Per-queue configuration support
- Buffer overflow protection
