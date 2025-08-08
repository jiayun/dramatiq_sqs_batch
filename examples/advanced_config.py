"""
Advanced configuration example for BatchSQSBroker.

This example demonstrates advanced features like per-queue configuration,
monitoring, error handling, and performance optimization.
"""

import time
import dramatiq
from batch_sqs_broker import BatchSQSBroker

# Create broker with advanced per-queue configuration
broker = BatchSQSBroker(
    namespace="advanced-app-",
    
    # Default settings for all queues
    default_batch_interval=2.0,
    default_idle_timeout=0.5,
    
    # Per-queue batch intervals (max wait time)
    group_batch_intervals={
        "high_priority": 0,      # Send immediately (no batching delay)
        "normal": 1.0,           # Wait up to 1 second
        "low_priority": 5.0,     # Wait up to 5 seconds for better batching
        "bulk_processing": 10.0, # Wait up to 10 seconds for maximum batching
    },
    
    # Per-queue idle timeouts (send after idle time)
    group_idle_timeouts={
        "high_priority": 0,      # No idle timeout (immediate send)
        "normal": 0.2,           # Send after 200ms idle
        "low_priority": 1.0,     # Send after 1 second idle
        "bulk_processing": 2.0,  # Send after 2 seconds idle
    },
    
    # Memory and retry configuration
    max_buffer_size_per_queue=2000,
    max_retry_attempts=5,  # More retries for production systems
)

dramatiq.set_broker(broker)


# Define tasks with different queue priorities
@dramatiq.actor(queue_name="high_priority")
def critical_alert(alert_type: str, message: str, recipient: str):
    """Critical alerts that need immediate processing."""
    print(f"🚨 CRITICAL ALERT: {alert_type} - {message} (to: {recipient})")
    return f"Alert sent: {alert_type}"


@dramatiq.actor(queue_name="normal")
def user_notification(user_id: int, notification_type: str, content: str):
    """Normal user notifications."""
    print(f"📨 Notification to user {user_id}: {notification_type} - {content}")
    return f"Notification sent to {user_id}"


@dramatiq.actor(queue_name="low_priority")
def analytics_event(event_type: str, user_id: int, properties: dict):
    """Analytics events that can be batched efficiently."""
    print(f"📊 Analytics: {event_type} for user {user_id} - {properties}")
    return f"Event tracked: {event_type}"


@dramatiq.actor(queue_name="bulk_processing")
def data_export(export_type: str, record_count: int, format: str):
    """Large data processing tasks that benefit from batching."""
    print(f"📁 Exporting {record_count} {export_type} records as {format}")
    return f"Export completed: {export_type}"


def demonstrate_monitoring():
    """Demonstrate comprehensive monitoring capabilities."""
    print("\n" + "=" * 50)
    print("MONITORING DEMONSTRATION")
    print("=" * 50)
    
    # Enqueue various tasks
    print("1. Enqueueing tasks across different priority queues...")
    
    # High priority tasks
    critical_alert.send("system_down", "Database connection lost", "admin@company.com")
    critical_alert.send("security", "Multiple failed login attempts", "security@company.com")
    
    # Normal priority tasks
    for i in range(10):
        user_notification.send(
            user_id=i + 1,
            notification_type="order_update",
            content=f"Your order #{1000 + i} has been shipped!"
        )
    
    # Low priority analytics events
    for i in range(20):
        analytics_event.send(
            event_type="page_view",
            user_id=i + 1,
            properties={"page": "/dashboard", "session_id": f"sess_{i}"}
        )
    
    # Bulk processing tasks
    for i in range(5):
        data_export.send(
            export_type="customer_data",
            record_count=10000 + i * 1000,
            format="CSV"
        )
    
    # Wait a moment for buffering
    time.sleep(0.5)
    
    print("\n2. Overall broker metrics:")
    metrics = broker.get_metrics()
    print(f"   Buffer sizes: {metrics['buffer_sizes']}")
    print(f"   Failed message counts: {metrics['failed_message_counts']}")
    print(f"   Messages sent: {dict(metrics['metrics']['messages_sent'])}")
    print(f"   Messages failed: {dict(metrics['metrics']['messages_failed'])}")
    print(f"   Background thread alive: {metrics['background_thread_alive']}")
    
    print("\n3. Per-queue detailed status:")
    queues = ["high_priority", "normal", "low_priority", "bulk_processing"]
    
    for queue_name in queues:
        print(f"\n   📋 Queue: {queue_name}")
        status = broker.get_queue_status(queue_name)
        print(f"      Buffer size: {status['buffer_size']}")
        print(f"      Failed messages: {status['failed_message_count']}")
        print(f"      Batch interval: {status['batch_interval']}s")
        print(f"      Idle timeout: {status['idle_timeout']}s")
        print(f"      Messages sent: {status['messages_sent']}")


def demonstrate_manual_control():
    """Demonstrate manual queue control features."""
    print("\n" + "=" * 50)
    print("MANUAL CONTROL DEMONSTRATION")
    print("=" * 50)
    
    # Add some test messages
    print("1. Adding test messages to queues...")
    for i in range(5):
        user_notification.send(
            user_id=100 + i,
            notification_type="test",
            content=f"Test message {i}"
        )
    
    time.sleep(0.1)  # Let them buffer
    
    print(f"2. Current buffer sizes before manual flush:")
    metrics = broker.get_metrics()
    print(f"   {metrics['buffer_sizes']}")
    
    # Force flush specific queue
    print("3. Force flushing 'normal' queue...")
    broker.force_flush_queue("normal")
    
    # Check status after flush
    print(f"4. Buffer sizes after manual flush:")
    metrics = broker.get_metrics()
    print(f"   {metrics['buffer_sizes']}")
    
    # Demonstrate buffer clearing (emergency use)
    print("5. Emergency buffer clear demonstration...")
    
    # Add more test messages
    for i in range(3):
        analytics_event.send("emergency_test", i, {"test": True})
    
    time.sleep(0.1)
    cleared_count = broker.clear_queue_buffer("low_priority")
    print(f"   Cleared {cleared_count} messages from low_priority queue")


def demonstrate_error_handling():
    """Demonstrate error handling capabilities."""
    print("\n" + "=" * 50)
    print("ERROR HANDLING DEMONSTRATION")
    print("=" * 50)
    
    print("1. Testing buffer overflow protection...")
    
    # This would normally trigger buffer overflow protection
    # but we'll just show the concept
    original_limit = broker.max_buffer_size_per_queue
    print(f"   Current buffer limit: {original_limit} messages per queue")
    
    print("2. Monitoring metrics for error tracking...")
    metrics = broker.get_metrics()
    
    error_metrics = [
        "buffer_overflow_count",
        "retry_exhausted_count", 
        "batch_split_count",
        "oversized_message_dropped"
    ]
    
    for metric in error_metrics:
        values = dict(metrics["metrics"][metric])
        if values:
            print(f"   {metric}: {values}")
        else:
            print(f"   {metric}: No incidents")


def main():
    """Run the advanced configuration demonstration."""
    print("BatchSQSBroker Advanced Configuration Example")
    print("=" * 50)
    
    print(f"Broker configuration:")
    print(f"  Namespace: {broker.namespace}")
    print(f"  Default batch interval: {broker.default_batch_interval}s")
    print(f"  Default idle timeout: {broker.default_idle_timeout}s")
    print(f"  Max buffer size per queue: {broker.max_buffer_size_per_queue}")
    print(f"  Max retry attempts: {broker.max_retry_attempts}")
    print(f"  Per-queue batch intervals: {broker.group_batch_intervals}")
    print(f"  Per-queue idle timeouts: {broker.group_idle_timeouts}")
    
    try:
        # Run demonstrations
        demonstrate_monitoring()
        demonstrate_manual_control()
        demonstrate_error_handling()
        
        print("\n" + "=" * 50)
        print("FINAL CLEANUP")
        print("=" * 50)
        
        # Final flush and metrics
        print("1. Final flush of all queues...")
        broker.flush_all()
        
        print("2. Final metrics report:")
        final_metrics = broker.get_metrics()
        print(f"   Total buffer sizes: {sum(final_metrics['buffer_sizes'].values())}")
        print(f"   Total failed messages: {sum(final_metrics['failed_message_counts'].values())}")
        
        total_sent = sum(final_metrics['metrics']['messages_sent'].values())
        total_failed = sum(final_metrics['metrics']['messages_failed'].values())
        print(f"   Total messages sent: {total_sent}")
        print(f"   Total messages failed: {total_failed}")
        
        if total_sent + total_failed > 0:
            success_rate = (total_sent / (total_sent + total_failed)) * 100
            print(f"   Success rate: {success_rate:.1f}%")
        
    finally:
        # Always clean up
        print("\n3. Graceful shutdown...")
        broker.close()
        print("   Broker closed successfully.")


if __name__ == "__main__":
    print("WARNING: This example requires AWS SQS access.")
    print("Make sure you have:")
    print("1. AWS credentials configured")
    print("2. SQS queues with the following names in your account:")
    print("   - advanced-app-high_priority")
    print("   - advanced-app-normal")
    print("   - advanced-app-low_priority")
    print("   - advanced-app-bulk_processing")
    print("3. Proper IAM permissions for SQS operations")
    print()
    
    # Uncomment to run the example
    # main()
    
    print("Example setup complete. Uncomment main() call to run.")