"""
Basic usage example for BatchSQSBroker.

This example shows how to set up and use BatchSQSBroker for simple task processing.
"""

import dramatiq
from batch_sqs_broker import BatchSQSBroker

# Create the broker with basic configuration
broker = BatchSQSBroker(
    namespace="my-app-",  # Prefix for SQS queue names
    default_batch_interval=1.0,  # Wait up to 1 second before sending batch
    default_idle_timeout=0.1,  # Send after 100ms of no new messages
    max_buffer_size_per_queue=1000,  # Prevent memory issues
    max_retry_attempts=3,  # Retry failed messages up to 3 times
)

# Set as the default broker for Dramatiq
dramatiq.set_broker(broker)


# Define some example tasks
@dramatiq.actor
def send_email(user_id: int, subject: str, body: str):
    """Example task: send an email to a user."""
    print(f"Sending email to user {user_id}: '{subject}'")
    # Your email sending logic here
    return f"Email sent to user {user_id}"


@dramatiq.actor
def process_order(order_id: int, customer_id: int):
    """Example task: process a customer order."""
    print(f"Processing order {order_id} for customer {customer_id}")
    # Your order processing logic here
    return f"Order {order_id} processed"


@dramatiq.actor
def generate_report(report_type: str, date_range: str):
    """Example task: generate a report."""
    print(f"Generating {report_type} report for {date_range}")
    # Your report generation logic here
    return f"{report_type} report generated"


def main():
    """Example of enqueueing tasks."""
    print("BatchSQSBroker Basic Usage Example")
    print("=" * 40)
    
    # Enqueue some email tasks
    print("\n1. Enqueueing email tasks...")
    for i in range(5):
        send_email.send(
            user_id=i + 1,
            subject=f"Welcome User {i + 1}!",
            body=f"Welcome to our service, User {i + 1}!"
        )
    
    # Enqueue some order processing tasks
    print("2. Enqueueing order processing tasks...")
    for i in range(3):
        process_order.send(
            order_id=1000 + i,
            customer_id=500 + i
        )
    
    # Enqueue report generation tasks
    print("3. Enqueueing report generation tasks...")
    generate_report.send("sales", "2025-01-01 to 2025-01-31")
    generate_report.send("inventory", "2025-01-01 to 2025-01-31")
    
    print("\n4. Checking broker metrics...")
    metrics = broker.get_metrics()
    print(f"Current metrics: {metrics}")
    
    # Get status for specific queues
    for queue_name in ["default"]:  # dramatiq uses 'default' queue by default
        print(f"\n5. Status for queue '{queue_name}':")
        status = broker.get_queue_status(queue_name)
        for key, value in status.items():
            print(f"   {key}: {value}")
    
    print("\n6. Manual flush example...")
    broker.flush_all()
    print("All queues flushed!")
    
    # Clean shutdown
    print("\n7. Cleaning up...")
    broker.close()
    print("Broker closed gracefully.")


if __name__ == "__main__":
    # Note: This example assumes you have AWS credentials configured
    # and the necessary SQS queues exist in your AWS account.
    
    print("WARNING: This example requires AWS SQS access.")
    print("Make sure you have:")
    print("1. AWS credentials configured (AWS CLI, environment variables, or IAM role)")
    print("2. Necessary SQS queues created in your AWS account")
    print("3. Proper IAM permissions for SQS operations")
    print()
    
    # Uncomment the line below to run the example
    # main()
    
    print("Example setup complete. Uncomment main() call to run.")