"""
FastAPI integration example with BatchSQSBroker.

This example demonstrates:
1. How to integrate BatchSQSBroker with FastAPI
2. Proper shutdown handling to prevent message loss
3. Gunicorn configuration for production deployment
4. API endpoints that trigger background tasks
"""

import os
import sys
from typing import Optional

import dramatiq
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from batch_sqs_broker import BatchSQSBroker

# -----------------------------------------------------------------------------
# Broker Setup
# -----------------------------------------------------------------------------

# Create and configure the broker
broker = BatchSQSBroker(
    namespace="fastapi-app-",
    default_batch_interval=1.0,  # Wait up to 1 second
    default_idle_timeout=0.1,    # Send after 100ms idle
    max_buffer_size_per_queue=5000,
    max_retry_attempts=3,
    # High priority queues send immediately
    group_batch_intervals={
        "high_priority": 0,
        "default": 1.0,
        "low_priority": 5.0,
    },
    group_idle_timeouts={
        "high_priority": 0,
        "default": 0.1,
        "low_priority": 1.0,
    }
)

# Set as default broker
dramatiq.set_broker(broker)

# -----------------------------------------------------------------------------
# Dramatiq Tasks
# -----------------------------------------------------------------------------

@dramatiq.actor(queue_name="high_priority")
def send_notification(user_id: int, message: str):
    """High priority notification task."""
    print(f"[HIGH PRIORITY] Sending notification to user {user_id}: {message}")
    # Implement actual notification logic here
    return f"Notification sent to user {user_id}"


@dramatiq.actor(queue_name="default")
def process_payment(order_id: str, amount: float):
    """Process payment for an order."""
    print(f"Processing payment for order {order_id}: ${amount}")
    # Implement payment processing logic here
    return f"Payment processed for order {order_id}"


@dramatiq.actor(queue_name="low_priority")
def generate_analytics(report_type: str, start_date: str, end_date: str):
    """Generate analytics report (low priority)."""
    print(f"Generating {report_type} report from {start_date} to {end_date}")
    # Implement report generation logic here
    return f"{report_type} report generated"


# -----------------------------------------------------------------------------
# FastAPI Application
# -----------------------------------------------------------------------------

app = FastAPI(
    title="FastAPI with BatchSQSBroker",
    description="Example of integrating BatchSQSBroker with FastAPI",
    version="1.0.0"
)


# -----------------------------------------------------------------------------
# Lifecycle Events - CRITICAL FOR PREVENTING MESSAGE LOSS
# -----------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    print("FastAPI application starting up...")
    print(f"BatchSQSBroker initialized with namespace: {broker.namespace}")
    
    # Enable logging for debugging
    import logging
    logging.getLogger('batch_sqs_broker').setLevel(logging.INFO)


@app.on_event("shutdown")
async def shutdown_event():
    """
    CRITICAL: Ensure all buffered messages are flushed on shutdown.
    Without this, any messages in the buffer will be lost!
    """
    print("FastAPI application shutting down...")
    
    try:
        # Get metrics before closing
        metrics = broker.get_metrics()
        print(f"Final broker metrics: {metrics}")
        
        # Close the broker to flush all buffers
        broker.close()
        print("BatchSQSBroker closed successfully - all buffers flushed")
        
    except Exception as e:
        print(f"Error during broker shutdown: {e}")
        # Log error but don't raise - allow shutdown to continue
        import traceback
        traceback.print_exc()


# -----------------------------------------------------------------------------
# Request/Response Models
# -----------------------------------------------------------------------------

class NotificationRequest(BaseModel):
    user_id: int
    message: str


class PaymentRequest(BaseModel):
    order_id: str
    amount: float


class AnalyticsRequest(BaseModel):
    report_type: str
    start_date: str
    end_date: str


class TaskResponse(BaseModel):
    status: str
    message_id: Optional[str] = None
    queue: Optional[str] = None


# -----------------------------------------------------------------------------
# API Endpoints
# -----------------------------------------------------------------------------

@app.get("/")
async def root():
    """Root endpoint with broker status."""
    metrics = broker.get_metrics()
    return {
        "application": "FastAPI with BatchSQSBroker",
        "broker_status": "active" if broker._running else "stopped",
        "metrics": metrics
    }


@app.post("/notify", response_model=TaskResponse)
async def send_notification_endpoint(request: NotificationRequest):
    """Send a high-priority notification."""
    try:
        message = send_notification.send(
            user_id=request.user_id,
            message=request.message
        )
        
        return TaskResponse(
            status="queued",
            message_id=message.message_id,
            queue="high_priority"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/payment", response_model=TaskResponse)
async def process_payment_endpoint(request: PaymentRequest):
    """Process a payment (normal priority)."""
    try:
        message = process_payment.send(
            order_id=request.order_id,
            amount=request.amount
        )
        
        return TaskResponse(
            status="queued",
            message_id=message.message_id,
            queue="default"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analytics", response_model=TaskResponse)
async def generate_analytics_endpoint(request: AnalyticsRequest):
    """Generate analytics report (low priority)."""
    try:
        message = generate_analytics.send(
            report_type=request.report_type,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        return TaskResponse(
            status="queued",
            message_id=message.message_id,
            queue="low_priority"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{queue_name}")
async def get_queue_status(queue_name: str):
    """Get status of a specific queue."""
    try:
        status = broker.get_queue_status(queue_name)
        return status
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Queue '{queue_name}' not found")


@app.post("/flush/{queue_name}")
async def flush_queue(queue_name: str):
    """Manually flush a specific queue."""
    try:
        broker.force_flush_queue(queue_name)
        return {"status": "flushed", "queue": queue_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Gunicorn Configuration (save as gunicorn_conf.py)
# -----------------------------------------------------------------------------

GUNICORN_CONFIG = """
import multiprocessing

# Worker configuration
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "uvicorn.workers.UvicornWorker"
bind = "0.0.0.0:8000"

# Timeout settings
graceful_timeout = 30  # Time for graceful shutdown
timeout = 60
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# CRITICAL: Worker lifecycle hooks for proper shutdown
def worker_int(worker):
    \"\"\"Called when worker receives SIGINT or SIGQUIT.\"\"\"
    print(f"Worker {worker.pid} received interrupt signal")
    _cleanup_broker()


def worker_abort(worker):
    \"\"\"Called when worker receives SIGABRT.\"\"\"
    print(f"Worker {worker.pid} received abort signal")
    _cleanup_broker()


def on_exit(server):
    \"\"\"Called just before master process exits.\"\"\"
    print("Master process exiting")
    _cleanup_broker()


def _cleanup_broker():
    \"\"\"Ensure broker is properly closed.\"\"\"
    import dramatiq
    try:
        broker = dramatiq.get_broker()
        if hasattr(broker, 'close'):
            print("Closing BatchSQSBroker to flush buffers...")
            broker.close()
            print("BatchSQSBroker closed successfully")
    except Exception as e:
        print(f"Error closing broker: {e}")
"""

# -----------------------------------------------------------------------------
# Running the Application
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n" + "="*60)
    print("FastAPI with BatchSQSBroker Integration Example")
    print("="*60)
    print("\nTo run this application:")
    print("\n1. Development mode (with auto-reload):")
    print("   uvicorn fastapi_integration:app --reload --port 8000")
    print("\n2. Production mode with Gunicorn:")
    print("   Save the GUNICORN_CONFIG content to 'gunicorn_conf.py'")
    print("   gunicorn -c gunicorn_conf.py fastapi_integration:app")
    print("\n3. Docker container:")
    print("   Ensure your Dockerfile uses proper signal handling:")
    print("   CMD [\"gunicorn\", \"-c\", \"gunicorn_conf.py\", \"fastapi_integration:app\"]")
    print("\nIMPORTANT: Always ensure proper shutdown handling to prevent message loss!")
    print("="*60 + "\n")
    
    # For development - run with uvicorn
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)