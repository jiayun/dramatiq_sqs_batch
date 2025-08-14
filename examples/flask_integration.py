"""
Flask integration example with BatchSQSBroker.

This example demonstrates:
1. How to integrate BatchSQSBroker with Flask
2. Proper shutdown handling using atexit and signal handlers
3. Gunicorn configuration for production deployment
4. RESTful API endpoints that trigger background tasks
"""

import os
import sys
import atexit
import signal
from typing import Dict, Any

import dramatiq
from flask import Flask, jsonify, request
from batch_sqs_broker import BatchSQSBroker

# -----------------------------------------------------------------------------
# Broker Setup
# -----------------------------------------------------------------------------

# Create and configure the broker
broker = BatchSQSBroker(
    namespace="flask-app-",
    default_batch_interval=1.0,  # Wait up to 1 second
    default_idle_timeout=0.1,    # Send after 100ms idle
    max_buffer_size_per_queue=5000,
    max_retry_attempts=3,
    # Configure different priorities
    group_batch_intervals={
        "urgent": 0,          # Send immediately
        "default": 1.0,       # Normal priority
        "background": 10.0,   # Low priority, batch for efficiency
    },
    group_idle_timeouts={
        "urgent": 0,
        "default": 0.1,
        "background": 2.0,
    }
)

# Set as default broker
dramatiq.set_broker(broker)

# -----------------------------------------------------------------------------
# Dramatiq Tasks
# -----------------------------------------------------------------------------

@dramatiq.actor(queue_name="urgent")
def send_email(to_address: str, subject: str, body: str):
    """Send urgent email notification."""
    print(f"[URGENT] Sending email to {to_address}")
    print(f"  Subject: {subject}")
    print(f"  Body: {body[:100]}...")
    # Implement actual email sending logic here
    return f"Email sent to {to_address}"


@dramatiq.actor(queue_name="default")
def process_order(order_id: str, customer_id: str, items: list):
    """Process customer order."""
    print(f"Processing order {order_id} for customer {customer_id}")
    print(f"  Items: {items}")
    # Implement order processing logic here
    return f"Order {order_id} processed"


@dramatiq.actor(queue_name="background")
def cleanup_old_data(days_old: int):
    """Background task to cleanup old data."""
    print(f"[BACKGROUND] Cleaning up data older than {days_old} days")
    # Implement cleanup logic here
    return f"Cleaned up data older than {days_old} days"


@dramatiq.actor(queue_name="background")
def generate_report(report_type: str, params: dict):
    """Generate report in background."""
    print(f"[BACKGROUND] Generating {report_type} report")
    print(f"  Parameters: {params}")
    # Implement report generation logic here
    return f"{report_type} report generated"


# -----------------------------------------------------------------------------
# Flask Application
# -----------------------------------------------------------------------------

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Enable logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Enable batch_sqs_broker logging
logging.getLogger('batch_sqs_broker').setLevel(logging.INFO)


# -----------------------------------------------------------------------------
# Shutdown Handling - CRITICAL FOR PREVENTING MESSAGE LOSS
# -----------------------------------------------------------------------------

def cleanup_broker():
    """
    Cleanup function to ensure broker is properly closed.
    This is critical to prevent message loss!
    """
    logger.info("Flask application shutting down...")
    
    try:
        # Get final metrics
        metrics = broker.get_metrics()
        logger.info(f"Final broker metrics: {metrics}")
        
        # Close the broker to flush all buffers
        broker.close()
        logger.info("BatchSQSBroker closed successfully - all buffers flushed")
        
    except Exception as e:
        logger.error(f"Error during broker shutdown: {e}")
        import traceback
        traceback.print_exc()


# Register cleanup function with atexit
atexit.register(cleanup_broker)


# Also handle SIGTERM and SIGINT for container environments
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}")
    cleanup_broker()
    sys.exit(0)


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# -----------------------------------------------------------------------------
# API Routes
# -----------------------------------------------------------------------------

@app.route('/')
def index():
    """Root endpoint with broker status."""
    metrics = broker.get_metrics()
    return jsonify({
        "application": "Flask with BatchSQSBroker",
        "broker_status": "active" if broker._running else "stopped",
        "metrics": metrics
    })


@app.route('/email', methods=['POST'])
def send_email_endpoint():
    """Send an urgent email."""
    data = request.get_json()
    
    if not all(k in data for k in ['to', 'subject', 'body']):
        return jsonify({"error": "Missing required fields: to, subject, body"}), 400
    
    try:
        message = send_email.send(
            to_address=data['to'],
            subject=data['subject'],
            body=data['body']
        )
        
        return jsonify({
            "status": "queued",
            "message_id": message.message_id,
            "queue": "urgent"
        }), 202
        
    except Exception as e:
        logger.error(f"Error queueing email: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/order', methods=['POST'])
def process_order_endpoint():
    """Process a customer order."""
    data = request.get_json()
    
    if not all(k in data for k in ['order_id', 'customer_id', 'items']):
        return jsonify({"error": "Missing required fields"}), 400
    
    try:
        message = process_order.send(
            order_id=data['order_id'],
            customer_id=data['customer_id'],
            items=data['items']
        )
        
        return jsonify({
            "status": "queued",
            "message_id": message.message_id,
            "queue": "default"
        }), 202
        
    except Exception as e:
        logger.error(f"Error queueing order: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/cleanup', methods=['POST'])
def cleanup_data_endpoint():
    """Trigger data cleanup task."""
    data = request.get_json()
    days_old = data.get('days_old', 30)
    
    try:
        message = cleanup_old_data.send(days_old=days_old)
        
        return jsonify({
            "status": "queued",
            "message_id": message.message_id,
            "queue": "background"
        }), 202
        
    except Exception as e:
        logger.error(f"Error queueing cleanup: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/report', methods=['POST'])
def generate_report_endpoint():
    """Generate a report in background."""
    data = request.get_json()
    
    if 'report_type' not in data:
        return jsonify({"error": "Missing report_type"}), 400
    
    try:
        message = generate_report.send(
            report_type=data['report_type'],
            params=data.get('params', {})
        )
        
        return jsonify({
            "status": "queued",
            "message_id": message.message_id,
            "queue": "background"
        }), 202
        
    except Exception as e:
        logger.error(f"Error queueing report: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/status/<queue_name>')
def get_queue_status(queue_name: str):
    """Get status of a specific queue."""
    try:
        status = broker.get_queue_status(queue_name)
        return jsonify(status)
    except KeyError:
        return jsonify({"error": f"Queue '{queue_name}' not found"}), 404


@app.route('/flush/<queue_name>', methods=['POST'])
def flush_queue(queue_name: str):
    """Manually flush a specific queue."""
    try:
        broker.force_flush_queue(queue_name)
        return jsonify({"status": "flushed", "queue": queue_name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/health')
def health_check():
    """Health check endpoint."""
    try:
        metrics = broker.get_metrics()
        return jsonify({
            "status": "healthy",
            "broker_running": broker._running,
            "background_thread_alive": metrics.get("background_thread_alive", False)
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


# -----------------------------------------------------------------------------
# Error Handlers
# -----------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return jsonify({"error": "Internal server error"}), 500


# -----------------------------------------------------------------------------
# Gunicorn Configuration (save as gunicorn_conf.py)
# -----------------------------------------------------------------------------

GUNICORN_CONFIG = """
import multiprocessing
import sys

# Worker configuration
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "sync"  # Use 'gevent' for async if needed
bind = "0.0.0.0:5000"

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
        print(f"Error closing broker: {e}", file=sys.stderr)
"""

# -----------------------------------------------------------------------------
# Running the Application
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Flask with BatchSQSBroker Integration Example")
    print("="*60)
    print("\nTo run this application:")
    print("\n1. Development mode (with debug):")
    print("   python flask_integration.py")
    print("\n2. Production mode with Gunicorn:")
    print("   Save the GUNICORN_CONFIG content to 'gunicorn_conf.py'")
    print("   gunicorn -c gunicorn_conf.py flask_integration:app")
    print("\n3. Docker container:")
    print("   Ensure your Dockerfile uses proper signal handling:")
    print("   CMD [\"gunicorn\", \"-c\", \"gunicorn_conf.py\", \"flask_integration:app\"]")
    print("\n4. Test endpoints:")
    print("   curl http://localhost:5000/")
    print("   curl -X POST http://localhost:5000/email -H 'Content-Type: application/json' \\")
    print("        -d '{\"to\": \"user@example.com\", \"subject\": \"Test\", \"body\": \"Hello!\"}'")
    print("\nIMPORTANT: Always ensure proper shutdown handling to prevent message loss!")
    print("="*60 + "\n")
    
    # Run Flask development server
    app.run(host='0.0.0.0', port=5000, debug=True)