"""
BatchSQSBroker - A high-performance batch processing broker for AWS SQS.

This package provides intelligent message splitting, retry mechanisms, 
and comprehensive monitoring for Dramatiq with AWS SQS.
"""

__version__ = "1.0.0"

from .broker import BatchSQSBroker, FailedMessage

__all__ = ["BatchSQSBroker", "FailedMessage", "__version__"]