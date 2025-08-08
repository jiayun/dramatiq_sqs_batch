# Changelog

All notable changes to BatchSQSBroker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-08-08

### 🎉 Initial Release

#### Features
- **Intelligent Batch Processing**
  - Automatically batches messages up to AWS SQS limits (10 messages, 256KB)
  - Smart message splitting using greedy algorithm optimization
  - Handles oversized batches gracefully

- **Reliability & Performance**
  - Exponential backoff retry mechanism with configurable limits
  - Thread-safe operations for concurrent message processing
  - Buffer overflow protection with backpressure management
  - Optimized for high-volume task queues

- **Monitoring & Observability**
  - Comprehensive metrics API for monitoring
  - Per-queue status tracking
  - Failed message count tracking
  - Batch split statistics

- **Configuration & Flexibility**
  - Per-queue batch intervals and idle timeouts
  - Configurable retry attempts
  - Adjustable buffer sizes
  - Support for immediate flushing

- **Developer Experience**
  - Simple drop-in replacement for standard SQSBroker
  - Clean API design
  - Extensive documentation and examples
  - Python 3.8+ support

#### Technical Highlights
- Addresses AWS SQS 256KB batch size limitation
- Prevents infinite retry loops through smart error handling
- Memory-efficient with configurable buffer limits
- Background thread for automatic timeout-based flushing

#### Documentation
- Complete README with usage examples
- API reference documentation
- Best practices guide
- Performance tuning tips

## [Unreleased]

### Planned Features
- [ ] Metrics export to CloudWatch
- [ ] Dead letter queue (DLQ) support
- [ ] Message priority queuing
- [ ] Enhanced error reporting
- [ ] Performance benchmarks
- [ ] Integration tests with LocalStack

### Under Consideration
- Support for FIFO queues
- Message deduplication strategies
- Custom serialization formats
- Rate limiting capabilities
- Circuit breaker pattern

---

For more information, see the [GitHub repository](https://github.com/jiayun/dramatiq_sqs_batch).