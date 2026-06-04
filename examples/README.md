# ckafka Examples

This directory contains examples of using the ckafka (company-kafka-reliable) library.

## Running Examples

### Prerequisites

- Go 1.25.0 or later
- Kafka broker running (for real usage, not needed for basic example with fallback)

### Basic Usage

The `basic_usage.go` example demonstrates:

```bash
go run examples/basic_usage.go
```

This example:
1. Creates a ReliableProducer with SQLite fallback storage
2. Sends 10 messages to a Kafka topic
3. Demonstrates graceful shutdown

### Configuration

Key configuration options:

- **Brokers**: List of Kafka broker addresses
- **BatchSize**: Max messages per batch (default: 100)
- **FlushInterval**: Max time to wait before flushing a batch (default: 1s)
- **Workers**: Number of concurrent workers (default: 10)
- **RetryCount**: Max retry attempts (default: 3)
- **RetryBackoff**: Initial backoff duration for retries (default: 100ms)
- **FallbackEnabled**: Enable SQLite fallback for failed messages
- **FallbackDBPath**: Path to SQLite database file (default: ckafka_fallback.db)

### Message Structure

```go
msg := internal.Message{
    Topic: "pranav-test-topic",
    Key:   []byte("message-key"),
    Value: []byte("message-value"),
}

err := producer.Send(ctx, msg)
```

### Error Handling

The producer automatically:
- Retries failed messages with exponential backoff
- Saves failed messages to SQLite fallback storage
- Replays messages when Kafka connection is restored
- Provides graceful shutdown with context timeout

### Fallback Storage

Failed messages are automatically saved to SQLite. When Kafka recovers, messages are automatically replayed. This ensures at-least-once delivery semantics.

To use fallback:
```go
cfg.FallbackEnabled = true
cfg.FallbackDBPath = "ckafka_fallback.db"
```

### Health Monitoring

The producer continuously monitors Kafka connection health and automatically triggers replay when connection is restored.
