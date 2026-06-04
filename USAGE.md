# ckafka Usage Guide

## Installation

```bash
go get github.com/Pranav2259/go-kafka-reliable
```

---

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/Pranav2259/go-kafka-reliable/config"
    "github.com/Pranav2259/go-kafka-reliable/internal"
    "github.com/Pranav2259/go-kafka-reliable/producer"
)

func main() {
    cfg := &config.Config{
        Brokers:         []string{"localhost:9092"},
        BatchSize:       100,
        FlushInterval:   1 * time.Second,
        Workers:         5,
        RetryCount:      3,
        RetryBackoff:    500 * time.Millisecond,
        FallbackEnabled: true,
        FallbackDBPath:  ".ckafka_fallback.db",
        Debug:           false,
    }

    prod, err := producer.New(cfg)
    if err != nil {
        log.Fatalf("failed to create producer: %v", err)
    }

    ctx := context.Background()
    prod.Start(ctx)
    defer prod.Shutdown(ctx)

    prod.Send(ctx, internal.Message{
        Topic: "my-topic",
        Key:   []byte("device-1"),
        Value: []byte(`{"temp": 42}`),
    })
}
```

---

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `Brokers` | `[]string` | required | Kafka broker addresses |
| `BatchSize` | `int` | `100` | Max messages per batch |
| `FlushInterval` | `duration` | `3s` | Flush batch after this interval even if not full |
| `Workers` | `int` | `5` | Number of concurrent send workers |
| `RetryCount` | `int` | `3` | Retries per batch before sending to fallback |
| `RetryBackoff` | `duration` | `500ms` | Base backoff between retries (exponential) |
| `SendTimeout` | `duration` | `5s` | Timeout per Kafka send attempt |
| `FallbackEnabled` | `bool` | `false` | Enable SQLite fallback on Kafka failure |
| `FallbackDBPath` | `string` | `.ckafka_fallback.db` | Path to SQLite fallback database |
| `MaxMessageBytes` | `int` | `1048576` (1MB) | Max size of a single message in bytes. Increase for large payloads (e.g. SNMP/timeseries). Must also match broker `message.max.bytes`. |
| `Debug` | `bool` | `false` | Enable verbose debug logging |

---

## Message

```go
internal.Message{
    ID:    "optional-custom-id",   // auto-generated if empty
    Topic: "my-topic",             // required
    Key:   []byte("partition-key"),
    Value: []byte("payload"),
}
```

`ID` is used for deduplication in the fallback store. If you leave it empty, a unique ID is generated automatically.

---

## Lifecycle

```go
prod, _ := producer.New(cfg)

// Start background goroutines (batcher, workers, health checker)
prod.Start(ctx)

// Send messages (non-blocking, queues into batcher)
prod.Send(ctx, msg)

// Graceful shutdown — drains in-flight messages
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
prod.Shutdown(shutdownCtx)
```

`Start` must be called before `Send`. `Shutdown` flushes the batcher and waits for workers to finish.

---

## Fallback & Replay

When Kafka is unreachable:
- Failed batches are saved to a local SQLite database
- The health checker pings brokers every 5 seconds
- When connectivity is restored, the replay engine automatically re-sends stored messages and deletes them from the database after successful delivery

No application code changes are needed — this happens entirely in the background.

```
Kafka down  → messages saved to SQLite
Kafka up    → replay engine sends → deletes from SQLite → done
```

---

## Logging

**Production (Debug: false)** — only significant events:
```
[ckafka] Kafka connection lost, failed batches will be saved to fallback: ...
[ckafka] worker=2 batch=5 saved 100 messages to fallback (Kafka error: ...)
[ckafka] Kafka connection restored, starting fallback replay
[ckafka] fallback replay complete, replayed 300 messages
[ckafka] ERROR worker=1 batch=3 failed to send ...
```

**Debug (Debug: true)** — verbose, every batch and ping:
```
[ckafka] DEBUG worker=1 batch=1 processing messages=100
[ckafka] DEBUG worker=1 batch=1 done messages=100 duration=12ms
[ckafka] DEBUG health check: ping OK
[ckafka] DEBUG fallback replay sending batch of 100 messages
[ckafka] DEBUG fallback replay deleted 100 messages from store
```

---

## Error Handling

`Send` returns an error only if:
- The producer is shut down (`ErrShutdown`)
- The context is cancelled before the message is queued

Kafka delivery errors do **not** surface back to `Send` — they are handled internally via retry and fallback.

---

## Delivery Guarantee

**At-least-once.** A message may be delivered more than once if:
- Kafka acks but then crashes before the producer records success
- A replayed message was already delivered before the producer crashed

This is standard for high-throughput reliable producers. Design consumers to be idempotent (use the message `ID` as a deduplication key if needed).

---

## Custom Fallback ID for Deduplication

```go
prod.Send(ctx, internal.Message{
    ID:    "order-svc-order-1234",  // stable business key
    Topic: "orders",
    Value: payload,
})
```

If the same message is saved to fallback twice (e.g. crash mid-retry), `INSERT OR IGNORE` ensures it is stored only once.
