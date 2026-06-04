# company-kafka-reliable (ckafka)

Production-grade reliable Kafka producer library for internal company use.

This library standardizes Kafka publishing across teams by providing:

- Automatic batching
- Worker pool processing
- Retry handling
- Durable fallback storage
- Automatic replay on recovery
- Health monitoring
- At-least-once delivery guarantees

---

## 📦 Module Name

```
company-kafka-reliable
```

Short alias:

```
ckafka
```

---

## 🎯 Design Goals

- Prevent message loss during Kafka outages
- Eliminate duplicate retry logic across services
- Provide infrastructure-level reliability standard
- Keep service API extremely simple
- Support high throughput with batching + concurrency

---

## 🏗 High-Level Architecture

```
company-kafka-reliable/
│
├── producer/
│   ├── producer.go
│   ├── batcher.go
│   ├── worker_pool.go
│   ├── retry.go
│
├── fallback/
│   ├── store.go
│   ├── replay.go
│
├── health/
│   ├── health.go
│
├── config/
│   ├── config.go
│
└── internal/
```

---

## 🧠 Core Components

### 1️⃣ Reliable Producer Engine

Wraps Kafka producer (recommended: Sarama).

**Responsibilities:**

- Accept messages from application
- Push messages into batcher
- Manage worker pool
- Handle Kafka acknowledgments
- Route failures to fallback storage

---

### 2️⃣ Batching Layer

Buffers messages before sending to Kafka.

**Flush conditions:**

- Max batch size reached
- Flush timeout reached

```go
type BatchConfig struct {
    MaxBatchSize  int
    FlushInterval time.Duration
}
```

**Benefits:**

- Higher throughput
- Lower network overhead
- Reduced broker pressure

---

### 3️⃣ Worker Pool

Multiple workers consume batches concurrently.

```go
type WorkerPool struct {
    Workers int
}
```

Each worker:

1. Pulls batch  
2. Sends to Kafka  
3. Waits for ack  
4. On success → done  
5. On failure → send to fallback store  

---

### 4️⃣ Fallback Storage (Durable Reliability Layer)

Used when Kafka send fails.

#### Interface

```go
type FallbackStore interface {
    Save(messages []Message) error
    FetchBatch(limit int) ([]Message, error)
    Delete(ids []string) error
}
```

#### Recommended Implementation

PostgreSQL (best practice)

Other options:

- Redis
- ClickHouse
- Local persistent queue

PostgreSQL is preferred for:

- Durability
- Query control
- Transaction support
- Operational familiarity

---

### 5️⃣ Replay Engine

Automatically replays failed messages when Kafka recovers.

**Flow:**

1. Health check detects Kafka recovery  
2. Replay worker starts  
3. Fetch unsent messages  
4. Send in batches  
5. Delete after successful acknowledgment  

Runs in background goroutine.

---

### 6️⃣ Health Check System

Tracks Kafka connection state.

```go
type KafkaStatus struct {
    Connected   bool
    LastError   error
    LastSuccess time.Time
}
```

Health loop:

- Periodically ping broker  
- Detect disconnection  
- Detect recovery  
- Trigger replay when connection restored  

---

## 🔥 Full Message Flow

```
App → Submit(msg)
        ↓
    Batcher
        ↓
    Worker Pool
        ↓
    Kafka Send
        ↓
   ┌─────────────┐
   │ Ack Success │ → DONE
   └─────────────┘
        ↓
   ┌─────────────┐
   │ Ack Fail    │ → Save to DB
   └─────────────┘

Health Restored →
        ↓
   Replay From DB →
        ↓
   Delete After Success
```

---

## 🧩 Public API Design

Extremely simple for services:

```go
producer, _ := reliablekafka.New(config)

producer.Send(ctx, Message{
    Topic: "device-metrics",
    Key:   []byte("device1"),
    Value: payload,
})
```

All batching, retry, fallback, and replay are handled internally.

---

## ⚙️ Configuration

```go
type Config struct {
    Brokers []string

    BatchSize     int
    FlushInterval time.Duration
    Workers       int

    RetryCount   int
    RetryBackoff time.Duration

    FallbackEnabled bool
}
```

---

## 🛡 Delivery Guarantee

**Default Strategy:**

At-least-once delivery  
With durable DB fallback  

This is the most practical and production-safe reliability model.

---

## 💎 Advanced Features (Future v2)

- Circuit breaker
- Dead Letter Queue (DLQ) support
- Prometheus metrics
- Graceful shutdown
- Idempotent producer support
- Exactly-once semantics
- Compression support
- Partition strategy control

---

## 🏢 Why This Library Matters

Without this library, teams would:

- Write their own retry logic
- Lose messages during Kafka outages
- Implement inconsistent replay mechanisms
- Duplicate reliability code

With this library:

```
import "company-kafka-reliable"
```

And reliability is handled automatically.

This becomes infrastructure-level standardization.

---

## 🏷 Versioning Strategy

Start with:

```
v1.0.0
```

Maintain API stability.

For breaking changes:

```
module company.com/kafka-reliable/v2
```

---

## 🚀 Publishing to Nexus

Host internally on Sonatype Nexus.

Set Go environment:

```bash
go env -w GOPROXY=https://nexus.company.com/repository/go-group/,direct
go env -w GOPRIVATE=company.com
```

---

## 🧠 Recommended Mode

Recommended production mode:

- Async batching
- Worker pool concurrency
- At-least-once delivery
- Durable PostgreSQL fallback
- Automatic replay on recovery

This provides strong reliability with high throughput and operational simplicity.

---

# ✅ Status

Initial Version: `v1.0.0`  
Delivery Guarantee: At-least-once  
Fallback: Durable DB  
Replay: Automatic  
Designed For: Production scale systems  
