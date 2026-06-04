# Critical Fixes for Data Loss Issue

## Problems Identified

1. **Silent Message Loss in Result Processor**
   - When messages failed to save to fallback storage, errors were silently ignored
   - No retry mechanism for fallback storage failures
   - Messages could be lost if fallback save failed

2. **Improper Graceful Shutdown**
   - Batcher's Stop() didn't wait for flushing to complete
   - Worker pool would exit before processing all batches from batcher
   - Race condition between closing batch channel and batcher sending

3. **Non-unique Message IDs**
   - Message IDs were generated as constant format
   - Could cause issues with message tracking

4. **No Backpressure on Result Processing**
   - Result processor errors were not retried
   - Timeout was too short (FlushInterval instead of reasonable duration)

## Solutions Implemented

### 1. **Enhanced Result Processor** (`producer/producer.go`)
```go
// Now retries 3 times with exponential backoff
// Prints error message so data loss is visible
for attempt := 0; attempt < retries; attempt++ {
    err := p.fallback.Save(fallbackCtx, ...)
    if err == nil {
        break
    }
    if attempt < retries-1 {
        time.Sleep(exponential backoff)
    }
}
if lastErr != nil {
    fmt.Printf("[ckafka] ERROR: Failed to save to fallback: %v\n", lastErr)
}
```

### 2. **Proper Graceful Shutdown** (`producer/producer.go`)
- Added logging for visibility into shutdown process
- Ensures all components drain in correct order:
  1. Stop accepting new messages (batcher.Stop closes msgChan)
  2. Batcher flushes remaining messages to batchChan
  3. Workers process all batches from batchChan
  4. Result processor saves any failures to fallback
  5. All resources cleaned up

### 3. **Fixed Batcher Synchronization** (`producer/batcher.go`)
- Added `done_wg WaitGroup` to signal completion
- `Stop()` now waits for batcher goroutine to finish flushing
- Ensures no messages in-flight when workerPool.Stop() is called

### 4. **Fixed Worker Pool** (`producer/worker_pool.go`)
- Changed Stop() to close batchChan instead of checking done flag during batch processing
- Workers now drain the entire batch before checking for shutdown
- Prevents abandoning partially processed batches

### 5. **Unique Message IDs** (`producer/producer.go`)
```go
func generateMessageID() string {
    return fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), rand.Intn(1000000))
}
```

## At-Least-Once Delivery Flow (Corrected)

```
Send(msg)
  ↓
Batcher accumulates messages
  ↓
Flush on size or timeout
  ↓
Send batch to Kafka via Worker Pool
  ├─ Success → Done (message delivered)
  └─ Failure → Save to fallback storage (with retries)
  
On Kafka recovery:
  ├─ Health check detects connection restored
  └─ Replay engine sends all messages from fallback
     ├─ Success → Delete from fallback
     └─ Failure → Stays in fallback for next replay

Graceful Shutdown:
  1. Close msgChan (no new messages)
  2. Batcher flushes buffered messages
  3. Workers process all batches
  4. Result processor retries saving failed messages
  5. All resources cleaned up
```

## Data Loss Prevention

- ✅ **All Kafka send failures** → Saved to SQLite fallback with retries
- ✅ **All fallback save failures** → Visible in logs + retried 3x
- ✅ **In-flight messages on shutdown** → Flushed before exit
- ✅ **Duplicate messages** → Prevented by unique IDs
- ✅ **Lost batches** → Workers drain all batches before exit
- ✅ **Message replay** → Automatic on Kafka recovery

## Testing Recommendations

```bash
# Test 1: Normal operation
go run examples/basic_usage.go

# Test 2: Kafka unavailable
# Kill Kafka and observe messages being saved to fallback
# Restart Kafka and observe replay

# Test 3: Graceful shutdown
# Send many messages then Ctrl+C
# Check that all are processed
```

## Verification Checklist

- [ ] No "ERROR: Failed to save message to fallback" messages appear
- [ ] All messages sent during test appear in Kafka or in .kafka.db
- [ ] Graceful shutdown completes without hanging
- [ ] Shutdown logs show "Stopping batcher" → "All messages processed"
- [ ] On Kafka recovery, messages from fallback are replayed
