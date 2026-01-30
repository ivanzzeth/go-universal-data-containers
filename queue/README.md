# Queue Package

A generic, thread-safe queue implementation for Go with support for multiple storage backends, automatic retry mechanisms, dead-letter queues (DLQ), and Prometheus metrics.

## Features

- **Generic Type Support**: Works with any data type using Go generics
- **Multiple Backends**: Memory and Redis implementations with unified factory
- **Zero Registration**: Built-in types work out of the box, no registration required
- **Automatic Retry**: Failed messages are automatically retried with configurable limits
- **Dead-Letter Queue (DLQ)**: Messages exceeding retry limits are moved to DLQ for manual inspection
- **Priority Retry Queue**: Retry messages are processed with higher priority than new messages
- **Blocking Operations**: Support for blocking enqueue/dequeue with context cancellation
- **Parallel Callbacks**: Optional parallel execution of message handlers
- **Prometheus Metrics**: Built-in observability with queue depth, latency, and error metrics
- **Thread-Safe**: All operations are thread-safe

## Installation

```bash
go get github.com/ivanzzeth/go-universal-data-containers
```

## Quick Start

### Using Unified Factory (Recommended)

The unified factory allows you to switch between backends using configuration. **No registration required** for built-in types (Memory, Redis):

```go
package main

import (
    "context"
    "fmt"

    "github.com/ivanzzeth/go-universal-data-containers/queue"
)

func main() {
    // Configuration can come from JSON file or environment
    config := queue.UnifiedQueueConfig{
        Type:              queue.QueueTypeMemory, // or queue.QueueTypeRedis
        MaxSize:           1000,
        MaxHandleFailures: 3,
        ConsumerCount:     2,
    }

    // Create factory - works with any message type via generics
    factory, err := queue.NewUnifiedFactory(config, queue.NewJsonMessage([]byte{}))
    if err != nil {
        panic(err)
    }

    // Create queue
    q, err := factory.GetOrCreateSafe("my-queue")
    if err != nil {
        panic(err)
    }
    defer q.Close()

    ctx := context.Background()

    // Enqueue message
    err = q.Enqueue(ctx, []byte("hello world"))
    if err != nil {
        panic(err)
    }

    // Dequeue message
    msg, err := q.Dequeue(ctx)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Received: %s\n", msg.Data())
}
```

### Using Custom Message Types

The factory automatically supports any message type through Go generics:

```go
type MyMessage struct {
    ID   string `json:"id"`
    Data string `json:"data"`
}

config := queue.UnifiedQueueConfig{
    Type:    queue.QueueTypeMemory,
    MaxSize: 1000,
}

// Just pass your type to NewJsonMessage - no registration needed!
factory, _ := queue.NewUnifiedFactory(config, queue.NewJsonMessage(MyMessage{}))
q, _ := factory.GetOrCreateSafe("my-queue")

// Enqueue and dequeue with your custom type
q.Enqueue(ctx, MyMessage{ID: "123", Data: "test"})
msg, _ := q.Dequeue(ctx)
fmt.Println(msg.Data().ID) // "123"
```

### Using Redis Backend

```go
// Redis configuration
backendConfig, _ := json.Marshal(queue.RedisQueueConfig{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
})

config := queue.UnifiedQueueConfig{
    Type:              queue.QueueTypeRedis,
    MaxSize:           10000,
    MaxHandleFailures: 5,
    BackendConfig:     backendConfig,
}

factory, _ := queue.NewUnifiedFactory(config, queue.NewJsonMessage([]byte{}))
q, _ := factory.GetOrCreateSafe("redis-queue")
```

### Using Existing Redis Client

```go
import "github.com/redis/go-redis/v9"

// Create your own Redis client
rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

// Pass client directly via config - no registration needed!
config := queue.UnifiedQueueConfig{
    Type:        queue.QueueTypeRedis,
    MaxSize:     10000,
    RedisClient: rdb, // Use existing client
}

factory, _ := queue.NewUnifiedFactory(config, queue.NewJsonMessage([]byte{}))
```

### JSON Configuration

Configuration can be loaded from JSON:

```json
{
    "type": "redis",
    "max_size": 10000,
    "max_handle_failures": 5,
    "consumer_count": 4,
    "callback_parallel": true,
    "backend_config": {
        "addr": "localhost:6379",
        "password": "",
        "db": 0
    }
}
```

```go
var config queue.UnifiedQueueConfig
json.Unmarshal(configJSON, &config)
factory, _ := queue.NewUnifiedFactory(config, queue.NewJsonMessage([]byte{}))
```

### Custom Queue Backend

For custom queue backend implementations (e.g., Kafka, SQS), use the registry:

```go
func init() {
    queue.RegisterQueueCreator(MyQueueType, myQueueCreator)
    queue.RegisterQueueValidator(MyQueueType, &MyConfigValidator{})
}
```

## Subscribe Pattern

For continuous message processing, use the Subscribe pattern:

```go
ctx := context.Background()

q.Subscribe(ctx, func(ctx context.Context, msg queue.Message[[]byte]) error {
    // Process message
    data := msg.Data()

    // Return nil on success
    // Return error to trigger automatic retry
    if err := processMessage(data); err != nil {
        return err // Message will be retried
    }

    return nil
})

// Keep the application running
select {}
```

**Important**: When the handler returns an error, the message is automatically placed in the retry queue and will be reprocessed. Messages exceeding `MaxHandleFailures` are moved to the DLQ.

## Dead-Letter Queue (DLQ)

Access and manage failed messages:

```go
// Get DLQ
dlq, err := q.DLQ()
if err != nil {
    // DLQ not supported or error
}

// Manually dequeue from DLQ for inspection
msg, err := dlq.Dequeue(ctx)

// Redrive messages back to the main queue
// This moves `n` messages from DLQ to the retry queue for reprocessing
err = dlq.Redrive(ctx, 10) // Redrive 10 messages
```

## Configuration Options

### UnifiedQueueConfig

| Field | Type | Description |
|-------|------|-------------|
| `Type` | `QueueType` | Backend type: `"memory"` or `"redis"` |
| `MaxSize` | `int` | Maximum queue size (-1 for unlimited) |
| `MaxHandleFailures` | `int` | Max retries before moving to DLQ |
| `ConsumerCount` | `int` | Number of concurrent consumers |
| `CallbackParallel` | `bool` | Enable parallel callback execution |
| `UnlimitedCapacity` | `int` | Buffer size when MaxSize is unlimited (Memory only) |
| `RetryQueueCapacity` | `int` | Retry queue buffer size |
| `BackendConfig` | `json.RawMessage` | Backend-specific configuration |
| `RedisClient` | `redis.Cmdable` | Existing Redis client (optional, not serializable) |

### RedisQueueConfig

| Field | Type | Description |
|-------|------|-------------|
| `Addr` | `string` | Redis server address (e.g., `"localhost:6379"`) |
| `Password` | `string` | Redis password (optional) |
| `DB` | `int` | Redis database number (default: 0) |

### Functional Options

You can also pass options when creating queues:

```go
q, _ := factory.GetOrCreateSafe("my-queue",
    queue.WithMaxSize(5000),
    queue.WithMaxHandleFailures(10),
    queue.WithConsumerCount(4),
    queue.WithCallbackParallelExecution(true),
    queue.WithCallbackTimeout(30*time.Second),
    queue.WithRetryQueueCapacity(20000),
)
```

## Blocking Operations

For producer/consumer patterns with backpressure:

```go
// Blocking enqueue - waits until space is available
err := q.BEnqueue(ctx, data)

// Blocking dequeue - waits until message is available
msg, err := q.BDequeue(ctx)
```

Both operations respect context cancellation:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

msg, err := q.BDequeue(ctx)
if err == context.DeadlineExceeded {
    // Timeout
}
```

## Prometheus Metrics

The package exports the following Prometheus metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `queue_depth` | Gauge | Current number of messages (labels: queue_name, queue_type) |
| `queue_capacity` | Gauge | Maximum queue capacity (labels: queue_name, queue_type) |
| `queue_inflight` | Gauge | Messages currently being processed |
| `queue_consumers_active` | Gauge | Number of active consumers |
| `queue_enqueue_total` | Counter | Total enqueue operations |
| `queue_enqueue_error_total` | Counter | Failed enqueue operations |
| `queue_dequeue_total` | Counter | Total dequeue operations |
| `queue_dequeue_error_total` | Counter | Failed dequeue operations |
| `queue_enqueue_duration_seconds` | Histogram | Enqueue operation latency |
| `queue_dequeue_duration_seconds` | Histogram | Dequeue operation latency |
| `queue_handle_duration_seconds` | Histogram | Message handler execution time |
| `queue_message_age_seconds` | Histogram | Time from message creation to processing |
| `queue_dlq_messages_total` | Counter | Messages moved to DLQ |
| `queue_redrive_total` | Counter | DLQ redrive operations |
| `queue_redrive_successful_total` | Counter | Successful redrive operations |
| `queue_redrive_error_total` | Counter | Failed redrive operations |

## Architecture

```
                    +-------------------+
                    |  UnifiedFactory   |
                    +-------------------+
                            |
              +-------------+-------------+
              |                           |
    +------------------+        +------------------+
    |   MemoryQueue    |        |    RedisQueue    |
    +------------------+        +------------------+
              |                           |
              +-------------+-------------+
                            |
                    +-------------------+
                    |   SimpleQueue     |
                    |   (SafeQueue)     |
                    +-------------------+
                            |
                    +-------------------+
                    |   Metrics Layer   |
                    +-------------------+
```

### Queue Flow

```
+--------+     +-----------+     +-------------+
| Client | --> | MainQueue | --> | RetryQueue  | (higher priority)
+--------+     +-----------+     +-------------+
                    |                   |
                    +-------------------+
                            |
                    +---------------+
                    |   Consumer    |
                    +---------------+
                            |
                    +---------------+
                    |   Handler     | --> success: done
                    +---------------+ --> failure: RetryQueue
                            |              (up to MaxHandleFailures)
                            v
                    +---------------+
                    |     DLQ       | --> manual inspection
                    +---------------+     or Redrive
```

## Error Handling

| Error | Description |
|-------|-------------|
| `ErrQueueClosed` | Queue has been closed |
| `ErrQueueFull` | Queue is at capacity (non-blocking operations) |
| `ErrQueueEmpty` | No messages available (non-blocking operations) |
| `ErrInvalidData` | Invalid message data |

## Thread Safety

All queue operations are thread-safe. Multiple goroutines can safely:
- Enqueue messages concurrently
- Subscribe with multiple consumers
- Access DLQ operations

## Best Practices

1. **Always handle errors**: Check return values, especially for DLQ operations
2. **Use Subscribe for consumers**: It handles retry logic automatically
3. **Set appropriate MaxHandleFailures**: Balance between retry attempts and moving to DLQ
4. **Monitor DLQ**: Regularly check and process DLQ messages
5. **Use context cancellation**: Enables graceful shutdown
6. **Close queues on shutdown**: Call `q.Close()` to drain in-flight messages

## License

MIT License
