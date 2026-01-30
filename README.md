# Universal Data Containers (UDC)

## Overview
**Universal Data Containers (UDC)** is a Go library that provides standardized interfaces for common data structures with multiple backend implementations. It enables developers to write environment-agnostic code that can seamlessly transition between in-memory (for testing) and production-ready backends (Redis, GORM/PostgreSQL, etc.) without changing application logic.

## Core Principles

1. **Standardized Interfaces**: Well-defined contracts for each data structure
2. **Multiple Implementations**: Memory, Redis, GORM and other backends
3. **Zero-Cost Abstraction**: Minimal performance overhead
4. **Thread Safety**: All implementations are concurrency-safe
5. **Pluggable Architecture**: Easy to add new backends
6. **Prometheus Metrics**: Built-in observability for all components

## Installation

```bash
go get github.com/ivanzzeth/go-universal-data-containers
```

## Packages

### 1. Queue (`queue`)

Generic, thread-safe queue with automatic retry, DLQ, and multiple backends.

**Features:**
- Generic type support with Go generics
- Memory and Redis backends
- Automatic retry with configurable limits
- Dead-Letter Queue (DLQ) for failed messages
- Blocking operations with context cancellation
- Queue discovery for persistent backends
- Prometheus metrics

**Factory Types:**

| Factory | Type Parameter | Discoverable | Use Case |
|---------|---------------|--------------|----------|
| `MemoryFactory[T]` | At construction | No | Single type, simple API |
| `RedisQueueFactory[T]` | At construction | No | Single type, simple API |
| `MemoryQueueFactory` | At queue creation | Yes | Multiple types, discovery |
| `RedisFactory` | At queue creation | Yes | Multiple types, discovery |
| `UnifiedFactory` | At queue creation | Yes | Config-driven, backend switching |

**Example:**
```go
import "github.com/ivanzzeth/go-universal-data-containers/queue"

// Generic factory (single type)
factory := queue.NewMemoryFactory(queue.NewJsonMessage([]byte{}))
q, _ := factory.GetOrCreateSafe("orders")
q.Enqueue(ctx, []byte("order data"))

// Non-generic factory (multiple types + discovery)
factory, _ := queue.NewMemoryQueueFactory()
bytesQueue, _ := queue.MemoryGetOrCreateSafe[[]byte](factory, "bytes-queue", queue.NewJsonMessage([]byte{}))
ordersQueue, _ := queue.MemoryGetOrCreateSafe[Order](factory, "orders-queue", queue.NewJsonMessage(Order{}))
queues, _ := factory.DiscoverQueues(ctx, "orders-*")

// Configuration-driven
config := queue.UnifiedQueueConfig{
    Type:              queue.QueueTypeRedis,
    MaxSize:           1000,
    MaxHandleFailures: 3,
    RedisClient:       rdb,
}
factory, _ := queue.NewUnifiedFactory(config)
q, _ := queue.GetOrCreateSafe[[]byte](factory, "orders", queue.NewJsonMessage([]byte{}))
```

See [queue/README.md](queue/README.md) for detailed documentation.

---

### 2. State (`state`)

Comprehensive state management with multiple storage backends, caching, and snapshots.

**Features:**
- Generic state interface with ID composition
- Memory, Redis, and GORM storage backends
- Two-tier caching (memory + persistence)
- State snapshots and rollback
- Distributed locking integration
- Prometheus metrics

**Core Types:**
- `State` - Base state interface
- `Storage` - Generic storage interface
- `StorageFactory` - Creates and manages Storage instances
- `Finalizer` - Two-tier storage management (cache + persist)
- `Registry` - State type registration and factory
- `StateContainer[T]` - Convenient wrapper for state operations

**Example:**
```go
import "github.com/ivanzzeth/go-universal-data-containers/state"

// Define your state
type UserState struct {
    state.BaseState
    UserID   string
    Username string
    Email    string
}

func (s *UserState) StateName() string { return "user" }
func (s *UserState) StateIDComponents() []any { return []any{s.UserID} }

// Register state
registry := state.NewRegistry()
registry.RegisterState(&UserState{})

// Create storage
storage := state.NewMemoryStorage()

// Use StateContainer for convenient operations
container := state.NewStateContainer[*UserState](registry, finalizer)
user, _ := container.Get(ctx, "user123")
user.Username = "newname"
container.Save(ctx, user)
```

---

### 3. Locker (`locker`)

Distributed and local synchronization locking mechanisms.

**Features:**
- Memory-based locks using `sync.Mutex` / `sync.RWMutex`
- Redis-based distributed locks using redsync
- Generator pattern for creating lockers by name

**Interfaces:**
- `SyncLocker` - Basic mutual exclusion lock
- `SyncRWLocker` - Read-write lock
- `SyncLockerGenerator` / `SyncRWLockerGenerator` - Factory interfaces

**Example:**
```go
import "github.com/ivanzzeth/go-universal-data-containers/locker"

// Memory locker
generator := locker.NewMemoryLockerGenerator()
lock := generator.Get("my-resource")
lock.Lock(ctx)
defer lock.Unlock(ctx)

// Redis distributed locker
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
generator := locker.NewRedisLockerGenerator(rdb)
lock := generator.Get("distributed-resource")
lock.Lock(ctx)
defer lock.Unlock(ctx)
```

---

### 4. Message (`message`)

Generic message abstraction for queue and network communication with versioning.

**Features:**
- Generic message interface
- JSON serialization
- Semantic versioning for backward compatibility
- Metadata support
- ID generation utilities

**Example:**
```go
import "github.com/ivanzzeth/go-universal-data-containers/message"

// Create message
msg := message.NewJsonMessage(MyData{})
msg.SetData(MyData{ID: "123", Value: "test"})
msg.SetVersion(common.MustNewSemanticVersion("v1.0.0"))

// Serialize
data, _ := msg.Pack()

// Deserialize
msg2 := message.NewJsonMessage(MyData{})
msg2.Unpack(data)
```

---

### 5. Time (`time`)

Distributed ticker mechanism using queue and state management.

**Features:**
- Queue-backed distributed ticker
- State storage for tracking last tick time
- Distributes ticks across multiple instances
- Configurable interval and partition support

**Example:**
```go
import udctime "github.com/ivanzzeth/go-universal-data-containers/time"

ticker, _ := udctime.NewDistributedTicker(
    "partition1",
    "my-ticker",
    time.Minute,
    registry,
    storage,
    lockerGenerator,
    queueFactory,
)

for t := range ticker.Tick() {
    fmt.Println("Tick at:", t)
}
```

---

### 6. Metrics (`metrics`)

Prometheus metrics for monitoring state and queue operations.

**Queue Metrics:**
- `queue_depth` - Current message count
- `queue_enqueue_total` / `queue_dequeue_total` - Operation counters
- `queue_handle_duration_seconds` - Handler latency histogram
- `queue_dlq_messages_total` - DLQ message counter

**State Metrics:**
- `state_storage_operation_total` - Storage operation counter
- `state_storage_operation_duration_seconds` - Operation latency
- `state_snapshot_operation_total` - Snapshot operation counter

---

### 7. Common (`common`)

Shared utilities and types.

- `SemanticVersion` - Semantic versioning type with comparison
- `ErrNotImplemented` - Standard error

---

### 8. Utils (`utils`)

General utility functions.

- `SetNestedField` - Dynamically set nested struct fields via reflection

## Architecture

```
[Your Application]
    |
    | Uses
    v
[UDC Interfaces] (Queue, State, Locker, etc.)
    ^
    | Implements
    |
[Backend Implementations]
    ├── Memory (for testing)
    ├── Redis (production)
    └── GORM/PostgreSQL (production)
```

## Testing vs Production

```go
// In test (memory backend)
func TestOrderProcessing(t *testing.T) {
    factory, _ := queue.NewMemoryQueueFactory()
    testQueue, _ := queue.MemoryGetOrCreateSafe[[]byte](factory, "test-orders", queue.NewJsonMessage([]byte{}))
    // Test logic...
}

// In production (Redis backend)
func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "redis-prod:6379"})
    factory, _ := queue.NewRedisFactory(rdb)
    prodQueue, _ := queue.RedisGetOrCreateSafe[[]byte](factory, "prod-orders", queue.NewJsonMessage([]byte{}))
    // Production logic...
}
```

## Benefits

1. **Development Speed**: Rapid prototyping with in-memory backends
2. **Testing Reliability**: No external dependencies in unit tests
3. **Production Readiness**: Switch to scalable backends with config changes
4. **Code Consistency**: Same application logic across environments
5. **Vendor Neutral**: Avoid lock-in to specific technologies
6. **Observability**: Built-in Prometheus metrics

## License

MIT License
