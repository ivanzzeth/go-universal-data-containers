# Universal Data Containers (UDC)

## Overview
**Universal Data Containers (UDC)** is a Go library that provides standardized interfaces for common data structures with multiple backend implementations. It enables developers to write environment-agnostic code that can seamlessly transition between in-memory (for testing) and production-ready backends (Redis, Kafka, PostgreSQL, etc.) without changing application logic.

## Core Principles

1. **Standardized Interfaces**: Well-defined contracts for each data structure
2. **Multiple Implementations**: Memory, Redis, Kafka, and other backends
3. **Zero-Cost Abstraction**: Minimal performance overhead
4. **Thread Safety**: All implementations are concurrency-safe
5. **Pluggable Architecture**: Easy to add new backends

## Current Implementations

### 1. Queue (Already Implemented)
```go
type Queue interface {
    Kind() Kind
    MaxSize() int
    Enqueue([]byte) error
    Dequeue() ([]byte, error)
    Subscribe(Handler)
}

type RecoverableQueue interface {
    Queue
    Recover([]byte) error
}
```

### 2. Cache (Planned)
```go
type Cache interface {
    Get(key string) ([]byte, error)
    Set(key string, value []byte, ttl time.Duration) error
    Delete(key string) error
    Exists(key string) (bool, error)
}
```

### 3. Future Structures
- PubSub
- Key-Value Store
- Priority Queue
- Rate Limiter

## Usage Example

### Application Code (Environment-Agnostic)
```go
// Initialize factory (could be memory, redis, kafka)
factory := udc.NewMemoryFactory() 

// Get queue - same interface regardless of backend
orderQueue := factory.GetQueue("orders", 1000)

// Get cache - same interface
productCache := factory.GetCache("products", 10*time.Minute)
```

### Testing vs Production
```go
// In test (memory backend)
func TestOrderProcessing(t *testing.T) {
    factory := udc.NewMemoryFactory()
    testQueue := factory.GetQueue("test-orders", 100)
    // Test logic...
}

// In production (Redis backend)
func main() {
    factory := udc.NewRedisFactory(&RedisConfig{
        Addr: "redis-prod:6379",
    })
    prodQueue := factory.GetQueue("prod-orders", 10000)
    // Production logic...
}
```

## Architecture

```
[Your Application]
    |
    | Uses
    v
[UDC Interfaces] (Queue, Cache, etc.)
    ^
    | Implements
    |
[Backend Implementations]
    ├── Memory (for testing)
    ├── Redis (production)
    ├── Kafka (production)
    └── PostgreSQL (production)
```

## Benefits

1. **Development Speed**: Rapid prototyping with in-memory backends
2. **Testing Reliability**: No external dependencies in unit tests
3. **Production Readiness**: Switch to scalable backends with config changes
4. **Code Consistency**: Same application logic across environments
5. **Vendor Neutral**: Avoid lock-in to specific technologies

## Roadmap

1. **v0.1**: Queue implementation (Memory + Redis)
2. **v0.2**: Cache implementation (Memory + Redis)
3. **v0.3**: PubSub implementation (Memory + Redis + Kafka)
4. **v1.0**: Stable API with multiple production-ready backends

## Contribution Guidelines
We welcome implementations for additional:
- Data structures
- Backend providers
- Language bindings

## Example Configurations

```yaml
# development.yaml
data_containers:
  default_backend: memory
  queues:
    orders:
      max_size: 1000
  caches:
    products:
      ttl: 10m

# production.yaml
data_containers:
  default_backend: redis
  redis:
    addr: "redis-cluster:6379"
  queues:
    orders:
      max_size: 10000
      backend: kafka
      kafka_topic: "orders"
  caches:
    products:
      ttl: 1h
```

This library follows the philosophy: "Write once, run anywhere" - from local development to distributed production systems.