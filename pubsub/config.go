package pubsub

import "time"

// Backend identifies the pubsub backend type
type Backend string

const (
	// BackendMemory uses in-memory channels for pub/sub (for testing/development)
	BackendMemory Backend = "memory"

	// BackendRedis uses Redis Pub/Sub for distributed messaging
	BackendRedis Backend = "redis"
)

// Config configures a PubSub instance
type Config struct {
	// Backend specifies which backend to use
	Backend Backend

	// BufferSize is the channel buffer size for subscribers (default: 100)
	BufferSize int

	// OnFull specifies behavior when subscriber buffer is full
	OnFull OverflowPolicy

	// BatchSizeMax is the maximum number of messages per pipeline batch (default: 1000)
	// Only applicable to backends that support pipelining (e.g., Redis)
	BatchSizeMax int

	// Options contains backend-specific configuration
	Options map[string]any
}

const (
	// DefaultBatchSizeMax is the default maximum batch size for pipelining
	DefaultBatchSizeMax = 1000
)

// OverflowPolicy defines behavior when subscriber buffer is full
type OverflowPolicy int

const (
	// OverflowDrop drops the message when buffer is full (lossy, non-blocking)
	OverflowDrop OverflowPolicy = iota

	// OverflowBlock blocks until buffer has space (lossless, may slow publisher)
	OverflowBlock
)

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig() Config {
	return Config{
		Backend:      BackendMemory,
		BufferSize:   100,
		OnFull:       OverflowDrop,
		BatchSizeMax: DefaultBatchSizeMax,
		Options:      make(map[string]any),
	}
}

// RedisOptions contains Redis-specific configuration
type RedisOptions struct {
	// Addr is the Redis server address (default: "localhost:6379")
	Addr string

	// Password is the Redis password (optional)
	Password string

	// DB is the Redis database number (default: 0)
	DB int

	// Prefix is the key prefix for pubsub channels (default: "pubsub:")
	Prefix string

	// PoolSize is the maximum number of socket connections (default: 10)
	PoolSize int

	// ReadTimeout is the timeout for reading from Redis (default: 3s)
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for writing to Redis (default: 3s)
	WriteTimeout time.Duration
}

// DefaultRedisOptions returns RedisOptions with sensible defaults
func DefaultRedisOptions() RedisOptions {
	return RedisOptions{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		Prefix:       "pubsub:",
		PoolSize:     10,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// ToMap converts RedisOptions to a map for Config.Options
func (o RedisOptions) ToMap() map[string]any {
	return map[string]any{
		"addr":          o.Addr,
		"password":      o.Password,
		"db":            o.DB,
		"prefix":        o.Prefix,
		"pool_size":     o.PoolSize,
		"read_timeout":  o.ReadTimeout,
		"write_timeout": o.WriteTimeout,
	}
}

// RedisOptionsFromMap extracts RedisOptions from a map
func RedisOptionsFromMap(m map[string]any) RedisOptions {
	opts := DefaultRedisOptions()

	if v, ok := m["addr"].(string); ok && v != "" {
		opts.Addr = v
	}
	if v, ok := m["password"].(string); ok {
		opts.Password = v
	}
	if v, ok := m["db"].(int); ok {
		opts.DB = v
	}
	if v, ok := m["prefix"].(string); ok && v != "" {
		opts.Prefix = v
	}
	if v, ok := m["pool_size"].(int); ok && v > 0 {
		opts.PoolSize = v
	}
	if v, ok := m["read_timeout"].(time.Duration); ok && v > 0 {
		opts.ReadTimeout = v
	}
	if v, ok := m["write_timeout"].(time.Duration); ok && v > 0 {
		opts.WriteTimeout = v
	}

	return opts
}
