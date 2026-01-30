package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// QueueType represents the storage backend type
type QueueType string

const (
	QueueTypeMemory QueueType = "memory"
	QueueTypeRedis  QueueType = "redis"
	// Future: QueueTypeKafka, QueueTypeSQS, etc.
)

// builtinQueueTypes contains all built-in queue types that are directly supported
var builtinQueueTypes = map[QueueType]bool{
	QueueTypeMemory: true,
	QueueTypeRedis:  true,
}

// QueueCreator is a function that creates a queue from raw JSON config.
// Used for registering custom queue backend implementations.
type QueueCreator[T any] func(ctx context.Context, name string, defaultMsg Message[T], rawConfig json.RawMessage, options ...Option) (Queue[T], error)

// QueueConfigValidator validates backend-specific configuration
type QueueConfigValidator interface {
	Validate(rawConfig json.RawMessage) error
}

// UnifiedQueueConfig is the top-level configuration for creating queues
type UnifiedQueueConfig struct {
	// Type determines which backend to use: "memory", "redis", etc.
	Type QueueType `json:"type"`

	// Common options that apply to all backends
	MaxSize            int  `json:"max_size,omitempty"`             // -1 for unlimited
	MaxHandleFailures  int  `json:"max_handle_failures,omitempty"`  // Max failures before DLQ
	ConsumerCount      int  `json:"consumer_count,omitempty"`       // Number of concurrent consumers
	CallbackParallel   bool `json:"callback_parallel,omitempty"`    // Enable parallel callback execution
	UnlimitedCapacity  int  `json:"unlimited_capacity,omitempty"`   // Buffer size when unlimited (Memory only)
	RetryQueueCapacity int  `json:"retry_queue_capacity,omitempty"` // Retry queue buffer size

	// BackendConfig contains backend-specific configuration as raw JSON
	// Parsed according to Type:
	// - "memory": MemoryQueueConfig (currently empty, reserved for future)
	// - "redis": RedisQueueConfig
	BackendConfig json.RawMessage `json:"backend_config,omitempty"`

	// RedisClient allows using an existing Redis client instead of creating a new one.
	// When set, BackendConfig is ignored for Redis queues.
	// This field is not serializable to JSON.
	RedisClient redis.Cmdable `json:"-"`
}

// MemoryQueueConfig is the backend-specific config for memory queues
// Currently empty but reserved for future extensions
type MemoryQueueConfig struct {
	// Reserved for future memory-specific options
}

// RedisQueueConfig is the backend-specific config for Redis queues
type RedisQueueConfig struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string `json:"addr"`
	// Password is the Redis password (optional)
	Password string `json:"password,omitempty"`
	// DB is the Redis database number (optional, default 0)
	DB int `json:"db,omitempty"`
}

// Global registry for custom queue creators and validators.
// Built-in types (memory, redis) are handled directly without registration.
var (
	creatorRegistry   = make(map[QueueType]any)
	validatorRegistry = make(map[QueueType]QueueConfigValidator)
	registryMu        sync.RWMutex
)

// RegisterQueueCreator registers a creator function for a custom queue type.
// Built-in types (memory, redis) don't need registration.
//
// Note: Due to Go's generics limitations, creators are stored as any
// and type-asserted at runtime.
func RegisterQueueCreator[T any](queueType QueueType, creator QueueCreator[T]) {
	registryMu.Lock()
	defer registryMu.Unlock()
	creatorRegistry[queueType] = creator
}

// RegisterQueueValidator registers a config validator for a custom queue type.
// Built-in types have built-in validation.
func RegisterQueueValidator(queueType QueueType, validator QueueConfigValidator) {
	registryMu.Lock()
	defer registryMu.Unlock()
	validatorRegistry[queueType] = validator
}

// GetRegisteredQueueTypes returns all available queue types (built-in + registered)
func GetRegisteredQueueTypes() []QueueType {
	registryMu.RLock()
	defer registryMu.RUnlock()

	types := make([]QueueType, 0, len(builtinQueueTypes)+len(creatorRegistry))

	// Add built-in types
	for t := range builtinQueueTypes {
		types = append(types, t)
	}

	// Add registered custom types
	for t := range creatorRegistry {
		if !builtinQueueTypes[t] {
			types = append(types, t)
		}
	}

	return types
}

// IsQueueTypeSupported checks if a queue type is supported (built-in or registered)
func IsQueueTypeSupported(queueType QueueType) bool {
	if builtinQueueTypes[queueType] {
		return true
	}

	registryMu.RLock()
	defer registryMu.RUnlock()
	_, ok := creatorRegistry[queueType]
	return ok
}

// UnifiedFactory creates queues based on configuration.
// It is type-agnostic and can create queues of any message type.
// Built-in types (memory, redis) are created directly without registration.
// Custom types use the registry.
type UnifiedFactory struct {
	config UnifiedQueueConfig

	// cache stores created queues by name for reuse
	// Key format: "name:type" where type is the reflect type name
	cache   map[string]any
	cacheMu sync.Mutex

	// redisClient is cached for reuse across multiple queue creations
	redisClient redis.Cmdable
}

// NewUnifiedFactory creates a new unified queue factory.
// No registration required for built-in types (memory, redis).
// The factory can create queues of any message type using GetOrCreateSafe.
func NewUnifiedFactory(config UnifiedQueueConfig) (*UnifiedFactory, error) {
	// Validate configuration
	if err := validateUnifiedConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	f := &UnifiedFactory{
		config: config,
		cache:  make(map[string]any),
	}

	// Pre-create Redis client if needed
	if config.Type == QueueTypeRedis {
		if config.RedisClient != nil {
			f.redisClient = config.RedisClient
		} else {
			var cfg RedisQueueConfig
			if err := json.Unmarshal(config.BackendConfig, &cfg); err != nil {
				return nil, fmt.Errorf("failed to parse redis config: %w", err)
			}

			client := redis.NewClient(&redis.Options{
				Addr:     cfg.Addr,
				Password: cfg.Password,
				DB:       cfg.DB,
			})

			// Test connection
			ctx := context.Background()
			if err := client.Ping(ctx).Err(); err != nil {
				return nil, fmt.Errorf("failed to connect to redis at %s: %w", cfg.Addr, err)
			}

			f.redisClient = client
		}
	}

	return f, nil
}

// validateUnifiedConfig validates the queue configuration
func validateUnifiedConfig(config UnifiedQueueConfig) error {
	if config.Type == "" {
		return fmt.Errorf("queue type is required")
	}

	// Check if type is supported
	if !IsQueueTypeSupported(config.Type) {
		return fmt.Errorf("unknown queue type %q, available types: %v", config.Type, GetRegisteredQueueTypes())
	}

	// Validate built-in types
	if builtinQueueTypes[config.Type] {
		return validateBuiltinConfig(config)
	}

	// Validate custom types using registered validator
	registryMu.RLock()
	validator, hasValidator := validatorRegistry[config.Type]
	registryMu.RUnlock()

	if hasValidator {
		if err := validator.Validate(config.BackendConfig); err != nil {
			return fmt.Errorf("invalid backend config for %q: %w", config.Type, err)
		}
	}

	return nil
}

// validateBuiltinConfig validates configuration for built-in queue types
func validateBuiltinConfig(config UnifiedQueueConfig) error {
	switch config.Type {
	case QueueTypeMemory:
		// Memory queues don't require any specific config
		return nil

	case QueueTypeRedis:
		// If RedisClient is provided, no config validation needed
		if config.RedisClient != nil {
			return nil
		}

		// Otherwise, validate BackendConfig
		if len(config.BackendConfig) == 0 {
			return fmt.Errorf("redis backend_config is required when RedisClient is not provided")
		}

		var cfg RedisQueueConfig
		if err := json.Unmarshal(config.BackendConfig, &cfg); err != nil {
			return fmt.Errorf("invalid redis config JSON: %w", err)
		}

		if cfg.Addr == "" {
			return fmt.Errorf("redis addr is required")
		}

		return nil

	default:
		return fmt.Errorf("unknown built-in queue type %q", config.Type)
	}
}

// GetOrCreateSafe creates or returns a cached SafeQueue with the given name and message type.
// This is a generic function that allows creating queues of different types from the same factory.
//
// Example:
//
//	factory, _ := queue.NewUnifiedFactory(config)
//	bytesQueue, _ := queue.GetOrCreateSafe[[]byte](factory, "bytes-queue", queue.NewJsonMessage([]byte{}))
//	myTypeQueue, _ := queue.GetOrCreateSafe[MyType](factory, "mytype-queue", queue.NewJsonMessage(MyType{}))
func GetOrCreateSafe[T any](f *UnifiedFactory, name string, defaultMsg Message[T], options ...Option) (SafeQueue[T], error) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	// Generate cache key using name (type is implicit in the generic parameter)
	cacheKey := name

	// Return cached queue if exists and type matches
	if cached, ok := f.cache[cacheKey]; ok {
		if q, ok := cached.(SafeQueue[T]); ok {
			return q, nil
		}
		// Type mismatch - queue exists with different type
		return nil, fmt.Errorf("queue %q already exists with a different message type", name)
	}

	// Build options from config
	configOptions := buildOptions(f.config)
	allOptions := append(configOptions, options...)

	// Create queue based on type
	var q Queue[T]
	var err error

	if builtinQueueTypes[f.config.Type] {
		q, err = createBuiltinQueue(f, name, defaultMsg, allOptions...)
	} else {
		q, err = createCustomQueue[T](f, name, defaultMsg, allOptions...)
	}

	if err != nil {
		return nil, err
	}

	// Wrap in SimpleQueue
	safeQ, err := NewSimpleQueue(q)
	if err != nil {
		return nil, fmt.Errorf("failed to create SimpleQueue: %w", err)
	}

	// Cache and return
	f.cache[cacheKey] = safeQ
	return safeQ, nil
}

// GetOrCreate creates or returns a cached Queue with the given name and message type.
// This is a convenience wrapper around GetOrCreateSafe.
func GetOrCreate[T any](f *UnifiedFactory, name string, defaultMsg Message[T], options ...Option) (Queue[T], error) {
	return GetOrCreateSafe(f, name, defaultMsg, options...)
}

// createBuiltinQueue creates a queue for built-in types directly
func createBuiltinQueue[T any](f *UnifiedFactory, name string, defaultMsg Message[T], options ...Option) (Queue[T], error) {
	switch f.config.Type {
	case QueueTypeMemory:
		return NewMemoryQueue(name, defaultMsg, options...)

	case QueueTypeRedis:
		return NewRedisQueue(f.redisClient, name, defaultMsg, options...)

	default:
		return nil, fmt.Errorf("unknown built-in queue type %q", f.config.Type)
	}
}

// createCustomQueue creates a queue using registered creator
func createCustomQueue[T any](f *UnifiedFactory, name string, defaultMsg Message[T], options ...Option) (Queue[T], error) {
	registryMu.RLock()
	creatorIface, ok := creatorRegistry[f.config.Type]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown queue type %q", f.config.Type)
	}

	creator, ok := creatorIface.(QueueCreator[T])
	if !ok {
		return nil, fmt.Errorf("creator type mismatch for queue type %q, ensure RegisterQueueCreator was called with matching type parameter", f.config.Type)
	}

	ctx := context.Background()
	q, err := creator(ctx, name, defaultMsg, f.config.BackendConfig, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create %q queue: %w", f.config.Type, err)
	}

	return q, nil
}

// buildOptions converts config fields to Option functions
func buildOptions(config UnifiedQueueConfig) []Option {
	var opts []Option

	if config.MaxSize != 0 {
		opts = append(opts, WithMaxSize(config.MaxSize))
	}
	if config.MaxHandleFailures != 0 {
		opts = append(opts, WithMaxHandleFailures(config.MaxHandleFailures))
	}
	if config.ConsumerCount != 0 {
		opts = append(opts, WithConsumerCount(config.ConsumerCount))
	}
	if config.CallbackParallel {
		opts = append(opts, WithCallbackParallelExecution(true))
	}
	if config.UnlimitedCapacity != 0 {
		opts = append(opts, WithUnlimitedCapacity(config.UnlimitedCapacity))
	}
	if config.RetryQueueCapacity != 0 {
		opts = append(opts, WithRetryQueueCapacity(config.RetryQueueCapacity))
	}

	return opts
}

// Config returns a copy of the factory's configuration.
// The returned config is safe to modify without affecting the factory.
func (f *UnifiedFactory) Config() UnifiedQueueConfig {
	// Return a copy to prevent external modification
	configCopy := f.config

	// Deep copy BackendConfig slice to prevent modification
	if f.config.BackendConfig != nil {
		configCopy.BackendConfig = make(json.RawMessage, len(f.config.BackendConfig))
		copy(configCopy.BackendConfig, f.config.BackendConfig)
	}

	return configCopy
}

// Deprecated: ValidateUnifiedConfig is deprecated, use NewUnifiedFactory which validates internally.
// Kept for backward compatibility.
func ValidateUnifiedConfig(config UnifiedQueueConfig) error {
	return validateUnifiedConfig(config)
}

// Deprecated: IsQueueTypeRegistered is deprecated, use IsQueueTypeSupported instead.
// Kept for backward compatibility.
func IsQueueTypeRegistered(queueType QueueType) bool {
	return IsQueueTypeSupported(queueType)
}

// =============================================================================
// Typed Factory (for backward compatibility and convenience)
// =============================================================================

// TypedFactory is a typed wrapper around UnifiedFactory for convenience.
// Use this when you only need queues of a single message type.
type TypedFactory[T any] struct {
	factory    *UnifiedFactory
	defaultMsg Message[T]
}

// NewTypedFactory creates a typed factory wrapper.
// This is a convenience for when you only need queues of one message type.
//
// Example:
//
//	factory, _ := queue.NewTypedFactory(config, queue.NewJsonMessage([]byte{}))
//	q, _ := factory.GetOrCreateSafe("my-queue")
func NewTypedFactory[T any](config UnifiedQueueConfig, defaultMsg Message[T]) (*TypedFactory[T], error) {
	f, err := NewUnifiedFactory(config)
	if err != nil {
		return nil, err
	}

	return &TypedFactory[T]{
		factory:    f,
		defaultMsg: defaultMsg,
	}, nil
}

// GetOrCreate creates or returns a cached queue with the given name
func (f *TypedFactory[T]) GetOrCreate(name string, options ...Option) (Queue[T], error) {
	return GetOrCreateSafe(f.factory, name, f.defaultMsg, options...)
}

// GetOrCreateSafe creates or returns a cached SafeQueue with the given name
func (f *TypedFactory[T]) GetOrCreateSafe(name string, options ...Option) (SafeQueue[T], error) {
	return GetOrCreateSafe(f.factory, name, f.defaultMsg, options...)
}

// Config returns the factory's configuration
func (f *TypedFactory[T]) Config() UnifiedQueueConfig {
	return f.factory.Config()
}

// Factory returns the underlying UnifiedFactory
func (f *TypedFactory[T]) Factory() *UnifiedFactory {
	return f.factory
}
