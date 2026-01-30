package queue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedFactory_Memory(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
		ConsumerCount:     2,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)
	require.NotNil(t, factory)

	// Create queue using generic function
	q, err := GetOrCreateSafe[[]byte](factory, "test-memory-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, q)

	// Test basic operations
	ctx := context.Background()
	err = q.Enqueue(ctx, []byte("test-data"))
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-data"), msg.Data())

	// Verify queue is cached
	q2, err := GetOrCreateSafe[[]byte](factory, "test-memory-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	assert.Equal(t, q, q2)

	q.Close()
}

func TestUnifiedFactory_Redis(t *testing.T) {
	// Start miniredis
	s := miniredis.RunT(t)

	backendConfig, _ := json.Marshal(RedisQueueConfig{
		Addr: s.Addr(),
	})

	config := UnifiedQueueConfig{
		Type:              QueueTypeRedis,
		MaxSize:           100,
		MaxHandleFailures: 3,
		BackendConfig:     backendConfig,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)
	require.NotNil(t, factory)

	// Create queue using generic function
	q, err := GetOrCreateSafe[[]byte](factory, "test-redis-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, q)

	// Test basic operations
	ctx := context.Background()
	err = q.Enqueue(ctx, []byte("test-data"))
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-data"), msg.Data())

	q.Close()
}

func TestUnifiedFactory_RedisWithExistingClient(t *testing.T) {
	// Start miniredis
	s := miniredis.RunT(t)

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	config := UnifiedQueueConfig{
		Type:        QueueTypeRedis,
		MaxSize:     100,
		RedisClient: rdb,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	q, err := GetOrCreateSafe[[]byte](factory, "test-redis-client-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, q)

	// Test basic operations
	ctx := context.Background()
	err = q.Enqueue(ctx, []byte("test-data"))
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-data"), msg.Data())

	q.Close()
}

func TestUnifiedFactory_FromJSON(t *testing.T) {
	// Test parsing from JSON
	configJSON := `{
		"type": "memory",
		"max_size": 1000,
		"max_handle_failures": 5,
		"consumer_count": 2,
		"callback_parallel": true
	}`

	var config UnifiedQueueConfig
	err := json.Unmarshal([]byte(configJSON), &config)
	require.NoError(t, err)

	assert.Equal(t, QueueTypeMemory, config.Type)
	assert.Equal(t, 1000, config.MaxSize)
	assert.Equal(t, 5, config.MaxHandleFailures)
	assert.Equal(t, 2, config.ConsumerCount)
	assert.True(t, config.CallbackParallel)

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	q, err := GetOrCreateSafe[[]byte](factory, "json-config-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, q)

	// Verify config was applied
	assert.Equal(t, 1000, q.MaxSize())
	assert.Equal(t, 5, q.MaxHandleFailures())

	q.Close()
}

func TestUnifiedFactory_RedisFromJSON(t *testing.T) {
	// Start miniredis
	s := miniredis.RunT(t)

	// Test parsing from JSON with nested backend config
	configJSON := `{
		"type": "redis",
		"max_size": 10000,
		"max_handle_failures": 3,
		"backend_config": {
			"addr": "` + s.Addr() + `",
			"password": "",
			"db": 0
		}
	}`

	var config UnifiedQueueConfig
	err := json.Unmarshal([]byte(configJSON), &config)
	require.NoError(t, err)

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	q, err := GetOrCreateSafe[[]byte](factory, "redis-json-config-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, q)

	q.Close()
}

func TestUnifiedFactory_ValidationErrors(t *testing.T) {
	tests := []struct {
		name        string
		config      UnifiedQueueConfig
		expectedErr string
	}{
		{
			name:        "empty type",
			config:      UnifiedQueueConfig{},
			expectedErr: "queue type is required",
		},
		{
			name: "unknown type",
			config: UnifiedQueueConfig{
				Type: "unknown",
			},
			expectedErr: "unknown queue type",
		},
		{
			name: "redis without backend config",
			config: UnifiedQueueConfig{
				Type: QueueTypeRedis,
			},
			expectedErr: "redis backend_config is required",
		},
		{
			name: "redis without addr",
			config: UnifiedQueueConfig{
				Type:          QueueTypeRedis,
				BackendConfig: json.RawMessage(`{"password": "secret"}`),
			},
			expectedErr: "redis addr is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewUnifiedFactory(tt.config)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestUnifiedFactory_Subscribe(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	q, err := GetOrCreateSafe[[]byte](factory, "subscribe-test-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	ctx := context.Background()
	received := make(chan []byte, 1)

	q.Subscribe(ctx, func(ctx context.Context, msg Message[[]byte]) error {
		received <- msg.Data()
		return nil
	})

	// Give subscription time to register
	time.Sleep(20 * time.Millisecond)

	err = q.Enqueue(ctx, []byte("subscribe-test"))
	require.NoError(t, err)

	select {
	case data := <-received:
		assert.Equal(t, []byte("subscribe-test"), data)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	q.Close()
}

func TestGetRegisteredQueueTypes(t *testing.T) {
	// Built-in types are always available
	types := GetRegisteredQueueTypes()
	assert.Contains(t, types, QueueTypeMemory)
	assert.Contains(t, types, QueueTypeRedis)
}

func TestIsQueueTypeSupported(t *testing.T) {
	// Built-in types are always supported
	assert.True(t, IsQueueTypeSupported(QueueTypeMemory))
	assert.True(t, IsQueueTypeSupported(QueueTypeRedis))
	assert.False(t, IsQueueTypeSupported("nonexistent"))
}

// TestIsQueueTypeRegistered tests backward compatibility
func TestIsQueueTypeRegistered(t *testing.T) {
	// Built-in types should return true via deprecated function
	assert.True(t, IsQueueTypeRegistered(QueueTypeMemory))
	assert.True(t, IsQueueTypeRegistered(QueueTypeRedis))
	assert.False(t, IsQueueTypeRegistered("nonexistent"))
}

func TestUnifiedQueueConfig_JSONMarshal(t *testing.T) {
	backendConfig, _ := json.Marshal(RedisQueueConfig{
		Addr:     "localhost:6379",
		Password: "secret",
		DB:       1,
	})

	config := UnifiedQueueConfig{
		Type:               QueueTypeRedis,
		MaxSize:            1000,
		MaxHandleFailures:  5,
		ConsumerCount:      2,
		CallbackParallel:   true,
		UnlimitedCapacity:  500000,
		RetryQueueCapacity: 5000,
		BackendConfig:      backendConfig,
	}

	data, err := json.Marshal(config)
	require.NoError(t, err)

	var parsed UnifiedQueueConfig
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, config.Type, parsed.Type)
	assert.Equal(t, config.MaxSize, parsed.MaxSize)
	assert.Equal(t, config.MaxHandleFailures, parsed.MaxHandleFailures)
	assert.Equal(t, config.ConsumerCount, parsed.ConsumerCount)
	assert.Equal(t, config.CallbackParallel, parsed.CallbackParallel)
	assert.Equal(t, config.UnlimitedCapacity, parsed.UnlimitedCapacity)
	assert.Equal(t, config.RetryQueueCapacity, parsed.RetryQueueCapacity)

	// Parse backend config
	var redisConfig RedisQueueConfig
	err = json.Unmarshal(parsed.BackendConfig, &redisConfig)
	require.NoError(t, err)
	assert.Equal(t, "localhost:6379", redisConfig.Addr)
	assert.Equal(t, "secret", redisConfig.Password)
	assert.Equal(t, 1, redisConfig.DB)
}

// TestUnifiedFactory_MultipleTypes tests creating queues of different types from the same factory
type customMessage struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func TestUnifiedFactory_MultipleTypes(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
	}

	// Create a single factory
	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a []byte queue
	bytesQueue, err := GetOrCreateSafe[[]byte](factory, "bytes-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	err = bytesQueue.Enqueue(ctx, []byte("hello"))
	require.NoError(t, err)

	bytesMsg, err := bytesQueue.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), bytesMsg.Data())

	// Create a custom type queue from the same factory
	customQueue, err := GetOrCreateSafe[customMessage](factory, "custom-queue", NewJsonMessage(customMessage{}))
	require.NoError(t, err)

	err = customQueue.Enqueue(ctx, customMessage{ID: "123", Data: "test"})
	require.NoError(t, err)

	customMsg, err := customQueue.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "123", customMsg.Data().ID)
	assert.Equal(t, "test", customMsg.Data().Data)

	bytesQueue.Close()
	customQueue.Close()
}

// TestUnifiedFactory_TypeMismatch tests that accessing a queue with wrong type returns error
func TestUnifiedFactory_TypeMismatch(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	// Create a []byte queue
	_, err = GetOrCreateSafe[[]byte](factory, "typed-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	// Try to get the same queue with a different type
	_, err = GetOrCreateSafe[customMessage](factory, "typed-queue", NewJsonMessage(customMessage{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists with a different message type")
}

// TestTypedFactory tests the TypedFactory convenience wrapper
func TestTypedFactory(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
	}

	// Create a typed factory for []byte
	factory, err := NewTypedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)

	// Create queue - no need to specify type
	q, err := factory.GetOrCreateSafe("typed-queue")
	require.NoError(t, err)

	ctx := context.Background()
	err = q.Enqueue(ctx, []byte("test-data"))
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-data"), msg.Data())

	// Verify config access
	assert.Equal(t, QueueTypeMemory, factory.Config().Type)

	// Verify underlying factory access
	assert.NotNil(t, factory.Factory())

	q.Close()
}

// TestTypedFactory_CustomType tests TypedFactory with custom message type
func TestTypedFactory_CustomType(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewTypedFactory(config, NewJsonMessage(customMessage{}))
	require.NoError(t, err)

	q, err := factory.GetOrCreateSafe("custom-typed-queue")
	require.NoError(t, err)

	ctx := context.Background()
	err = q.Enqueue(ctx, customMessage{ID: "456", Data: "custom"})
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "456", msg.Data().ID)
	assert.Equal(t, "custom", msg.Data().Data)

	q.Close()
}

// TestUnifiedFactory_ConcurrentAccess tests that the factory is thread-safe
func TestUnifiedFactory_ConcurrentAccess(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	const numGoroutines = 10
	const numQueuesPerGoroutine = 5

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*numQueuesPerGoroutine)
	queueChan := make(chan SafeQueue[[]byte], numGoroutines*numQueuesPerGoroutine)

	// Concurrently create queues from the same factory
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numQueuesPerGoroutine; j++ {
				// Use same queue name to test caching under concurrent access
				queueName := "concurrent-queue-" + string(rune('a'+j%3))
				q, err := GetOrCreateSafe[[]byte](factory, queueName, NewJsonMessage([]byte{}))
				if err != nil {
					errChan <- err
					continue
				}
				queueChan <- q
			}
		}(i)
	}

	wg.Wait()
	close(errChan)
	close(queueChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify all queues were created successfully
	queues := make([]SafeQueue[[]byte], 0)
	for q := range queueChan {
		queues = append(queues, q)
	}
	assert.Equal(t, numGoroutines*numQueuesPerGoroutine, len(queues))

	// Verify that same-named queues return the same instance
	queuesByName := make(map[string]SafeQueue[[]byte])
	for _, q := range queues {
		name := q.Name()
		if existing, ok := queuesByName[name]; ok {
			// Same name should return same queue instance
			assert.Equal(t, existing, q, "same queue name should return same instance")
		} else {
			queuesByName[name] = q
		}
	}

	// Should only have 3 unique queues (a, b, c)
	assert.Equal(t, 3, len(queuesByName))

	// Clean up
	for _, q := range queuesByName {
		q.Close()
	}
}

// TestUnifiedFactory_ConcurrentDifferentTypes tests concurrent creation of queues with different types
func TestUnifiedFactory_ConcurrentDifferentTypes(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	// Concurrently try to create queues with different types for same name
	// This should fail for type mismatch
	for i := 0; i < 10; i++ {
		wg.Add(2)

		// Create []byte queue
		go func() {
			defer wg.Done()
			_, err := GetOrCreateSafe[[]byte](factory, "typed-concurrent-queue", NewJsonMessage([]byte{}))
			if err != nil {
				errChan <- err
			}
		}()

		// Try to create customMessage queue with same name
		go func() {
			defer wg.Done()
			_, err := GetOrCreateSafe[customMessage](factory, "typed-concurrent-queue", NewJsonMessage(customMessage{}))
			if err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)

	// Collect errors - we expect some type mismatch errors
	var typeMismatchCount int
	for err := range errChan {
		if err != nil && err.Error() == "queue \"typed-concurrent-queue\" already exists with a different message type" {
			typeMismatchCount++
		}
	}

	// At least some should have type mismatch (all but the first successful one of one type)
	assert.GreaterOrEqual(t, typeMismatchCount, 1, "expected at least one type mismatch error")
}

// TestUnifiedFactory_ConfigImmutability tests that modifying returned config doesn't affect factory
func TestUnifiedFactory_ConfigImmutability(t *testing.T) {
	backendConfig := json.RawMessage(`{"test": "value"}`)
	config := UnifiedQueueConfig{
		Type:          QueueTypeMemory,
		MaxSize:       100,
		BackendConfig: backendConfig,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	// Get config and modify it
	returnedConfig := factory.Config()
	returnedConfig.MaxSize = 9999
	returnedConfig.BackendConfig[0] = 'X' // Modify the backing slice

	// Verify original config is unchanged
	originalConfig := factory.Config()
	assert.Equal(t, 100, originalConfig.MaxSize, "MaxSize should not be modified")
	assert.Equal(t, byte('{'), originalConfig.BackendConfig[0], "BackendConfig should not be modified")
}

// TestUnifiedFactory_ClearCache tests that ClearCache removes all cached queues
func TestUnifiedFactory_ClearCache(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	// Create multiple queues
	q1, err := GetOrCreateSafe[[]byte](factory, "cache-test-1", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	q2, err := GetOrCreateSafe[[]byte](factory, "cache-test-2", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	// Verify cache size
	assert.Equal(t, 2, factory.CacheSize())

	// Close queues
	q1.Close()
	q2.Close()

	// Clear cache
	factory.ClearCache()
	assert.Equal(t, 0, factory.CacheSize())

	// Create new queue with same name - should create new instance
	q3, err := GetOrCreateSafe[[]byte](factory, "cache-test-1", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	assert.NotEqual(t, q1, q3, "should create new queue after cache clear")

	q3.Close()
}

// TestUnifiedFactory_RemoveFromCache tests that RemoveFromCache removes specific queue
func TestUnifiedFactory_RemoveFromCache(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	// Create multiple queues
	q1, err := GetOrCreateSafe[[]byte](factory, "remove-test-1", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	q2, err := GetOrCreateSafe[[]byte](factory, "remove-test-2", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	assert.Equal(t, 2, factory.CacheSize())

	// Close and remove one queue
	q1.Close()
	removed := factory.RemoveFromCache("remove-test-1")
	assert.True(t, removed)
	assert.Equal(t, 1, factory.CacheSize())

	// Remove non-existent queue
	removed = factory.RemoveFromCache("non-existent")
	assert.False(t, removed)
	assert.Equal(t, 1, factory.CacheSize())

	// Create new queue with same name - should create new instance
	q3, err := GetOrCreateSafe[[]byte](factory, "remove-test-1", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	assert.NotEqual(t, q1, q3, "should create new queue after removal")

	// q2 should still be the same cached instance
	q2Again, err := GetOrCreateSafe[[]byte](factory, "remove-test-2", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	assert.Equal(t, q2, q2Again, "unremoved queue should still be cached")

	q2.Close()
	q3.Close()
}

// TestUnifiedFactory_CacheSize tests that CacheSize returns correct count
func TestUnifiedFactory_CacheSize(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	// Initial cache should be empty
	assert.Equal(t, 0, factory.CacheSize())

	// Create queues
	queues := make([]SafeQueue[[]byte], 0)
	for i := 0; i < 5; i++ {
		q, err := GetOrCreateSafe[[]byte](factory, "size-test-"+string(rune('a'+i)), NewJsonMessage([]byte{}))
		require.NoError(t, err)
		queues = append(queues, q)
		assert.Equal(t, i+1, factory.CacheSize())
	}

	// Cleanup
	for _, q := range queues {
		q.Close()
	}
}
