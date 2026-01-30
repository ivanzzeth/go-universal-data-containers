package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedFactory_Memory(t *testing.T) {
	// No registration needed for built-in types
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
		ConsumerCount:     2,
	}

	factory, err := NewUnifiedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, factory)

	// Create queue
	q, err := factory.GetOrCreateSafe("test-memory-queue")
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
	q2, err := factory.GetOrCreateSafe("test-memory-queue")
	require.NoError(t, err)
	assert.Equal(t, q, q2)

	q.Close()
}

func TestUnifiedFactory_Redis(t *testing.T) {
	// Start miniredis
	s := miniredis.RunT(t)

	// No registration needed for built-in types
	backendConfig, _ := json.Marshal(RedisQueueConfig{
		Addr: s.Addr(),
	})

	config := UnifiedQueueConfig{
		Type:              QueueTypeRedis,
		MaxSize:           100,
		MaxHandleFailures: 3,
		BackendConfig:     backendConfig,
	}

	factory, err := NewUnifiedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)
	require.NotNil(t, factory)

	// Create queue
	q, err := factory.GetOrCreateSafe("test-redis-queue")
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

	// Use RedisClient field instead of registration
	config := UnifiedQueueConfig{
		Type:        QueueTypeRedis,
		MaxSize:     100,
		RedisClient: rdb,
	}

	factory, err := NewUnifiedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)

	q, err := factory.GetOrCreateSafe("test-redis-client-queue")
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
	// No registration needed for built-in types

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

	factory, err := NewUnifiedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)

	q, err := factory.GetOrCreateSafe("json-config-queue")
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

	// No registration needed for built-in types

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

	factory, err := NewUnifiedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)

	q, err := factory.GetOrCreateSafe("redis-json-config-queue")
	require.NoError(t, err)
	require.NotNil(t, q)

	q.Close()
}

func TestUnifiedFactory_ValidationErrors(t *testing.T) {
	// No registration needed - built-in types have built-in validation

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
			_, err := NewUnifiedFactory(tt.config, NewJsonMessage([]byte{}))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestUnifiedFactory_Subscribe(t *testing.T) {
	// No registration needed for built-in types
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
	}

	factory, err := NewUnifiedFactory(config, NewJsonMessage([]byte{}))
	require.NoError(t, err)

	q, err := factory.GetOrCreateSafe("subscribe-test-queue")
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

// TestUnifiedFactory_CustomType tests using custom message types
type customMessage struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func TestUnifiedFactory_CustomType(t *testing.T) {
	// No registration needed - generic type is inferred from defaultMsg
	config := UnifiedQueueConfig{
		Type:              QueueTypeMemory,
		MaxSize:           100,
		MaxHandleFailures: 3,
	}

	factory, err := NewUnifiedFactory(config, NewJsonMessage(customMessage{}))
	require.NoError(t, err)

	q, err := factory.GetOrCreateSafe("custom-type-queue")
	require.NoError(t, err)

	ctx := context.Background()
	err = q.Enqueue(ctx, customMessage{ID: "123", Data: "test"})
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "123", msg.Data().ID)
	assert.Equal(t, "test", msg.Data().Data)

	q.Close()
}
