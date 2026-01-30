package queue

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisFactory_DiscoverQueues(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx := context.Background()

	// Create some queues by adding data to Redis
	// Queue keys are in format: container::queue::::name (keyPrefix + name)
	prefix := keyPrefix

	// Main queues
	rdb.RPush(ctx, prefix+"orders", "msg1", "msg2")
	rdb.RPush(ctx, prefix+"payments", "msg1")
	rdb.RPush(ctx, prefix+"notifications", "msg1", "msg2", "msg3")

	// Retry queues
	rdb.RPush(ctx, prefix+"orders::retry", "retry1")

	// DLQ queues
	rdb.RPush(ctx, prefix+"orders::DLQ", "dlq1")

	factory, err := NewRedisFactory(rdb)
	require.NoError(t, err)

	// Test discovering all queues
	queues, err := factory.DiscoverQueues(ctx, "")
	require.NoError(t, err)
	require.Len(t, queues, 3)

	// Verify queue info
	queueMap := make(map[string]QueueInfo)
	for _, q := range queues {
		queueMap[q.Name] = q
	}

	assert.Contains(t, queueMap, "orders")
	assert.Contains(t, queueMap, "payments")
	assert.Contains(t, queueMap, "notifications")

	assert.Equal(t, int64(2), queueMap["orders"].Depth)
	assert.Equal(t, int64(1), queueMap["payments"].Depth)
	assert.Equal(t, int64(3), queueMap["notifications"].Depth)

	assert.Equal(t, QueueInfoTypeMain, queueMap["orders"].Type)
}

func TestRedisFactory_DiscoverQueues_WithPattern(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx := context.Background()

	prefix := keyPrefix

	// Create queues with different prefixes
	rdb.RPush(ctx, prefix+"orders-us", "msg1")
	rdb.RPush(ctx, prefix+"orders-eu", "msg1")
	rdb.RPush(ctx, prefix+"payments-us", "msg1")
	rdb.RPush(ctx, prefix+"notifications", "msg1")

	factory, err := NewRedisFactory(rdb)
	require.NoError(t, err)

	// Test pattern matching with prefix
	queues, err := factory.DiscoverQueues(ctx, "orders-*")
	require.NoError(t, err)
	require.Len(t, queues, 2)

	names := make([]string, len(queues))
	for i, q := range queues {
		names[i] = q.Name
	}
	assert.Contains(t, names, "orders-us")
	assert.Contains(t, names, "orders-eu")
}

func TestRedisFactory_DiscoverAllQueues(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx := context.Background()

	prefix := keyPrefix

	// Create queues with main, retry, and DLQ
	rdb.RPush(ctx, prefix+"orders", "msg1", "msg2")
	rdb.RPush(ctx, prefix+"orders::retry", "retry1")
	rdb.RPush(ctx, prefix+"orders::DLQ", "dlq1", "dlq2")

	factory, err := NewRedisFactory(rdb)
	require.NoError(t, err)

	// Test discovering all queues including retry and DLQ
	queues, err := factory.DiscoverAllQueues(ctx, "")
	require.NoError(t, err)
	require.Len(t, queues, 3)

	// Verify queue types
	typeMap := make(map[string]string)
	depthMap := make(map[string]int64)
	for _, q := range queues {
		typeMap[q.Name] = q.Type
		depthMap[q.Name] = q.Depth
	}

	assert.Equal(t, QueueInfoTypeMain, typeMap["orders"])
	assert.Equal(t, QueueInfoTypeRetry, typeMap["orders::retry"])
	assert.Equal(t, QueueInfoTypeDLQ, typeMap["orders::DLQ"])

	assert.Equal(t, int64(2), depthMap["orders"])
	assert.Equal(t, int64(1), depthMap["orders::retry"])
	assert.Equal(t, int64(2), depthMap["orders::DLQ"])
}

func TestUnifiedFactory_DiscoverQueues_Redis(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	config := UnifiedQueueConfig{
		Type:        QueueTypeRedis,
		MaxSize:     100,
		RedisClient: rdb,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Create some queues through the factory
	q1, err := GetOrCreateSafe[[]byte](factory, "test-queue-1", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q1.Close()

	q2, err := GetOrCreateSafe[[]byte](factory, "test-queue-2", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q2.Close()

	// Enqueue some messages
	err = q1.Enqueue(ctx, []byte("msg1"))
	require.NoError(t, err)
	err = q1.Enqueue(ctx, []byte("msg2"))
	require.NoError(t, err)

	err = q2.Enqueue(ctx, []byte("msg3"))
	require.NoError(t, err)

	// Discover queues
	queues, err := factory.DiscoverQueues(ctx, "")
	require.NoError(t, err)

	// Should find at least our 2 queues (may also find DLQ queues created by the factory)
	names := make(map[string]bool)
	for _, q := range queues {
		names[q.Name] = true
	}

	assert.True(t, names["test-queue-1"], "should find test-queue-1")
	assert.True(t, names["test-queue-2"], "should find test-queue-2")
}

func TestUnifiedFactory_DiscoverQueues_Memory(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Create some queues through the factory
	q1, err := GetOrCreateSafe[[]byte](factory, "memory-queue-1", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q1.Close()

	q2, err := GetOrCreateSafe[[]byte](factory, "memory-queue-2", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q2.Close()

	// Discover queues (returns cached queues for memory backend)
	queues, err := factory.DiscoverQueues(ctx, "")
	require.NoError(t, err)
	require.Len(t, queues, 2)

	names := make(map[string]bool)
	for _, q := range queues {
		names[q.Name] = true
	}

	assert.True(t, names["memory-queue-1"])
	assert.True(t, names["memory-queue-2"])
}

func TestUnifiedFactory_DiscoverQueues_MemoryWithPattern(t *testing.T) {
	config := UnifiedQueueConfig{
		Type:    QueueTypeMemory,
		MaxSize: 100,
	}

	factory, err := NewUnifiedFactory(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Create queues with different names
	q1, err := GetOrCreateSafe[[]byte](factory, "orders-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q1.Close()

	q2, err := GetOrCreateSafe[[]byte](factory, "orders-archive", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q2.Close()

	q3, err := GetOrCreateSafe[[]byte](factory, "payments-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)
	defer q3.Close()

	// Discover with pattern
	queues, err := factory.DiscoverQueues(ctx, "orders-*")
	require.NoError(t, err)
	require.Len(t, queues, 2)

	names := make(map[string]bool)
	for _, q := range queues {
		names[q.Name] = true
	}

	assert.True(t, names["orders-queue"])
	assert.True(t, names["orders-archive"])
	assert.False(t, names["payments-queue"])
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern  string
		name     string
		expected bool
	}{
		// Wildcard all
		{"*", "anything", true},
		{"", "anything", true},

		// Prefix matching
		{"orders-*", "orders-us", true},
		{"orders-*", "orders-eu", true},
		{"orders-*", "payments-us", false},

		// Suffix matching
		{"*-queue", "orders-queue", true},
		{"*-queue", "payments-queue", true},
		{"*-queue", "orders-archive", false},

		// Contains matching
		{"*order*", "my-order-queue", true},
		{"*order*", "orders", true},
		{"*order*", "payments", false},

		// Exact matching
		{"orders", "orders", true},
		{"orders", "orders-us", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			result := matchPattern(tt.pattern, tt.name)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMemoryQueueFactory_DiscoverQueues(t *testing.T) {
	ctx := context.Background()

	factory, err := NewMemoryQueueFactory()
	require.NoError(t, err)

	// Create some queues
	_, err = MemoryGetOrCreateSafe[[]byte](factory, "orders-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	_, err = MemoryGetOrCreateSafe[[]byte](factory, "payments-queue", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	// Discover queues
	queues, err := factory.DiscoverQueues(ctx, "")
	require.NoError(t, err)
	require.Len(t, queues, 2)

	names := make(map[string]bool)
	for _, q := range queues {
		names[q.Name] = true
	}

	assert.True(t, names["orders-queue"])
	assert.True(t, names["payments-queue"])
}

func TestMemoryQueueFactory_DiscoverQueues_WithPattern(t *testing.T) {
	ctx := context.Background()

	factory, err := NewMemoryQueueFactory()
	require.NoError(t, err)

	// Create queues with different prefixes
	_, err = MemoryGetOrCreateSafe[[]byte](factory, "orders-us", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	_, err = MemoryGetOrCreateSafe[[]byte](factory, "orders-eu", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	_, err = MemoryGetOrCreateSafe[[]byte](factory, "payments-us", NewJsonMessage([]byte{}))
	require.NoError(t, err)

	// Discover with pattern
	queues, err := factory.DiscoverQueues(ctx, "orders-*")
	require.NoError(t, err)
	require.Len(t, queues, 2)

	names := make([]string, len(queues))
	for i, q := range queues {
		names[i] = q.Name
	}
	assert.Contains(t, names, "orders-us")
	assert.Contains(t, names, "orders-eu")
}

func TestRedisFactory_EmptyDatabase(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx := context.Background()

	factory, err := NewRedisFactory(rdb)
	require.NoError(t, err)

	queues, err := factory.DiscoverQueues(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, queues)
}
