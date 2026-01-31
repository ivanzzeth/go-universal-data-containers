package pubsub

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redisFactory(t testing.TB, opts ...func(*Config)) PubSub {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	ps, err := NewPubSub(cfg)
	if err != nil {
		t.Fatalf("failed to create redis pubsub: %v", err)
	}
	return ps
}

func redisTestFactory(t *testing.T, opts ...func(*Config)) PubSub {
	return redisFactory(t, opts...)
}

func redisBenchFactory(b *testing.B) PubSub {
	return redisFactory(b)
}

// ==================== Spec Tests ====================

func TestRedisPubSub_BasicPublishSubscribe(t *testing.T) {
	SpecTestBasicPublishSubscribe(t, redisTestFactory)
}

func TestRedisPubSub_FanOut(t *testing.T) {
	SpecTestFanOut(t, redisTestFactory)
}

func TestRedisPubSub_NoSubscribers(t *testing.T) {
	SpecTestNoSubscribers(t, redisTestFactory)
}

func TestRedisPubSub_Unsubscribe(t *testing.T) {
	SpecTestUnsubscribe(t, redisTestFactory)
}

func TestRedisPubSub_SubscribeWithHandler(t *testing.T) {
	SpecTestSubscribeWithHandler(t, redisTestFactory)
}

func TestRedisPubSub_HandlerError(t *testing.T) {
	SpecTestHandlerError(t, redisTestFactory)
}

func TestRedisPubSub_Topics(t *testing.T) {
	SpecTestTopics(t, redisTestFactory)
}

func TestRedisPubSub_SubscriberCount(t *testing.T) {
	SpecTestSubscriberCount(t, redisTestFactory)
}

func TestRedisPubSub_Close(t *testing.T) {
	SpecTestClose(t, redisTestFactory)
}

func TestRedisPubSub_DoubleClose(t *testing.T) {
	SpecTestDoubleClose(t, redisTestFactory)
}

func TestRedisPubSub_Validation(t *testing.T) {
	SpecTestValidation(t, redisTestFactory)
}

func TestRedisPubSub_PublishBatch(t *testing.T) {
	SpecTestPublishBatch(t, redisTestFactory)
}

func TestRedisPubSub_OverflowDrop(t *testing.T) {
	SpecTestOverflowDrop(t, redisTestFactory)
}

func TestRedisPubSub_OverflowBlock(t *testing.T) {
	SpecTestOverflowBlock(t, redisTestFactory)
}

func TestRedisPubSub_ClosedSubscriptionFilter(t *testing.T) {
	SpecTestClosedSubscriptionFilter(t, redisTestFactory)
}

// ==================== Benchmarks ====================

func BenchmarkRedisPubSub_Publish(b *testing.B) {
	SpecBenchmarkPublish(b, redisBenchFactory)
}

// ==================== Additional Redis-Specific Tests ====================

func TestRedisOptions_ToMap(t *testing.T) {
	opts := RedisOptions{
		Addr:     "localhost:6380",
		Password: "secret",
		DB:       1,
		Prefix:   "test:",
		PoolSize: 20,
	}

	m := opts.ToMap()
	require.Equal(t, "localhost:6380", m["addr"])
	require.Equal(t, "secret", m["password"])
	require.Equal(t, 1, m["db"])
	require.Equal(t, "test:", m["prefix"])
	require.Equal(t, 20, m["pool_size"])
}

func TestRedisOptionsFromMap_Defaults(t *testing.T) {
	opts := RedisOptionsFromMap(map[string]any{})

	defaults := DefaultRedisOptions()
	require.Equal(t, defaults.Addr, opts.Addr)
	require.Equal(t, defaults.DB, opts.DB)
	require.Equal(t, defaults.Prefix, opts.Prefix)
}

func TestRedisOptionsFromMap_Custom(t *testing.T) {
	m := map[string]any{
		"addr":     "redis.example.com:6379",
		"password": "pass123",
		"db":       2,
		"prefix":   "myapp:",
		"pool_size": 50,
	}

	opts := RedisOptionsFromMap(m)
	require.Equal(t, "redis.example.com:6379", opts.Addr)
	require.Equal(t, "pass123", opts.Password)
	require.Equal(t, 2, opts.DB)
	require.Equal(t, "myapp:", opts.Prefix)
	require.Equal(t, 50, opts.PoolSize)
}

func TestRedisPubSub_SubscriptionString(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx := context.Background()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "redis-string-topic")
	require.NoError(t, err)

	// Test String() method
	rs := sub.(*redisSubscription)
	str := rs.String()
	assert.Contains(t, str, "RedisSubscription{")
	assert.Contains(t, str, "redis-string-topic")
}
