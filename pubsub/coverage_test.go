package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ivanzzeth/go-universal-data-containers/message"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Config Coverage Tests ====================

func TestRedisOptionsFromMap_ReadWriteTimeout(t *testing.T) {
	m := map[string]any{
		"addr":          "localhost:6379",
		"read_timeout":  5 * time.Second,
		"write_timeout": 10 * time.Second,
	}

	opts := RedisOptionsFromMap(m)
	assert.Equal(t, 5*time.Second, opts.ReadTimeout)
	assert.Equal(t, 10*time.Second, opts.WriteTimeout)
}

func TestRedisOptionsFromMap_ZeroTimeout(t *testing.T) {
	m := map[string]any{
		"read_timeout":  time.Duration(0),
		"write_timeout": time.Duration(0),
	}

	defaults := DefaultRedisOptions()
	opts := RedisOptionsFromMap(m)
	// Zero timeout should not override defaults
	assert.Equal(t, defaults.ReadTimeout, opts.ReadTimeout)
	assert.Equal(t, defaults.WriteTimeout, opts.WriteTimeout)
}

// ==================== Factory Coverage Tests ====================

func TestFactory_RegisterBackendNilFactory(t *testing.T) {
	assert.Panics(t, func() {
		RegisterBackend("nil-factory", nil)
	})
}

func TestFactory_RegisterBackendDuplicate(t *testing.T) {
	// First registration should succeed
	RegisterBackend("dup-test", func(cfg Config) (PubSub, error) {
		return nil, nil
	})

	// Second registration should panic
	assert.Panics(t, func() {
		RegisterBackend("dup-test", func(cfg Config) (PubSub, error) {
			return nil, nil
		})
	})
}

func TestFactory_EmptyBackend(t *testing.T) {
	cfg := Config{
		Backend:    "",
		BufferSize: 100,
	}

	_, err := NewPubSub(cfg)
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

// ==================== Memory Coverage Tests ====================

func TestMemoryPubSub_SubscribeClosed(t *testing.T) {
	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)

	ps.Close()

	// Subscribe after close should fail
	_, err = ps.Subscribe(context.Background(), "topic")
	assert.ErrorIs(t, err, ErrPubSubClosed)
}

func TestMemoryPubSub_SubscribeWithHandlerNil(t *testing.T) {
	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	err = ps.SubscribeWithHandler(context.Background(), "topic", nil)
	assert.ErrorIs(t, err, ErrNilHandler)
}

func TestMemoryPubSub_SubscribeWithHandlerError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	// Subscribe with handler that returns after subscription error
	errChan := make(chan error, 1)
	go func() {
		err := ps.SubscribeWithHandler(ctx, "topic", func(ctx context.Context, msg message.Message[any]) error {
			return nil
		})
		errChan <- err
	}()

	// Wait for handler to start
	time.Sleep(100 * time.Millisecond)

	// Close the pubsub, which should cause the handler to exit
	ps.Close()

	select {
	case err := <-errChan:
		// Should return nil (handler exits when channel closes)
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler to exit")
	}
}

func TestMemoryPubSub_PublishBatchError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowBlock,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "batch-error-topic"
	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("fill", "data"))
	require.NoError(t, err)

	// Cancel context before batch publish completes
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	msgs := []message.Message[any]{
		newTestMessage("batch-1", "data"),
		newTestMessage("batch-2", "data"),
	}

	err = ps.PublishBatch(ctx, topic, msgs)
	assert.ErrorIs(t, err, context.Canceled)

	_ = sub
}

func TestMemoryPubSub_SafeSendClosedCheck(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 10,
		OnFull:     OverflowBlock,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "safe-send-topic"

	// Subscribe
	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Close subscription immediately
	sub.Unsubscribe()
	time.Sleep(50 * time.Millisecond)

	// Publish to topic with no active subscribers (all filtered out as closed)
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	assert.NoError(t, err) // Should succeed (no subscribers)
}

func TestMemoryPubSub_OverflowBlockDone(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowBlock,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "block-done-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("fill", "data"))
	require.NoError(t, err)

	// Unsubscribe while publish would be blocked
	go func() {
		time.Sleep(50 * time.Millisecond)
		sub.Unsubscribe()
	}()

	// This publish should exit via the done channel
	timeoutCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = ps.Publish(timeoutCtx, topic, newTestMessage("msg", "data"))
	// Should not block indefinitely - either succeeds or context cancelled
}

func TestMemoryPubSub_OverflowDropDone(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowDrop,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "drop-done-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("fill", "data"))
	require.NoError(t, err)

	// Unsubscribe
	sub.Unsubscribe()
	time.Sleep(50 * time.Millisecond)

	// Publish to closed subscription - should be filtered
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	assert.NoError(t, err)
}

// ==================== Redis Coverage Tests ====================

func TestRedisPubSub_ZeroBufferSize(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 0, // Should default to 100
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)
	defer ps.Close()

	assert.NotNil(t, ps)
}

func TestRedisPubSub_SubscribeClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)

	ps.Close()

	// Subscribe after close should fail
	_, err = ps.Subscribe(context.Background(), "topic")
	assert.ErrorIs(t, err, ErrPubSubClosed)
}

func TestRedisPubSub_SubscribeWithHandlerNil(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

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

	err = ps.SubscribeWithHandler(context.Background(), "topic", nil)
	assert.ErrorIs(t, err, ErrNilHandler)
}

func TestRedisPubSub_PublishBatchClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)

	ps.Close()

	msgs := []message.Message[any]{newTestMessage("msg", "data")}
	err = ps.PublishBatch(context.Background(), "topic", msgs)
	assert.ErrorIs(t, err, ErrPubSubClosed)
}

// ==================== Middleware Coverage Tests ====================

func TestMiddleware_RetryBatchWithFailures(t *testing.T) {
	ctx := context.Background()

	var attempts int
	failingPublisher := &mockPublisher{
		publishBatchFn: func(ctx context.Context, topic string, msgs []message.Message[any]) error {
			attempts++
			if attempts <= 2 {
				return errors.New("temporary error")
			}
			return nil
		},
	}

	publisher := ChainPublisher(failingPublisher, WithRetry(3, 10*time.Millisecond))

	msgs := []message.Message[any]{newTestMessage("msg", "data")}
	err := publisher.PublishBatch(ctx, "topic", msgs)
	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestMiddleware_RetryBatchExhausted(t *testing.T) {
	ctx := context.Background()

	failingPublisher := &mockPublisher{
		publishBatchFn: func(ctx context.Context, topic string, msgs []message.Message[any]) error {
			return errors.New("permanent error")
		},
	}

	publisher := ChainPublisher(failingPublisher, WithRetry(2, 10*time.Millisecond))

	msgs := []message.Message[any]{newTestMessage("msg", "data")}
	err := publisher.PublishBatch(ctx, "topic", msgs)
	assert.Error(t, err)
}

func TestMiddleware_RetryBatchContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	failingPublisher := &mockPublisher{
		publishBatchFn: func(ctx context.Context, topic string, msgs []message.Message[any]) error {
			return errors.New("error")
		},
	}

	publisher := ChainPublisher(failingPublisher, WithRetry(10, 100*time.Millisecond))

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	msgs := []message.Message[any]{newTestMessage("msg", "data")}
	err := publisher.PublishBatch(ctx, "topic", msgs)
	assert.ErrorIs(t, err, context.Canceled)
}

// ==================== Additional Edge Cases ====================

func TestMemoryPubSub_DefaultOverflowPolicy(t *testing.T) {
	ctx := context.Background()

	// Don't specify OnFull - should default to OverflowDrop
	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
	})
	require.NoError(t, err)
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "default-policy-topic")
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, "default-policy-topic", newTestMessage("msg1", "data"))
	require.NoError(t, err)

	// This should drop (not block)
	done := make(chan struct{})
	go func() {
		ps.Publish(ctx, "default-policy-topic", newTestMessage("msg2", "data"))
		close(done)
	}()

	select {
	case <-done:
		// Good - didn't block
	case <-time.After(time.Second):
		t.Fatal("publish blocked - should have dropped")
	}

	_ = sub
}

func TestMemoryPubSub_PublishNoActiveSubscribers(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	topic := "no-active-subs"

	// Subscribe and immediately unsubscribe
	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)
	sub.Unsubscribe()
	time.Sleep(50 * time.Millisecond)

	// Now there are no active subscribers but the topic may still exist
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	assert.NoError(t, err)
}

func TestMemoryPubSub_PublishAllSubscribersFiltered(t *testing.T) {
	ctx := context.Background()

	ps := MustNewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 100,
	})
	defer ps.Close()

	topic := "all-filtered-topic"

	// Create subscription
	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Close it immediately so it gets filtered during publish
	sub.Unsubscribe()
	time.Sleep(100 * time.Millisecond)

	// Publish - all subscribers should be filtered out
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	assert.NoError(t, err)
}

func TestMemoryPubSub_SubscribeWithHandlerSubscribeError(t *testing.T) {
	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)

	// Close immediately
	ps.Close()

	// SubscribeWithHandler should fail
	err = ps.SubscribeWithHandler(context.Background(), "topic", func(ctx context.Context, msg message.Message[any]) error {
		return nil
	})
	assert.ErrorIs(t, err, ErrPubSubClosed)
}

func TestRedisPubSub_SubscribeWithHandlerSubscribeError(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)

	// Close immediately
	ps.Close()

	// SubscribeWithHandler should fail
	err = ps.SubscribeWithHandler(context.Background(), "topic", func(ctx context.Context, msg message.Message[any]) error {
		return nil
	})
	assert.ErrorIs(t, err, ErrPubSubClosed)
}

func TestRedisPubSub_PublishBatchNilMessage(t *testing.T) {
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

	// Subscribe to receive messages
	_, err = ps.Subscribe(ctx, "batch-nil-topic")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Batch with nil message should skip nil
	msgs := []message.Message[any]{
		newTestMessage("msg-1", "data"),
		nil, // Should be skipped
		newTestMessage("msg-2", "data"),
	}

	err = ps.PublishBatch(ctx, "batch-nil-topic", msgs)
	require.NoError(t, err)
}

func TestRedisPubSub_PublishBatchEmptyTopic(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

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

	msgs := []message.Message[any]{newTestMessage("msg", "data")}
	err = ps.PublishBatch(context.Background(), "", msgs)
	assert.ErrorIs(t, err, ErrTopicEmpty)
}

func TestMemoryPubSub_SafeSendBlockDone(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowBlock,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "safe-send-block-done"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("fill", "data"))
	require.NoError(t, err)

	// Start a blocked publish
	publishDone := make(chan error, 1)
	go func() {
		err := ps.Publish(ctx, topic, newTestMessage("blocked", "data"))
		publishDone <- err
	}()

	// Give some time for publish to block
	time.Sleep(50 * time.Millisecond)

	// Unsubscribe to trigger done channel
	sub.Unsubscribe()

	// Publish should exit via done channel
	select {
	case <-publishDone:
		// Good - publish exited
	case <-time.After(time.Second):
		t.Fatal("publish did not exit after unsubscribe")
	}
}

func TestMemoryPubSub_SafeSendDropContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowDrop,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "drop-ctx-cancel"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("fill", "data"))
	require.NoError(t, err)

	// Cancel context
	cancel()

	// This should return context.Canceled
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	assert.ErrorIs(t, err, context.Canceled)

	_ = sub
}

func TestMemoryPubSub_SafeSendDropDone(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowDrop,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "drop-done"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Mark subscription as closed manually for testing
	ms := sub.(*memorySubscription)
	ms.closed.Store(true)

	// Publish should filter out closed subscription
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	assert.NoError(t, err)
}

func TestMemoryPubSub_UnknownOverflowPolicy(t *testing.T) {
	ctx := context.Background()

	// Use internal constructor to create with unknown policy
	ps := &memoryPubSub{
		subs:       make(map[string]map[string]*memorySubscription),
		bufferSize: 100,
		onFull:     OverflowPolicy(99), // Unknown policy
		metrics:    NewMetrics("memory-unknown"),
	}
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "unknown-policy")
	require.NoError(t, err)

	// Publish with unknown policy should fall through to return false
	err = ps.Publish(ctx, "unknown-policy", newTestMessage("msg", "data"))
	assert.NoError(t, err)

	_ = sub
}

// ==================== Redis Client Provider Tests ====================

func TestRedisPubSub_WithRedisClientInterface(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create a real redis client and pass it as RedisClient interface
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"client": RedisClient(client), // Pass as RedisClient interface
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)
	defer ps.Close()

	assert.NotNil(t, ps)
}

func TestRedisPubSub_WithRedisClientPointer(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create a real redis client and pass it as *redis.Client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"client": client, // Pass as *redis.Client
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)
	defer ps.Close()

	assert.NotNil(t, ps)
}

func TestRedisPubSub_ConnectionFailed(t *testing.T) {
	// Try to connect to a non-existent Redis server
	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": "localhost:59999", // Non-existent port
		},
	}

	_, err := NewPubSub(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis ping failed")
}

func TestRedisPubSub_ReceiveMessagesChannelClosed(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 100,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)

	sub, err := ps.Subscribe(ctx, "channel-close-topic")
	require.NoError(t, err)

	// Close the pubsub which will close the Redis channel
	ps.Close()

	// Message channel should be closed
	_, ok := <-sub.Messages()
	assert.False(t, ok)
}

func TestRedisPubSub_ReceiveMessagesContextDone(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx, cancel := context.WithCancel(context.Background())

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

	sub, err := ps.Subscribe(ctx, "ctx-done-topic")
	require.NoError(t, err)

	// Cancel context to trigger receiveMessages exit
	cancel()

	// Wait for cleanup
	time.Sleep(200 * time.Millisecond)

	// Subscription should be cleaned up
	assert.Equal(t, 0, ps.SubscriberCount("ctx-done-topic"))
	_ = sub
}

func TestRedisPubSub_ReceiveMessagesDone(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx := context.Background()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 1,
		OnFull:     OverflowDrop,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "done-topic")
	require.NoError(t, err)

	// Unsubscribe to close done channel
	sub.Unsubscribe()

	// Wait for cleanup
	time.Sleep(200 * time.Millisecond)

	// Subscription should be cleaned up
	assert.Equal(t, 0, ps.SubscriberCount("done-topic"))
}

func TestRedisPubSub_ReceiveMessagesOverflowBlock(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := Config{
		Backend:    BackendRedis,
		BufferSize: 1,
		OnFull:     OverflowBlock,
		Options: map[string]any{
			"addr": mr.Addr(),
		},
	}

	ps, err := NewPubSub(cfg)
	require.NoError(t, err)
	defer ps.Close()

	topic := "block-receive-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Publish messages - some will block due to buffer size
	for i := 0; i < 5; i++ {
		go ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	}

	// Consume messages
	for i := 0; i < 5; i++ {
		select {
		case <-sub.Messages():
		case <-ctx.Done():
			return
		}
	}
}

func TestMemoryPubSub_SafeSendClosedDuringBlock(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 1,
		OnFull:     OverflowBlock,
	})
	require.NoError(t, err)
	defer ps.Close()

	topic := "closed-during-block"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Get the underlying subscription
	ms := sub.(*memorySubscription)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("fill", "data"))
	require.NoError(t, err)

	// Set closed flag while publish would be checking it
	go func() {
		time.Sleep(10 * time.Millisecond)
		ms.closed.Store(true)
		close(ms.done)
	}()

	// This should detect the closed flag in safeSend
	timeoutCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	ps.Publish(timeoutCtx, topic, newTestMessage("msg", "data"))
}
