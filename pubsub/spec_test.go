package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PubSubFactory creates PubSub instances for testing
type PubSubFactory func(t *testing.T, opts ...func(*Config)) PubSub

// TestMessage is a simple test message payload
type TestMessage struct {
	ID   string
	Data string
}

// newTestMessage creates a test message
func newTestMessage(id, data string) message.Message[any] {
	msg := &message.JsonMessage[any]{}
	msg.SetData(TestMessage{ID: id, Data: data})
	msg.SetID([]byte(id))
	return msg
}

// ==================== Spec Tests ====================

// SpecTestBasicPublishSubscribe tests basic publish/subscribe functionality
func SpecTestBasicPublishSubscribe(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := factory(t)
	defer ps.Close()

	topic := "basic-topic"

	// Subscribe first
	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)
	assert.NotEmpty(t, sub.ID())
	assert.Equal(t, topic, sub.Topic())

	// Publish message
	msg := newTestMessage("msg-1", "hello world")
	err = ps.Publish(ctx, topic, msg)
	require.NoError(t, err)

	// Receive message
	select {
	case received := <-sub.Messages():
		assert.NotNil(t, received.Data())
		assert.NotNil(t, received.ID())
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

// SpecTestFanOut tests that messages are delivered to all subscribers
func SpecTestFanOut(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := factory(t)
	defer ps.Close()

	topic := "fanout-topic"
	numSubscribers := 5

	// Create multiple subscribers
	subs := make([]Subscription, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		sub, err := ps.Subscribe(ctx, topic)
		require.NoError(t, err)
		subs[i] = sub
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish message
	msg := newTestMessage("fanout-msg", "broadcast")
	err := ps.Publish(ctx, topic, msg)
	require.NoError(t, err)

	// All subscribers should receive the message
	for i, sub := range subs {
		select {
		case received := <-sub.Messages():
			assert.NotNil(t, received.Data(), "subscriber %d", i)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for message on subscriber %d", i)
		}
	}
}

// SpecTestNoSubscribers tests publishing when there are no subscribers
func SpecTestNoSubscribers(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)
	defer ps.Close()

	// Publish to topic with no subscribers should not error
	msg := newTestMessage("msg", "data")
	err := ps.Publish(ctx, "empty-topic", msg)
	assert.NoError(t, err)
}

// SpecTestUnsubscribe tests unsubscribe functionality
func SpecTestUnsubscribe(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)
	defer ps.Close()

	topic := "unsub-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Unsubscribe
	err = sub.Unsubscribe()
	require.NoError(t, err)

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Second unsubscribe should return error
	err = sub.Unsubscribe()
	assert.ErrorIs(t, err, ErrSubscriptionClosed)
}

// SpecTestSubscribeWithHandler tests handler-based subscription
func SpecTestSubscribeWithHandler(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := factory(t)
	defer ps.Close()

	topic := "handler-topic"
	var received atomic.Int32

	// Subscribe with handler
	go func() {
		ps.SubscribeWithHandler(ctx, topic, func(ctx context.Context, msg message.Message[any]) error {
			received.Add(1)
			return nil
		})
	}()

	// Wait for subscription to be ready
	time.Sleep(200 * time.Millisecond)

	// Publish messages
	for i := 0; i < 3; i++ {
		err := ps.Publish(ctx, topic, newTestMessage(fmt.Sprintf("msg-%d", i), "data"))
		require.NoError(t, err)
	}

	// Wait and verify
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(3), received.Load())
}

// SpecTestHandlerError tests that handler errors don't stop message processing
func SpecTestHandlerError(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := factory(t)
	defer ps.Close()

	topic := "handler-error-topic"
	var handlerCalls atomic.Int32

	go func() {
		ps.SubscribeWithHandler(ctx, topic, func(ctx context.Context, msg message.Message[any]) error {
			handlerCalls.Add(1)
			return fmt.Errorf("handler error")
		})
	}()

	time.Sleep(200 * time.Millisecond)

	// Publish message
	err := ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Handler should have been called despite returning error
	assert.Equal(t, int32(1), handlerCalls.Load())
}

// SpecTestTopics tests Topics() method
func SpecTestTopics(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)
	defer ps.Close()

	// Initially no topics
	assert.Empty(t, ps.Topics())

	// Subscribe to topics
	_, err := ps.Subscribe(ctx, "topic-a")
	require.NoError(t, err)
	_, err = ps.Subscribe(ctx, "topic-b")
	require.NoError(t, err)

	topics := ps.Topics()
	assert.Len(t, topics, 2)
	assert.Contains(t, topics, "topic-a")
	assert.Contains(t, topics, "topic-b")
}

// SpecTestSubscriberCount tests SubscriberCount() method
func SpecTestSubscriberCount(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)
	defer ps.Close()

	topic := "count-topic"

	assert.Equal(t, 0, ps.SubscriberCount(topic))

	sub1, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)
	assert.Equal(t, 1, ps.SubscriberCount(topic))

	sub2, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)
	assert.Equal(t, 2, ps.SubscriberCount(topic))

	sub1.Unsubscribe()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, ps.SubscriberCount(topic))

	sub2.Unsubscribe()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, ps.SubscriberCount(topic))
}

// SpecTestClose tests Close() functionality
func SpecTestClose(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)

	sub, err := ps.Subscribe(ctx, "close-topic")
	require.NoError(t, err)

	err = ps.Close()
	require.NoError(t, err)

	// Channel should be closed
	_, ok := <-sub.Messages()
	assert.False(t, ok)

	// Operations should fail
	err = ps.Publish(ctx, "close-topic", newTestMessage("msg", "data"))
	assert.ErrorIs(t, err, ErrPubSubClosed)
}

// SpecTestDoubleClose tests that Close() is idempotent
func SpecTestDoubleClose(t *testing.T, factory PubSubFactory) {
	ps := factory(t)

	err := ps.Close()
	require.NoError(t, err)

	err = ps.Close()
	require.NoError(t, err)
}

// SpecTestValidation tests input validation
func SpecTestValidation(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)
	defer ps.Close()

	// Empty topic
	err := ps.Publish(ctx, "", newTestMessage("msg", "data"))
	assert.ErrorIs(t, err, ErrTopicEmpty)

	_, err = ps.Subscribe(ctx, "")
	assert.ErrorIs(t, err, ErrTopicEmpty)

	// Nil message
	err = ps.Publish(ctx, "topic", nil)
	assert.ErrorIs(t, err, ErrNilMessage)
}

// SpecTestPublishBatch tests batch publishing
func SpecTestPublishBatch(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := factory(t)
	defer ps.Close()

	topic := "batch-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Create batch of messages
	msgs := make([]message.Message[any], 5)
	for i := 0; i < 5; i++ {
		msgs[i] = newTestMessage(fmt.Sprintf("batch-msg-%d", i), "batch data")
	}

	// Publish batch
	err = ps.PublishBatch(ctx, topic, msgs)
	require.NoError(t, err)

	// Receive all messages
	var received int
	timeout := time.After(2 * time.Second)
	for received < 5 {
		select {
		case <-sub.Messages():
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d of 5 messages", received)
		}
	}

	assert.Equal(t, 5, received)
}

// SpecTestOverflowDrop tests OverflowDrop policy
func SpecTestOverflowDrop(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t, func(c *Config) {
		c.BufferSize = 1
		c.OnFull = OverflowDrop
	})
	defer ps.Close()

	topic := "drop-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Flood with messages - some should be dropped
	for i := 0; i < 10; i++ {
		ps.Publish(ctx, topic, newTestMessage(fmt.Sprintf("msg-%d", i), "data"))
	}

	// Should not block
	time.Sleep(200 * time.Millisecond)
	_ = sub
}

// SpecTestOverflowBlock tests OverflowBlock policy
func SpecTestOverflowBlock(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := factory(t, func(c *Config) {
		c.BufferSize = 2
		c.OnFull = OverflowBlock
	})
	defer ps.Close()

	topic := "block-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Publish messages (will block after buffer is full)
	var published atomic.Int32
	go func() {
		for i := 0; i < 5; i++ {
			msg := newTestMessage(fmt.Sprintf("msg-%d", i), "data")
			if err := ps.Publish(ctx, topic, msg); err == nil {
				published.Add(1)
			}
		}
	}()

	// Consume messages slowly
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 5; i++ {
		select {
		case <-sub.Messages():
		case <-ctx.Done():
			break
		}
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(5), published.Load(), "all messages should eventually be published")
}

// SpecTestContextCancellation tests context cancellation during publish
func SpecTestContextCancellation(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithCancel(context.Background())

	ps := factory(t, func(c *Config) {
		c.BufferSize = 1
		c.OnFull = OverflowBlock
	})
	defer ps.Close()

	topic := "cancel-topic"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill the buffer
	msg := newTestMessage("msg", "data")
	err = ps.Publish(ctx, topic, msg)
	require.NoError(t, err)

	// Cancel context while blocking
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// This should return context.Canceled
	err = ps.Publish(ctx, topic, newTestMessage("msg2", "data"))
	assert.ErrorIs(t, err, context.Canceled)

	_ = sub
}

// SpecTestSubscriptionClosedDuringPublish tests race condition handling
func SpecTestSubscriptionClosedDuringPublish(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t, func(c *Config) {
		c.BufferSize = 1
		c.OnFull = OverflowBlock
	})
	defer ps.Close()

	topic := "close-during-publish"

	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Fill buffer
	err = ps.Publish(ctx, topic, newTestMessage("msg1", "data"))
	require.NoError(t, err)

	// Close subscription while publish is blocked
	go func() {
		time.Sleep(50 * time.Millisecond)
		sub.Unsubscribe()
	}()

	// This publish should handle the subscription being closed
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	// Should not block forever
	ps.Publish(ctxWithTimeout, topic, newTestMessage("msg2", "data"))
}

// SpecTestClosedSubscriptionFilter tests that closed subscriptions don't receive messages
func SpecTestClosedSubscriptionFilter(t *testing.T, factory PubSubFactory) {
	ctx := context.Background()

	ps := factory(t)
	defer ps.Close()

	topic := "filter-topic"

	// Create two subscriptions
	sub1, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	sub2, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Close one subscription
	sub1.Unsubscribe()
	time.Sleep(100 * time.Millisecond)

	// Publish should only go to sub2
	err = ps.Publish(ctx, topic, newTestMessage("msg", "data"))
	require.NoError(t, err)

	// sub2 should receive the message
	select {
	case <-sub2.Messages():
		// Good
	case <-time.After(time.Second):
		t.Fatal("sub2 should have received the message")
	}
}

// ==================== Concurrency Tests ====================

// SpecTestConcurrentPublish tests concurrent publishing
func SpecTestConcurrentPublish(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ps := factory(t, func(c *Config) {
		c.BufferSize = 10000
	})
	defer ps.Close()

	topic := "concurrent-publish-topic"
	numPublishers := 10
	messagesPerPublisher := 1000
	totalMessages := numPublishers * messagesPerPublisher

	// Subscribe and count messages
	sub, err := ps.Subscribe(ctx, topic)
	require.NoError(t, err)

	var received atomic.Int64
	done := make(chan struct{})

	go func() {
		for range sub.Messages() {
			received.Add(1)
			if received.Load() >= int64(totalMessages) {
				close(done)
				return
			}
		}
	}()

	// Concurrent publishers
	var wg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			for j := 0; j < messagesPerPublisher; j++ {
				msg := newTestMessage(
					fmt.Sprintf("pub-%d-msg-%d", publisherID, j),
					"concurrent data",
				)
				if err := ps.Publish(ctx, topic, msg); err != nil {
					t.Errorf("publish error: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Wait for all messages to be received
	select {
	case <-done:
		assert.Equal(t, int64(totalMessages), received.Load())
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout: received %d of %d messages", received.Load(), totalMessages)
	}
}

// SpecTestConcurrentSubscribeUnsubscribe tests concurrent subscribe/unsubscribe
func SpecTestConcurrentSubscribeUnsubscribe(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ps := factory(t)
	defer ps.Close()

	topic := "concurrent-sub-unsub-topic"
	numGoroutines := 50
	iterations := 100

	var wg sync.WaitGroup
	var subscribeCount atomic.Int64
	var unsubscribeCount atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sub, err := ps.Subscribe(ctx, topic)
				if err != nil {
					if err == ErrPubSubClosed {
						return
					}
					continue
				}
				subscribeCount.Add(1)

				// Random delay
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

				if err := sub.Unsubscribe(); err == nil {
					unsubscribeCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	// All subscriptions should be cleaned up
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, ps.SubscriberCount(topic), "all subscriptions should be cleaned up")
	t.Logf("subscribed: %d, unsubscribed: %d", subscribeCount.Load(), unsubscribeCount.Load())
}

// SpecTestConcurrentPublishSubscribe tests concurrent publish and subscribe
func SpecTestConcurrentPublishSubscribe(t *testing.T, factory PubSubFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ps := factory(t, func(c *Config) {
		c.BufferSize = 1000
	})
	defer ps.Close()

	topic := "concurrent-pub-sub-topic"
	numPublishers := 5
	numSubscribers := 5
	messagesPerPublisher := 500

	var totalReceived atomic.Int64
	var wg sync.WaitGroup

	// Start subscribers
	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func(subID int) {
			defer wg.Done()
			sub, err := ps.Subscribe(ctx, topic)
			if err != nil {
				return
			}

			for {
				select {
				case _, ok := <-sub.Messages():
					if !ok {
						return
					}
					totalReceived.Add(1)
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Wait for subscribers to be ready
	time.Sleep(100 * time.Millisecond)

	// Start publishers
	var publishWg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		publishWg.Add(1)
		go func(pubID int) {
			defer publishWg.Done()
			for j := 0; j < messagesPerPublisher; j++ {
				msg := newTestMessage(fmt.Sprintf("msg-%d-%d", pubID, j), "data")
				ps.Publish(ctx, topic, msg)
			}
		}(i)
	}

	publishWg.Wait()
	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()

	// Each subscriber should receive all messages (fan-out)
	expectedMin := int64(numPublishers * messagesPerPublisher * numSubscribers / 2) // Allow some drops
	assert.Greater(t, totalReceived.Load(), expectedMin,
		"should receive significant portion of messages")
	t.Logf("total received: %d (expected around %d)",
		totalReceived.Load(), numPublishers*messagesPerPublisher*numSubscribers)
}

// ==================== Benchmark Tests ====================

// SpecBenchmarkPublish benchmarks single publisher
func SpecBenchmarkPublish(b *testing.B, factory func(b *testing.B) PubSub) {
	ctx := context.Background()

	ps := factory(b)
	defer ps.Close()

	ps.Subscribe(ctx, "bench-topic")

	msg := newTestMessage("bench-msg", "benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Publish(ctx, "bench-topic", msg)
	}
}

// SpecBenchmarkFanOut benchmarks fan-out to multiple subscribers
func SpecBenchmarkFanOut(b *testing.B, factory func(b *testing.B) PubSub, numSubscribers int) {
	ctx := context.Background()

	ps := factory(b)
	defer ps.Close()

	for i := 0; i < numSubscribers; i++ {
		sub, _ := ps.Subscribe(ctx, "bench-fanout-topic")
		go func(s Subscription) {
			for range s.Messages() {
			}
		}(sub)
	}

	msg := newTestMessage("bench-msg", "benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Publish(ctx, "bench-fanout-topic", msg)
	}
}

// SpecBenchmarkPublishNoSubscribers benchmarks publishing without subscribers
func SpecBenchmarkPublishNoSubscribers(b *testing.B, factory func(b *testing.B) PubSub) {
	ctx := context.Background()

	ps := factory(b)
	defer ps.Close()

	msg := newTestMessage("bench-msg", "benchmark data")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ps.Publish(ctx, "no-sub-topic", msg)
		}
	})
}

// SpecBenchmarkSubscribe benchmarks subscribe/unsubscribe
func SpecBenchmarkSubscribe(b *testing.B, factory func(b *testing.B) PubSub) {
	ctx := context.Background()

	ps := factory(b)
	defer ps.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub, _ := ps.Subscribe(ctx, "bench-sub-topic")
		sub.Unsubscribe()
	}
}

// SpecBenchmarkPublishBatch benchmarks batch publishing with pipelining
func SpecBenchmarkPublishBatch(b *testing.B, factory func(b *testing.B) PubSub, batchSize int) {
	ctx := context.Background()

	ps := factory(b)
	defer ps.Close()

	sub, _ := ps.Subscribe(ctx, "bench-batch-topic")
	go func() {
		for range sub.Messages() {
		}
	}()

	// Prepare batch of messages
	msgs := make([]message.Message[any], batchSize)
	for i := range batchSize {
		msgs[i] = newTestMessage(fmt.Sprintf("batch-msg-%d", i), "benchmark data")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ps.PublishBatch(ctx, "bench-batch-topic", msgs)
	}
}

// SpecBenchmarkPublishBatchVsSequential compares batch vs sequential publishing
func SpecBenchmarkPublishBatchVsSequential(b *testing.B, factory func(b *testing.B) PubSub, batchSize int) {
	ctx := context.Background()

	// Prepare messages
	msgs := make([]message.Message[any], batchSize)
	for i := range batchSize {
		msgs[i] = newTestMessage(fmt.Sprintf("msg-%d", i), "benchmark data")
	}

	b.Run("Sequential", func(b *testing.B) {
		ps := factory(b)
		defer ps.Close()

		sub, _ := ps.Subscribe(ctx, "bench-seq-topic")
		go func() {
			for range sub.Messages() {
			}
		}()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			for _, msg := range msgs {
				ps.Publish(ctx, "bench-seq-topic", msg)
			}
		}
	})

	b.Run("Batch", func(b *testing.B) {
		ps := factory(b)
		defer ps.Close()

		sub, _ := ps.Subscribe(ctx, "bench-batch-topic")
		go func() {
			for range sub.Messages() {
			}
		}()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps.PublishBatch(ctx, "bench-batch-topic", msgs)
		}
	})
}
