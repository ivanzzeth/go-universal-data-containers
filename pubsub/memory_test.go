package pubsub

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func memoryFactory(t testing.TB, opts ...func(*Config)) PubSub {
	cfg := Config{
		Backend:    BackendMemory,
		BufferSize: 100,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	ps, err := NewPubSub(cfg)
	if err != nil {
		t.Fatalf("failed to create memory pubsub: %v", err)
	}
	return ps
}

func memoryTestFactory(t *testing.T, opts ...func(*Config)) PubSub {
	return memoryFactory(t, opts...)
}

func memoryBenchFactory(b *testing.B) PubSub {
	return memoryFactory(b)
}

// ==================== Spec Tests ====================

func TestMemoryPubSub_BasicPublishSubscribe(t *testing.T) {
	SpecTestBasicPublishSubscribe(t, memoryTestFactory)
}

func TestMemoryPubSub_FanOut(t *testing.T) {
	SpecTestFanOut(t, memoryTestFactory)
}

func TestMemoryPubSub_NoSubscribers(t *testing.T) {
	SpecTestNoSubscribers(t, memoryTestFactory)
}

func TestMemoryPubSub_Unsubscribe(t *testing.T) {
	SpecTestUnsubscribe(t, memoryTestFactory)
}

func TestMemoryPubSub_SubscribeWithHandler(t *testing.T) {
	SpecTestSubscribeWithHandler(t, memoryTestFactory)
}

func TestMemoryPubSub_HandlerError(t *testing.T) {
	SpecTestHandlerError(t, memoryTestFactory)
}

func TestMemoryPubSub_Topics(t *testing.T) {
	SpecTestTopics(t, memoryTestFactory)
}

func TestMemoryPubSub_SubscriberCount(t *testing.T) {
	SpecTestSubscriberCount(t, memoryTestFactory)
}

func TestMemoryPubSub_Close(t *testing.T) {
	SpecTestClose(t, memoryTestFactory)
}

func TestMemoryPubSub_DoubleClose(t *testing.T) {
	SpecTestDoubleClose(t, memoryTestFactory)
}

func TestMemoryPubSub_Validation(t *testing.T) {
	SpecTestValidation(t, memoryTestFactory)
}

func TestMemoryPubSub_PublishBatch(t *testing.T) {
	SpecTestPublishBatch(t, memoryTestFactory)
}

func TestMemoryPubSub_OverflowDrop(t *testing.T) {
	SpecTestOverflowDrop(t, memoryTestFactory)
}

func TestMemoryPubSub_OverflowBlock(t *testing.T) {
	SpecTestOverflowBlock(t, memoryTestFactory)
}

func TestMemoryPubSub_ContextCancellation(t *testing.T) {
	SpecTestContextCancellation(t, memoryTestFactory)
}

func TestMemoryPubSub_SubscriptionClosedDuringPublish(t *testing.T) {
	SpecTestSubscriptionClosedDuringPublish(t, memoryTestFactory)
}

func TestMemoryPubSub_ClosedSubscriptionFilter(t *testing.T) {
	SpecTestClosedSubscriptionFilter(t, memoryTestFactory)
}

// ==================== Concurrency Tests ====================

func TestMemoryPubSub_ConcurrentPublish(t *testing.T) {
	SpecTestConcurrentPublish(t, memoryTestFactory)
}

func TestMemoryPubSub_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	SpecTestConcurrentSubscribeUnsubscribe(t, memoryTestFactory)
}

func TestMemoryPubSub_ConcurrentPublishSubscribe(t *testing.T) {
	SpecTestConcurrentPublishSubscribe(t, memoryTestFactory)
}

// ==================== Benchmarks ====================

func BenchmarkMemoryPubSub_Publish(b *testing.B) {
	SpecBenchmarkPublish(b, memoryBenchFactory)
}

func BenchmarkMemoryPubSub_FanOut10(b *testing.B) {
	SpecBenchmarkFanOut(b, memoryBenchFactory, 10)
}

func BenchmarkMemoryPubSub_FanOut100(b *testing.B) {
	SpecBenchmarkFanOut(b, memoryBenchFactory, 100)
}

func BenchmarkMemoryPubSub_PublishNoSubscribers(b *testing.B) {
	SpecBenchmarkPublishNoSubscribers(b, memoryBenchFactory)
}

func BenchmarkMemoryPubSub_Subscribe(b *testing.B) {
	SpecBenchmarkSubscribe(b, memoryBenchFactory)
}

// ==================== Additional Memory-Specific Tests ====================

func TestMemoryPubSub_SubscriptionString(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "string-topic")
	require.NoError(t, err)

	// Test String() method
	ms := sub.(*memorySubscription)
	str := ms.String()
	assert.Contains(t, str, "Subscription{")
	assert.Contains(t, str, "string-topic")
}
