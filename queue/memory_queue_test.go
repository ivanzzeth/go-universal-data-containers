package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryQueueSequencial(t *testing.T) {
	q, err := NewMemoryQueue("", NewJsonMessage([]byte{}), queueOptions)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestMemoryQueueConcurrent(t *testing.T) {
	q, err := NewMemoryQueue("", NewJsonMessage([]byte{}), queueOptions)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueConcurrent(t, q)
}

func TestMemoryQueueSubscribeHandleReachedMaxFailures(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueSubscribeHandleReachedMaxFailures(t, f)
}

func TestMemoryQueueSubscribe(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueSubscribe(t, f)
}

func TestMemoryQueueSubscribeWithConsumerCount(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueSubscribeWithConsumerCount(t, f)
}

func TestMemoryQueueTimeout(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueTimeout(t, f)
}

func TestMemoryQueueStressTest(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueStressTest(t, f)
}

func TestMemoryQueueErrorHandling(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueErrorHandling(t, f)
}

func TestMemoryQueueBlockingOperations(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueBlockingOperations(t, f)
}

func TestMemoryQueueBlockingWithContext(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueBlockingWithContext(t, f)
}

// TestMemoryQueueBEnqueueReturnAfterSuccess tests that BEnqueue returns after successful enqueue
// This test reproduces a bug where BEnqueue was missing "return nil" after successful Enqueue,
// causing it to loop infinitely and enqueue the same data multiple times.
func TestMemoryQueueBEnqueueReturnAfterSuccess(t *testing.T) {
	q, err := NewMemoryQueue("benqueue-return-test", NewJsonMessage([]byte{}), queueOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// BEnqueue should return immediately after successful enqueue
	err = q.BEnqueue(ctx, []byte("test-data"))
	if err != nil {
		t.Fatalf("BEnqueue failed: %v", err)
	}

	// Wait a short time to let any potential duplicate enqueues happen
	time.Sleep(100 * time.Millisecond)

	// Dequeue the first message
	msg, err := q.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if string(msg.Data()) != "test-data" {
		t.Fatalf("expected 'test-data', got %s", string(msg.Data()))
	}

	// Queue should be empty now - if BEnqueue had the bug, there would be more items
	_, err = q.Dequeue(context.Background())
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty (BEnqueue should only enqueue once), got: %v", err)
	}
}
