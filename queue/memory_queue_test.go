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

// TestMemoryQueueGracefulCloseNoWaitGroupPanic tests that GracefulClose does not cause
// "sync: WaitGroup is reused before previous Wait has returned" panic.
// This test reproduces a race condition where:
// 1. GracefulClose() sets closing=true and calls Wait()
// 2. run() goroutine checks ExitChannel() (not closed yet) and continues
// 3. run() calls AddInflight() while Wait() is already running
// This violates WaitGroup usage rules and causes panic.
func TestMemoryQueueGracefulCloseNoWaitGroupPanic(t *testing.T) {
	// Run multiple iterations to increase chance of hitting the race condition
	for i := 0; i < 100; i++ {
		q, err := NewMemoryQueue("graceful-close-test", NewJsonMessage([]byte{}), queueOptions)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()

		// Subscribe to process messages
		processed := make(chan struct{}, 1000)
		q.Subscribe(ctx, func(ctx context.Context, msg Message[[]byte]) error {
			// Simulate some processing time to increase race condition window
			time.Sleep(time.Microsecond)
			select {
			case processed <- struct{}{}:
			default:
			}
			return nil
		})

		// Enqueue multiple messages rapidly
		for j := 0; j < 10; j++ {
			_ = q.Enqueue(ctx, []byte("test-data"))
		}

		// Give some time for processing to start
		time.Sleep(time.Millisecond)

		// Close the queue - this should NOT panic
		// The bug: run() might call AddInflight() after GracefulClose() starts Wait()
		q.Close()
	}
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
