package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiddleware_Retry(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "retry-topic")
	require.NoError(t, err)

	// Wrap with retry middleware
	publisher := ChainPublisher(ps, WithRetry(3, 10*time.Millisecond))

	msg := newTestMessage("retry-msg", "data")
	err = publisher.Publish(ctx, "retry-topic", msg)
	require.NoError(t, err)

	// Should receive message
	select {
	case <-sub.Messages():
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestMiddleware_RetryWithFailures(t *testing.T) {
	ctx := context.Background()

	var attempts atomic.Int32
	failingPublisher := &mockPublisher{
		publishFn: func(ctx context.Context, topic string, msg message.Message[any]) error {
			if attempts.Add(1) <= 2 {
				return errors.New("temporary error")
			}
			return nil
		},
	}

	publisher := ChainPublisher(failingPublisher, WithRetry(3, 10*time.Millisecond))

	msg := newTestMessage("msg", "data")
	err := publisher.Publish(ctx, "topic", msg)
	require.NoError(t, err)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestMiddleware_RetryExhausted(t *testing.T) {
	ctx := context.Background()

	failingPublisher := &mockPublisher{
		publishFn: func(ctx context.Context, topic string, msg message.Message[any]) error {
			return errors.New("permanent error")
		},
	}

	publisher := ChainPublisher(failingPublisher, WithRetry(2, 10*time.Millisecond))

	msg := newTestMessage("msg", "data")
	err := publisher.Publish(ctx, "topic", msg)
	assert.Error(t, err)
}

func TestMiddleware_RetryContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	failingPublisher := &mockPublisher{
		publishFn: func(ctx context.Context, topic string, msg message.Message[any]) error {
			return errors.New("error")
		},
	}

	publisher := ChainPublisher(failingPublisher, WithRetry(10, 100*time.Millisecond))

	// Cancel context after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	msg := newTestMessage("msg", "data")
	err := publisher.Publish(ctx, "topic", msg)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMiddleware_Timeout(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	publisher := ChainPublisher(ps, WithTimeout(time.Second))

	msg := newTestMessage("timeout-msg", "data")
	err = publisher.Publish(ctx, "timeout-topic", msg)
	require.NoError(t, err)
}

func TestMiddleware_TimeoutExceeded(t *testing.T) {
	ctx := context.Background()

	slowPublisher := &mockPublisher{
		publishFn: func(ctx context.Context, topic string, msg message.Message[any]) error {
			select {
			case <-time.After(time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	publisher := ChainPublisher(slowPublisher, WithTimeout(50*time.Millisecond))

	msg := newTestMessage("msg", "data")
	err := publisher.Publish(ctx, "topic", msg)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMiddleware_TimeoutBatch(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	publisher := ChainPublisher(ps, WithTimeout(time.Second))

	msgs := []message.Message[any]{
		newTestMessage("msg-1", "data"),
		newTestMessage("msg-2", "data"),
	}

	err = publisher.PublishBatch(ctx, "batch-topic", msgs)
	require.NoError(t, err)
}

func TestMiddleware_RetryBatch(t *testing.T) {
	ctx := context.Background()

	ps, err := NewPubSub(DefaultConfig())
	require.NoError(t, err)
	defer ps.Close()

	sub, err := ps.Subscribe(ctx, "retry-batch-topic")
	require.NoError(t, err)

	publisher := ChainPublisher(ps, WithRetry(2, 10*time.Millisecond))

	msgs := []message.Message[any]{
		newTestMessage("msg-1", "data"),
		newTestMessage("msg-2", "data"),
	}

	err = publisher.PublishBatch(ctx, "retry-batch-topic", msgs)
	require.NoError(t, err)

	// Should receive both messages
	for i := 0; i < 2; i++ {
		select {
		case <-sub.Messages():
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

func TestMiddleware_Logging(t *testing.T) {
	ctx := context.Background()

	var logged atomic.Int32
	logFn := func(format string, args ...any) {
		logged.Add(1)
	}

	handler := ChainHandler(
		func(ctx context.Context, msg message.Message[any]) error {
			return nil
		},
		WithLogging(logFn),
	)

	msg := newTestMessage("log-msg", "data")
	err := handler(ctx, msg)
	require.NoError(t, err)

	assert.Equal(t, int32(1), logged.Load())
}

func TestMiddleware_LoggingWithError(t *testing.T) {
	ctx := context.Background()

	var logs []string
	logFn := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	handler := ChainHandler(
		func(ctx context.Context, msg message.Message[any]) error {
			return fmt.Errorf("test error")
		},
		WithLogging(logFn),
	)

	msg := newTestMessage("log-error-msg", "data")
	err := handler(ctx, msg)
	assert.Error(t, err)

	assert.Len(t, logs, 1)
	assert.Contains(t, logs[0], "failed")
}

func TestMiddleware_Recovery(t *testing.T) {
	ctx := context.Background()

	var panicValue any
	handler := ChainHandler(
		func(ctx context.Context, msg message.Message[any]) error {
			panic("test panic")
		},
		WithRecovery(func(r any) {
			panicValue = r
		}),
	)

	msg := newTestMessage("panic-msg", "data")
	err := handler(ctx, msg)
	require.NoError(t, err) // Should not return error after recovery

	assert.Equal(t, "test panic", panicValue)
}

func TestMiddleware_RecoveryNilCallback(t *testing.T) {
	ctx := context.Background()

	handler := ChainHandler(
		func(ctx context.Context, msg message.Message[any]) error {
			panic("test panic")
		},
		WithRecovery(nil),
	)

	msg := newTestMessage("panic-msg", "data")
	// Should not panic even with nil callback
	assert.NotPanics(t, func() {
		handler(ctx, msg)
	})
}

func TestMiddleware_HandlerTimeout(t *testing.T) {
	ctx := context.Background()

	slowHandler := func(ctx context.Context, msg message.Message[any]) error {
		select {
		case <-time.After(time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	handler := ChainHandler(slowHandler, WithHandlerTimeout(50*time.Millisecond))

	msg := newTestMessage("timeout-msg", "data")
	err := handler(ctx, msg)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMiddleware_HandlerTimeoutSuccess(t *testing.T) {
	ctx := context.Background()

	fastHandler := func(ctx context.Context, msg message.Message[any]) error {
		return nil
	}

	handler := ChainHandler(fastHandler, WithHandlerTimeout(time.Second))

	msg := newTestMessage("fast-msg", "data")
	err := handler(ctx, msg)
	require.NoError(t, err)
}

func TestMiddleware_ChainMultiple(t *testing.T) {
	ctx := context.Background()

	var calls []string
	var mu sync.Mutex

	addCall := func(name string) {
		mu.Lock()
		calls = append(calls, name)
		mu.Unlock()
	}

	handler := ChainHandler(
		func(ctx context.Context, msg message.Message[any]) error {
			addCall("handler")
			return nil
		},
		func(next Handler) Handler {
			return func(ctx context.Context, msg message.Message[any]) error {
				addCall("middleware1-before")
				err := next(ctx, msg)
				addCall("middleware1-after")
				return err
			}
		},
		func(next Handler) Handler {
			return func(ctx context.Context, msg message.Message[any]) error {
				addCall("middleware2-before")
				err := next(ctx, msg)
				addCall("middleware2-after")
				return err
			}
		},
	)

	msg := newTestMessage("chain-msg", "data")
	err := handler(ctx, msg)
	require.NoError(t, err)

	expected := []string{
		"middleware1-before",
		"middleware2-before",
		"handler",
		"middleware2-after",
		"middleware1-after",
	}
	assert.Equal(t, expected, calls)
}

// mockPublisher is a mock implementation of Publisher for testing
type mockPublisher struct {
	publishFn      func(ctx context.Context, topic string, msg message.Message[any]) error
	publishBatchFn func(ctx context.Context, topic string, msgs []message.Message[any]) error
}

func (m *mockPublisher) Publish(ctx context.Context, topic string, msg message.Message[any]) error {
	if m.publishFn != nil {
		return m.publishFn(ctx, topic, msg)
	}
	return nil
}

func (m *mockPublisher) PublishBatch(ctx context.Context, topic string, msgs []message.Message[any]) error {
	if m.publishBatchFn != nil {
		return m.publishBatchFn(ctx, topic, msgs)
	}
	return nil
}

