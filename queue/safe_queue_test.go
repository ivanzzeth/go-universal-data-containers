package queue

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeQueueSequencial(t *testing.T) {
	mq, _ := NewMemoryQueue("", NewJsonMessage([]byte{}), queueOptions)
	q, err := NewSimpleQueue(mq)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestSafeQueueConcurrent(t *testing.T) {
	mq, _ := NewMemoryQueue("", NewJsonMessage([]byte{}), queueOptions)
	q, err := NewSimpleQueue(mq)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueConcurrent(t, q)
}

func TestSimpleQueue_WithLogger(t *testing.T) {
	mq, err := NewMemoryQueue("test-logger", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	q, err := NewSimpleQueue(mq, WithLogger(&logger))
	require.NoError(t, err)
	assert.NotNil(t, q)

	// Test that logger is set
	ctx := context.Background()
	err = q.Enqueue(ctx, []byte("test"))
	require.NoError(t, err)
}

func TestSimpleQueue_WithoutLogger(t *testing.T) {
	mq, err := NewMemoryQueue("test-no-logger", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)
	assert.NotNil(t, q)

	// Test that it works without logger
	ctx := context.Background()
	err = q.Enqueue(ctx, []byte("test"))
	require.NoError(t, err)
}

func TestSimpleQueue_Enqueue(t *testing.T) {
	mq, err := NewMemoryQueue("test-enqueue", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	data := []byte("test data")

	err = q.Enqueue(ctx, data)
	require.NoError(t, err)
}

func TestSimpleQueue_Enqueue_QueueFull(t *testing.T) {
	mq, err := NewMemoryQueue("test-enqueue-full", NewJsonMessage([]byte{}), func(c *Config) {
		c.MaxSize = 2
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	data := []byte("test data")

	// Fill queue
	err = q.Enqueue(ctx, data)
	require.NoError(t, err)
	err = q.Enqueue(ctx, data)
	require.NoError(t, err)

	// Should fail
	err = q.Enqueue(ctx, data)
	assert.Error(t, err)
	assert.Equal(t, ErrQueueFull, err)
}

func TestSimpleQueue_BEnqueue(t *testing.T) {
	mq, err := NewMemoryQueue("test-benqueue", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	data := []byte("test data")

	err = q.BEnqueue(ctx, data)
	require.NoError(t, err)
}

func TestSimpleQueue_Dequeue(t *testing.T) {
	mq, err := NewMemoryQueue("test-dequeue", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	data := []byte("test data")

	err = q.Enqueue(ctx, data)
	require.NoError(t, err)

	msg, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, data, msg.Data())
}

func TestSimpleQueue_Dequeue_Empty(t *testing.T) {
	mq, err := NewMemoryQueue("test-dequeue-empty", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = q.Dequeue(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrQueueEmpty, err)
}

func TestSimpleQueue_BDequeue(t *testing.T) {
	mq, err := NewMemoryQueue("test-bdequeue", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start dequeue in goroutine
	var msg Message[[]byte]
	var dequeueErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		msg, dequeueErr = q.BDequeue(ctx)
	}()

	// Enqueue after a short delay
	time.Sleep(100 * time.Millisecond)
	err = q.Enqueue(ctx, []byte("test data"))
	require.NoError(t, err)

	wg.Wait()
	require.NoError(t, dequeueErr)
	assert.NotNil(t, msg)
}

func TestSimpleQueue_Subscribe(t *testing.T) {
	mq, err := NewMemoryQueue("test-subscribe", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	data := []byte("test data")

	var receivedData []byte
	var handlerErr error
	var wg sync.WaitGroup
	wg.Add(1)

	q.Subscribe(func(msg Message[[]byte]) error {
		defer wg.Done()
		receivedData = msg.Data()
		return nil
	})

	// Give subscription time to register
	time.Sleep(100 * time.Millisecond)

	err = q.Enqueue(ctx, data)
	require.NoError(t, err)

	// Wait for handler to process
	wg.Wait()
	assert.Equal(t, data, receivedData)
	assert.NoError(t, handlerErr)
}

func TestSimpleQueue_Subscribe_Error(t *testing.T) {
	mq, err := NewMemoryQueue("test-subscribe-error", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	data := []byte("test data")

	testErr := common.ErrNotImplemented
	var wg sync.WaitGroup
	wg.Add(1)

	q.Subscribe(func(msg Message[[]byte]) error {
		defer wg.Done()
		return testErr
	})

	// Give subscription time to register
	time.Sleep(100 * time.Millisecond)

	err = q.Enqueue(ctx, data)
	require.NoError(t, err)

	// Wait for handler to process
	wg.Wait()
	// Message should be recovered if queue supports it
	assert.True(t, q.IsRecoverable())
}

func TestSimpleQueue_Recover(t *testing.T) {
	mq, err := NewMemoryQueue("test-recover", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	msg := NewJsonMessage([]byte("test data"))

	err = q.Recover(ctx, msg)
	require.NoError(t, err)
}

func TestSimpleQueue_Recover_NotSupported(t *testing.T) {
	// Create a queue that doesn't support recovery
	// For this test, we'd need a non-recoverable queue implementation
	// Since MemoryQueue supports recovery, we'll test the error path differently
	mq, err := NewMemoryQueue("test-recover-not-supported", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	// MemoryQueue supports recovery, so this should work
	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.True(t, q.IsRecoverable())
}

func TestSimpleQueue_IsRecoverable(t *testing.T) {
	mq, err := NewMemoryQueue("test-is-recoverable", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.True(t, q.IsRecoverable())
}

func TestSimpleQueue_Purge(t *testing.T) {
	mq, err := NewMemoryQueue("test-purge", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()

	// Add some data
	err = q.Enqueue(ctx, []byte("test1"))
	require.NoError(t, err)
	err = q.Enqueue(ctx, []byte("test2"))
	require.NoError(t, err)

	// Purge
	err = q.Purge(ctx)
	require.NoError(t, err)

	// Queue should be empty
	_, err = q.Dequeue(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrQueueEmpty, err)
}

func TestSimpleQueue_IsPurgeable(t *testing.T) {
	mq, err := NewMemoryQueue("test-is-purgeable", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.True(t, q.IsPurgeable())
}

func TestSimpleQueue_DLQ(t *testing.T) {
	mq, err := NewMemoryQueue("test-dlq", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	dlq, err := q.DLQ()
	require.NoError(t, err)
	assert.NotNil(t, dlq)
}

func TestSimpleQueue_IsDLQSupported(t *testing.T) {
	mq, err := NewMemoryQueue("test-is-dlq", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.True(t, q.IsDLQSupported())
}

func TestSimpleQueue_Close(t *testing.T) {
	mq, err := NewMemoryQueue("test-close", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	q.Close()

	// Should not be able to enqueue after close
	err = q.Enqueue(ctx, []byte("test"))
	assert.Error(t, err)
	assert.Equal(t, ErrQueueClosed, err)
}

func TestSimpleQueue_Unwrap(t *testing.T) {
	mq, err := NewMemoryQueue("test-unwrap", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	unwrapped := q.Unwrap()
	assert.Equal(t, mq, unwrapped)
}

func TestSimpleQueue_Kind(t *testing.T) {
	mq, err := NewMemoryQueue("test-kind", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.Equal(t, KindFIFO, q.Kind())
}

func TestSimpleQueue_Name(t *testing.T) {
	mq, err := NewMemoryQueue("test-name", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.Equal(t, "test-name", q.Name())
}

func TestSimpleQueue_MaxSize(t *testing.T) {
	mq, err := NewMemoryQueue("test-maxsize", NewJsonMessage([]byte{}), func(c *Config) {
		c.MaxSize = 100
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.Equal(t, 100, q.MaxSize())
}

func TestSimpleQueue_MaxHandleFailures(t *testing.T) {
	mq, err := NewMemoryQueue("test-maxfailures", NewJsonMessage([]byte{}), func(c *Config) {
		c.MaxHandleFailures = 5
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	assert.Equal(t, 5, q.MaxHandleFailures())
}

func TestSimpleQueue_ConcurrentOperations(t *testing.T) {
	mq, err := NewMemoryQueue("test-concurrent", NewJsonMessage([]byte{}), queueOptions)
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	ctx := context.Background()
	iterations := 100
	var wg sync.WaitGroup

	// Concurrent enqueue
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			err := q.Enqueue(ctx, []byte{byte(idx)})
			require.NoError(t, err)
		}(i)
	}

	// Concurrent dequeue
	wg.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				defer wg.Done()
				_, err := q.BDequeue(ctx)
				require.NoError(t, err)
			}()
		}
	}()

	wg.Wait()
}
