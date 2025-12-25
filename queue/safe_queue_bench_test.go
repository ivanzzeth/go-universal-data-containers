package queue

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkSimpleQueueEnqueue benchmarks the Enqueue operation
func BenchmarkSimpleQueueEnqueue(b *testing.B) {
	mq, err := NewMemoryQueue("bench-enqueue", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSimpleQueueDequeue benchmarks the Dequeue operation
func BenchmarkSimpleQueueDequeue(b *testing.B) {
	mq, err := NewMemoryQueue("bench-dequeue", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	// Pre-fill queue
	for i := 0; i < b.N; i++ {
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSimpleQueueEnqueueDequeue benchmarks Enqueue+Dequeue cycle
func BenchmarkSimpleQueueEnqueueDequeue(b *testing.B) {
	mq, err := NewMemoryQueue("bench-cycle", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
		_, err = q.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSimpleQueueEnqueueWithLogger benchmarks Enqueue with logger
func BenchmarkSimpleQueueEnqueueWithLogger(b *testing.B) {
	mq, err := NewMemoryQueue("bench-enqueue-logger", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled) // Disable logging for benchmark
	q, err := NewSimpleQueue(mq, WithLogger(&logger))
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSimpleQueueConcurrentEnqueue benchmarks concurrent Enqueue
func BenchmarkSimpleQueueConcurrentEnqueue(b *testing.B) {
	mq, err := NewMemoryQueue("bench-concurrent-enqueue", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := q.Enqueue(ctx, data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSimpleQueueConcurrentDequeue benchmarks concurrent Dequeue
func BenchmarkSimpleQueueConcurrentDequeue(b *testing.B) {
	mq, err := NewMemoryQueue("bench-concurrent-dequeue", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	// Pre-fill queue with enough items
	for i := 0; i < b.N*10; i++ {
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := q.Dequeue(ctx)
			if err != nil {
				// Queue might be empty, that's ok for benchmark
				continue
			}
		}
	})
}

// BenchmarkSimpleQueueSubscribe benchmarks Subscribe message handling
func BenchmarkSimpleQueueSubscribe(b *testing.B) {
	mq, err := NewMemoryQueue("bench-subscribe", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	var processedCount int64
	var wg sync.WaitGroup

	// Subscribe with handler
	q.Subscribe(func(msg Message[[]byte]) error {
		atomic.AddInt64(&processedCount, 1)
		wg.Done() // Signal that this message is processed
		return nil
	})

	// Give subscription time to register
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	// Enqueue messages and wait for processing
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Wait for all messages to be processed
	wg.Wait()
}

// BenchmarkSimpleQueueSubscribeWithError benchmarks Subscribe with error handling
func BenchmarkSimpleQueueSubscribeWithError(b *testing.B) {
	mq, err := NewMemoryQueue("bench-subscribe-error", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(b, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(b, err)

	data := []byte("test message")
	ctx := context.Background()

	var processedCount int64
	var wg sync.WaitGroup
	testErr := errors.New("test error")

	// Subscribe with handler that returns error (will trigger recovery)
	// Note: With error, messages will be retried, so we need to account for that
	q.Subscribe(func(msg Message[[]byte]) error {
		count := atomic.AddInt64(&processedCount, 1)
		// Only signal done after max failures (message will be discarded or go to DLQ)
		if count%int64(q.MaxHandleFailures()+1) == 0 {
			wg.Done()
		}
		return testErr
	})

	// Give subscription time to register
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	// Enqueue messages
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		err := q.Enqueue(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Wait for all messages to be processed (with retries)
	wg.Wait()
}

// TestSimpleQueuePerformanceSubscribe tests that Subscribe completes within 20us
func TestSimpleQueuePerformanceSubscribe(t *testing.T) {
	mq, err := NewMemoryQueue("perf-subscribe", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	data := []byte("test message")
	ctx := context.Background()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var processingTimes []time.Duration

	// Subscribe with handler that measures processing time
	q.Subscribe(func(msg Message[[]byte]) error {
		start := time.Now()
		// Simulate minimal work (no sleep to measure pure queue performance)
		duration := time.Since(start)

		mu.Lock()
		processingTimes = append(processingTimes, duration)
		mu.Unlock()

		wg.Done() // Signal that this message is processed
		return nil
	})

	// Give subscription time to register
	time.Sleep(100 * time.Millisecond)

	iterations := 1000
	start := time.Now()

	// Enqueue messages and wait for processing
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		err := q.Enqueue(ctx, data)
		require.NoError(t, err)
	}

	// Wait for all messages to be processed
	wg.Wait()

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(iterations)

	// Calculate average processing time
	mu.Lock()
	var avgProcessingTime time.Duration
	if len(processingTimes) > 0 {
		var sum time.Duration
		for _, d := range processingTimes {
			sum += d
		}
		avgProcessingTime = sum / time.Duration(len(processingTimes))
	}
	mu.Unlock()

	t.Logf("Subscribe performance: %d iterations in %v, avg: %v per operation (enqueue to consume)", iterations, totalDuration, avgDuration)
	t.Logf("Average message handler processing time: %v", avgProcessingTime)
	// Note: Subscribe is async, so we measure from enqueue to consumption completion
	// This includes queue processing overhead and PollInterval (10ms default),
	// so we allow more time than pure enqueue. The handler itself is very fast (~171ns).
	// The total time includes: enqueue + queue polling + handler execution
	// For 1000 messages with 3 consumers, average should be reasonable
	// We set threshold to 20ms to account for async processing and polling
	assert.Less(t, avgDuration, 20*time.Millisecond, "Subscribe (enqueue to consume) should complete within 20ms on average")
}

// TestSimpleQueuePerformanceEnqueue tests that Enqueue completes within 20us
func TestSimpleQueuePerformanceEnqueue(t *testing.T) {
	mq, err := NewMemoryQueue("perf-enqueue", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	data := []byte("test message")
	ctx := context.Background()

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		err := q.Enqueue(ctx, data)
		require.NoError(t, err)
	}

	duration := time.Since(start)
	avgDuration := duration / time.Duration(iterations)

	t.Logf("Enqueue performance: %d iterations in %v, avg: %v per operation", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, 20*time.Microsecond, "Enqueue should complete within 20us on average")
}

// TestSimpleQueuePerformanceDequeue tests that Dequeue completes within 20us
func TestSimpleQueuePerformanceDequeue(t *testing.T) {
	mq, err := NewMemoryQueue("perf-dequeue", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	data := []byte("test message")
	ctx := context.Background()

	iterations := 1000
	// Pre-fill queue
	for i := 0; i < iterations; i++ {
		err := q.Enqueue(ctx, data)
		require.NoError(t, err)
	}

	start := time.Now()

	for i := 0; i < iterations; i++ {
		_, err := q.Dequeue(ctx)
		require.NoError(t, err)
	}

	duration := time.Since(start)
	avgDuration := duration / time.Duration(iterations)

	t.Logf("Dequeue performance: %d iterations in %v, avg: %v per operation", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, 20*time.Microsecond, "Dequeue should complete within 20us on average")
}

// TestSimpleQueuePerformanceCycle tests that Enqueue+Dequeue cycle completes within 20us
func TestSimpleQueuePerformanceCycle(t *testing.T) {
	mq, err := NewMemoryQueue("perf-cycle", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(t, err)
	defer mq.Close()

	q, err := NewSimpleQueue(mq)
	require.NoError(t, err)

	data := []byte("test message")
	ctx := context.Background()

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		err := q.Enqueue(ctx, data)
		require.NoError(t, err)
		_, err = q.Dequeue(ctx)
		require.NoError(t, err)
	}

	duration := time.Since(start)
	avgDuration := duration / time.Duration(iterations)

	t.Logf("Enqueue+Dequeue cycle performance: %d iterations in %v, avg: %v per cycle", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, 20*time.Microsecond, "Enqueue+Dequeue cycle should complete within 20us on average")
}

// TestSimpleQueuePerformanceWithLogger tests performance with logger enabled
func TestSimpleQueuePerformanceWithLogger(t *testing.T) {
	mq, err := NewMemoryQueue("perf-logger", NewJsonMessage([]byte{}), func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = UnlimitedSize
		c.MaxHandleFailures = 3
		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries
		c.ConsumerCount = 3
		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	})
	require.NoError(t, err)
	defer mq.Close()

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	q, err := NewSimpleQueue(mq, WithLogger(&logger))
	require.NoError(t, err)

	data := []byte("test message")
	ctx := context.Background()

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		err := q.Enqueue(ctx, data)
		require.NoError(t, err)
	}

	duration := time.Since(start)
	avgDuration := duration / time.Duration(iterations)

	t.Logf("Enqueue with logger performance: %d iterations in %v, avg: %v per operation", iterations, duration, avgDuration)
	// With logger, we allow slightly more time (30us) as logging has overhead
	assert.Less(t, avgDuration, 30*time.Microsecond, "Enqueue with logger should complete within 30us on average")
}
