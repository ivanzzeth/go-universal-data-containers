package queue

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// QueueFactory is a function type for creating SafeQueue instances for benchmarking
type QueueFactory func(name string) (SafeQueue[[]byte], error)

// benchmarkOptions returns standard benchmark configuration as Option slice
func benchmarkOptions() []Option {
	return []Option{
		WithMaxSize(UnlimitedSize),
		WithMaxHandleFailures(3),
		WithPollInterval(DefaultPollInterval),
		WithMaxRetries(DefaultMaxRetries),
		WithConsumerCount(3),
		WithMessageIDGenerator(DefaultOptions.MessageIDGenerator),
	}
}

// SpecBenchmarkEnqueue benchmarks the Enqueue operation
func SpecBenchmarkEnqueue(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-enqueue")
	require.NoError(b, err)
	defer q.Close()

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

// SpecBenchmarkDequeue benchmarks the Dequeue operation
func SpecBenchmarkDequeue(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-dequeue")
	require.NoError(b, err)
	defer q.Close()

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

// SpecBenchmarkEnqueueDequeue benchmarks Enqueue+Dequeue cycle
func SpecBenchmarkEnqueueDequeue(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-cycle")
	require.NoError(b, err)
	defer q.Close()

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

// SpecBenchmarkEnqueueWithLogger benchmarks Enqueue with logger
func SpecBenchmarkEnqueueWithLogger(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-enqueue-logger")
	require.NoError(b, err)
	defer q.Close()

	// Add logger to existing queue (if it's a SimpleQueue, we can unwrap it)
	if sq, ok := q.(*SimpleQueue[[]byte]); ok {
		logger := zerolog.New(os.Stderr).Level(zerolog.Disabled) // Disable logging for benchmark
		qWithLogger, err := NewSimpleQueue(sq.Unwrap(), WithLogger(&logger))
		require.NoError(b, err)
		q = qWithLogger
	}

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

// SpecBenchmarkConcurrentEnqueue benchmarks concurrent Enqueue
func SpecBenchmarkConcurrentEnqueue(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-concurrent-enqueue")
	require.NoError(b, err)
	defer q.Close()

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

// SpecBenchmarkConcurrentDequeue benchmarks concurrent Dequeue
func SpecBenchmarkConcurrentDequeue(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-concurrent-dequeue")
	require.NoError(b, err)
	defer q.Close()

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

// SpecBenchmarkSubscribe benchmarks Subscribe message handling
func SpecBenchmarkSubscribe(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-subscribe")
	require.NoError(b, err)
	defer q.Close()

	data := []byte("test message")
	ctx := context.Background()

	var wg sync.WaitGroup

	// Subscribe with handler
	q.Subscribe(func(msg Message[[]byte]) error {
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

// SpecBenchmarkSubscribeWithError benchmarks Subscribe with error handling
func SpecBenchmarkSubscribeWithError(b *testing.B, factory QueueFactory) {
	q, err := factory("bench-subscribe-error")
	require.NoError(b, err)
	defer q.Close()

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

// SpecTestPerformanceEnqueue tests that Enqueue completes within threshold
// threshold is optional, defaults to 20us for memory queues, 100us for Redis queues
func SpecTestPerformanceEnqueue(t *testing.T, factory QueueFactory, threshold ...time.Duration) {
	q, err := factory("perf-enqueue")
	require.NoError(t, err)
	defer q.Close()

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

	thresh := 20 * time.Microsecond
	if len(threshold) > 0 {
		thresh = threshold[0]
	}

	t.Logf("Enqueue performance: %d iterations in %v, avg: %v per operation", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, thresh, "Enqueue should complete within %v on average", thresh)
}

// SpecTestPerformanceDequeue tests that Dequeue completes within threshold
// threshold is optional, defaults to 20us for memory queues, 100us for Redis queues
func SpecTestPerformanceDequeue(t *testing.T, factory QueueFactory, threshold ...time.Duration) {
	q, err := factory("perf-dequeue")
	require.NoError(t, err)
	defer q.Close()

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

	thresh := 20 * time.Microsecond
	if len(threshold) > 0 {
		thresh = threshold[0]
	}

	t.Logf("Dequeue performance: %d iterations in %v, avg: %v per operation", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, thresh, "Dequeue should complete within %v on average", thresh)
}

// SpecTestPerformanceCycle tests that Enqueue+Dequeue cycle completes within threshold
// threshold is optional, defaults to 20us for memory queues, 200us for Redis queues
func SpecTestPerformanceCycle(t *testing.T, factory QueueFactory, threshold ...time.Duration) {
	q, err := factory("perf-cycle")
	require.NoError(t, err)
	defer q.Close()

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

	thresh := 20 * time.Microsecond
	if len(threshold) > 0 {
		thresh = threshold[0]
	}

	t.Logf("Enqueue+Dequeue cycle performance: %d iterations in %v, avg: %v per cycle", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, thresh, "Enqueue+Dequeue cycle should complete within %v on average", thresh)
}

// SpecTestPerformanceSubscribe tests that Subscribe completes within 20ms
func SpecTestPerformanceSubscribe(t *testing.T, factory QueueFactory) {
	q, err := factory("perf-subscribe")
	require.NoError(t, err)
	defer q.Close()

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

// SpecTestPerformanceWithLogger tests performance with logger enabled
// threshold is optional, defaults to 30us for memory queues, 150us for Redis queues
func SpecTestPerformanceWithLogger(t *testing.T, factory QueueFactory, threshold ...time.Duration) {
	q, err := factory("perf-logger")
	require.NoError(t, err)
	defer q.Close()

	// Add logger to existing queue (if it's a SimpleQueue, we can unwrap it)
	if sq, ok := q.(*SimpleQueue[[]byte]); ok {
		logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
		qWithLogger, err := NewSimpleQueue(sq.Unwrap(), WithLogger(&logger))
		require.NoError(t, err)
		q = qWithLogger
	}

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

	thresh := 30 * time.Microsecond
	if len(threshold) > 0 {
		thresh = threshold[0]
	}

	t.Logf("Enqueue with logger performance: %d iterations in %v, avg: %v per operation", iterations, duration, avgDuration)
	assert.Less(t, avgDuration, thresh, "Enqueue with logger should complete within %v on average", thresh)
}

// ========== MemoryQueue Benchmarks ==========

func BenchmarkMemoryQueueEnqueue(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkEnqueue(b, factory)
}

func BenchmarkMemoryQueueDequeue(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkDequeue(b, factory)
}

func BenchmarkMemoryQueueEnqueueDequeue(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkEnqueueDequeue(b, factory)
}

func BenchmarkMemoryQueueEnqueueWithLogger(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkEnqueueWithLogger(b, factory)
}

func BenchmarkMemoryQueueConcurrentEnqueue(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkConcurrentEnqueue(b, factory)
}

func BenchmarkMemoryQueueConcurrentDequeue(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkConcurrentDequeue(b, factory)
}

func BenchmarkMemoryQueueSubscribe(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkSubscribe(b, factory)
}

func BenchmarkMemoryQueueSubscribeWithError(b *testing.B) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecBenchmarkSubscribeWithError(b, factory)
}

// ========== RedisQueue Benchmarks ==========

func BenchmarkRedisQueueEnqueue(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkEnqueue(b, factory)
}

func BenchmarkRedisQueueDequeue(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkDequeue(b, factory)
}

func BenchmarkRedisQueueEnqueueDequeue(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkEnqueueDequeue(b, factory)
}

func BenchmarkRedisQueueEnqueueWithLogger(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkEnqueueWithLogger(b, factory)
}

func BenchmarkRedisQueueConcurrentEnqueue(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkConcurrentEnqueue(b, factory)
}

func BenchmarkRedisQueueConcurrentDequeue(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkConcurrentDequeue(b, factory)
}

func BenchmarkRedisQueueSubscribe(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkSubscribe(b, factory)
}

func BenchmarkRedisQueueSubscribeWithError(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecBenchmarkSubscribeWithError(b, factory)
}

// ========== Performance Tests ==========

func TestMemoryQueuePerformanceEnqueue(t *testing.T) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecTestPerformanceEnqueue(t, factory)
}

func TestMemoryQueuePerformanceDequeue(t *testing.T) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecTestPerformanceDequeue(t, factory)
}

func TestMemoryQueuePerformanceCycle(t *testing.T) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecTestPerformanceCycle(t, factory)
}

func TestMemoryQueuePerformanceSubscribe(t *testing.T) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecTestPerformanceSubscribe(t, factory)
}

func TestMemoryQueuePerformanceWithLogger(t *testing.T) {
	factory := func(name string) (SafeQueue[[]byte], error) {
		mq, err := NewMemoryQueue(name, NewJsonMessage([]byte{}), benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return NewSimpleQueue(mq)
	}
	SpecTestPerformanceWithLogger(t, factory)
}

func TestRedisQueuePerformanceEnqueue(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	// Redis operations involve network I/O, so we use a more lenient threshold (100us)
	SpecTestPerformanceEnqueue(t, factory, 100*time.Microsecond)
}

func TestRedisQueuePerformanceDequeue(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	// Redis operations involve network I/O, so we use a more lenient threshold (100us)
	SpecTestPerformanceDequeue(t, factory, 100*time.Microsecond)
}

func TestRedisQueuePerformanceCycle(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	// Redis operations involve network I/O, so we use a more lenient threshold (200us)
	SpecTestPerformanceCycle(t, factory, 200*time.Microsecond)
}

func TestRedisQueuePerformanceSubscribe(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	SpecTestPerformanceSubscribe(t, factory)
}

func TestRedisQueuePerformanceWithLogger(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	factory := func(name string) (SafeQueue[[]byte], error) {
		f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
		q, err := f.GetOrCreateSafe(name, benchmarkOptions()...)
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	// Redis operations involve network I/O, so we use a more lenient threshold (150us)
	SpecTestPerformanceWithLogger(t, factory, 150*time.Microsecond)
}
