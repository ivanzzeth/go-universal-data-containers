package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

var (
	queueOptions = func(c *Config) {
		c.LockerGenerator = locker.NewMemoryLockerGenerator()
		c.MaxSize = 255
		c.MaxHandleFailures = 3

		c.PollInterval = DefaultPollInterval
		c.MaxRetries = DefaultMaxRetries

		c.ConsumerCount = 3

		c.MessageIDGenerator = DefaultOptions.MessageIDGenerator
	}
)

// CallbackChecker is an interface for queues that can report callback registration status.
type CallbackChecker interface {
	HasCallbacks() bool
}

// waitForCallbacks waits until the queue has registered callbacks or timeout.
// Returns true if callbacks are registered, false if timeout.
func waitForCallbacks(t *testing.T, q interface{}, timeout time.Duration) bool {
	t.Helper()

	var checker CallbackChecker

	// Try to extract the underlying queue that implements CallbackChecker
	switch v := q.(type) {
	case *SimpleQueue[[]byte]:
		if c, ok := v.Unwrap().(CallbackChecker); ok {
			checker = c
		}
	case CallbackChecker:
		checker = v
	}

	if checker == nil {
		// Fallback to time.Sleep if we can't check callbacks
		// TODO: This should be removed once all queue types support CallbackChecker
		time.Sleep(100 * time.Millisecond)
		return true
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if checker.HasCallbacks() {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}

	return checker.HasCallbacks()
}

func init() {
	// handler := log.NewTerminalHandler(os.Stdout, true)
	// logger := log.NewLogger(handler)
	// log.SetDefault(logger)
}

func SpecTestQueueSequencial(t *testing.T, q Queue[[]byte]) {
	maxSize := getMaxSize(q)
	for i := 0; i < maxSize; i++ {
		err := q.Enqueue(context.Background(), []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	if q.MaxSize() != UnlimitedSize {
		err := q.Enqueue(context.Background(), []byte{byte(maxSize + 1)})
		if !errors.Is(err, ErrQueueFull) {
			t.Fatal("expected", ErrQueueFull, "got", err)
		}
	}

	allData := [][]byte{}
	for i := 0; i < maxSize; i++ {
		data, err := q.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		allData = append(allData, data.Data())
	}

	sort.Slice(allData, func(i, j int) bool {
		return bytes.Compare(allData[i], allData[j]) < 0
	})

	for i := 0; i < maxSize; i++ {
		if !bytes.Equal(allData[i], []byte{byte(i)}) {
			t.Fatal("expected", []byte{byte(i)}, "got", allData[i])
		}
	}

	data, err := q.Dequeue(context.Background())
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatal("expected", ErrQueueEmpty, "got", err)
	}

	if data != nil {
		t.Fatal("expected", nil, "got", data)
	}

	q.Close()

	err = q.Enqueue(context.Background(), []byte{byte(0)})
	if !errors.Is(err, ErrQueueClosed) {
		t.Fatal("expected", ErrQueueClosed, "got", err)
	}
}

func SpecTestQueueConcurrent(t *testing.T, q Queue[[]byte]) {
	maxSize := getMaxSize(q)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	datasMutex := sync.RWMutex{}
	datas := [][]byte{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				data, err := q.Dequeue(context.Background())
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					// Commented out to avoid panic after the SpecTestQueueConcurrent has completed.
					// t.Logf("Dequeue failed: %v", err)
					continue
				}

				datasMutex.Lock()
				datas = append(datas, data.Data())
				datasMutex.Unlock()
			}
		}
	}()

	var wg sync.WaitGroup
	errsChan := make(chan error, maxSize)
	for i := 0; i < maxSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Enqueue(context.Background(), []byte{byte(i)})
			errsChan <- err
		}()
	}

	wg.Wait()
	close(errsChan)

	for err := range errsChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	timeout := time.NewTimer(1 * time.Second)
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-timeout.C:
			datasMutex.RLock()
			t.Fatalf("timeout, length=%v, maxSize=%v", len(datas), maxSize)
			datasMutex.RUnlock()
		default:
			datasMutex.Lock()
			if len(datas) >= maxSize {
				sort.Slice(datas, func(i, j int) bool {
					return bytes.Compare(datas[i], datas[j]) < 0
				})

				for i := 0; i < maxSize; i++ {
					if !bytes.Equal(datas[i], []byte{byte(i)}) {
						datasMutex.Unlock()
						t.Fatal("expected", []byte{byte(i)}, "got", datas[i])
					}
				}
				datasMutex.Unlock()
				return
			}
			datasMutex.Unlock()
		}
	}
}

func SpecTestQueueSubscribeHandleReachedMaxFailures(t *testing.T, f Factory[[]byte]) {
	maxFailures := 3
	q, err := f.GetOrCreateSafe("queue", WithMaxHandleFailures(maxFailures))
	if err != nil {
		t.Fatal(err)
	}

	var mutex sync.RWMutex
	currFailures := 0
	handleSucceeded := false
	handleSucceededCh := make(chan struct{})
	dlqPushed := make(chan struct{})

	// The handler fails for the first maxFailures+1 calls (retryCount 0 through maxFailures)
	// On the (maxFailures+1)th failure, Recover will push to DLQ since retryCount >= maxFailures
	// After DLQ redrive (retryCount reset to 0), the handler succeeds
	q.Subscribe(context.Background(), func(ctx context.Context, msg Message[[]byte]) error {
		mutex.Lock()
		currFailures++
		count := currFailures
		mutex.Unlock()

		// Fail for first maxFailures+1 calls, then succeed after DLQ redrive
		// The (maxFailures+1)th failure triggers DLQ push
		// After DLQ redrive, count will be maxFailures+2, which succeeds
		if count <= maxFailures+1 {
			return errors.New("test max failures")
		}

		mutex.Lock()
		handleSucceeded = true
		mutex.Unlock()
		select {
		case <-handleSucceededCh:
		default:
			close(handleSucceededCh)
		}

		return nil
	})

	// Wait for subscription to register
	if !waitForCallbacks(t, q, 5*time.Second) {
		t.Fatal("timeout waiting for callbacks to register")
	}

	err = q.Enqueue(context.Background(), []byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for all failures to occur (maxFailures+1 handler calls)
	// The last failure (at retryCount=maxFailures) triggers DLQ push
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mutex.RLock()
		// We need maxFailures+1 failures before DLQ push
		if currFailures >= maxFailures+1 {
			mutex.RUnlock()
			select {
			case <-dlqPushed:
			default:
				close(dlqPushed)
			}
			break
		}
		mutex.RUnlock()
		time.Sleep(10 * time.Millisecond)
	}

	mutex.RLock()
	if currFailures < maxFailures+1 {
		mutex.RUnlock()
		t.Fatalf("expected at least %d failures, got %d", maxFailures+1, currFailures)
	}
	mutex.RUnlock()

	// Give some time for the message to be pushed to DLQ after the last failure
	// TODO: Make this event-driven when DLQ supports notifications
	time.Sleep(100 * time.Millisecond)

	if q.IsDLQSupported() {
		dlq, err := q.DLQ()
		if err != nil {
			t.Fatal(err)
		}

		msg, err := dlq.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(msg.Data(), []byte("data")) {
			t.Fatal("expected", []byte("data"), "got", msg.Data())
		}

		// Push back
		err = dlq.Enqueue(context.Background(), msg.Data())
		if err != nil {
			t.Fatal(err)
		}

		// Then redrive
		err = dlq.Redrive(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}

		// Redrive again when dlq is empty
		err = dlq.Redrive(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for redriven message to be handled successfully
		// Poll handleSucceeded with timeout
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			mutex.RLock()
			if handleSucceeded {
				mutex.RUnlock()
				break
			}
			mutex.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}

	mutex.RLock()
	assert.Equal(t, true, handleSucceeded)
	mutex.RUnlock()
}

func SpecTestQueueSubscribe(t *testing.T, f Factory[[]byte]) {
	type dataAndErr struct {
		Data string
		Err  string
	}

	testcases := []struct {
		data []dataAndErr
	}{
		{
			data: []dataAndErr{
				{"data1", ""},
			},
		},
		{
			data: []dataAndErr{
				{"data1", ""},
				{"data2", ""},
				{"data3", ""},
				{"data4", ""},
				{"data5", ""},
			},
		},
		{
			data: []dataAndErr{
				{"data1", ""},
				{"data2", "err1"},
				{"data3", ""},
				{"data4", ""},
				{"data5", "err2"},
			},
		},
		{
			data: []dataAndErr{
				{"data1", ""},
				{"data2", "panic1"},
				{"data3", ""},
				{"data4", "err1"},
				{"data5", "panic2"},
			},
		},
	}

	for i, tt := range testcases {
		t.Run(fmt.Sprintf("#case%d", i), func(t *testing.T) {
			var mutex sync.RWMutex
			allData := make(map[string]int)

			var mutex2 sync.RWMutex
			allData2 := make(map[string]int)

			isSubscribeErrReturned := make(map[string]bool)

			// Channel to signal when all messages are successfully processed in Handler1
			allProcessed := make(chan struct{})
			expectedCount := len(tt.data)

			q, err := f.GetOrCreate(fmt.Sprintf("test-%d", i))
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()

			// Handler1
			q.Subscribe(context.Background(), func(ctx context.Context, msg Message[[]byte]) error {
				b := msg.Data()

				t.Logf("Subscribe in test: %v", msg)
				var data dataAndErr
				err := json.Unmarshal(b, &data)
				if err != nil {
					t.Fatal(err)
				}

				mutex.Lock()
				defer mutex.Unlock()
				if data.Err != "" && !isSubscribeErrReturned[data.Err] {
					isSubscribeErrReturned[data.Err] = true
					if strings.Contains(data.Err, "panic") {
						panic(data.Err)
					}

					return errors.New(data.Err)
				}

				t.Logf("Store data: %v", data.Data)
				allData[data.Data] += 1

				// Check if all messages have been processed
				if len(allData) == expectedCount {
					select {
					case <-allProcessed:
						// Already closed
					default:
						close(allProcessed)
					}
				}
				return nil
			})

			// Handler2
			q.Subscribe(context.Background(), func(ctx context.Context, msg Message[[]byte]) error {
				b := msg.Data()

				t.Logf("Subscribe 2 in test: %v", msg)
				var data dataAndErr
				err := json.Unmarshal(b, &data)
				if err != nil {
					t.Fatal(err)
				}

				mutex2.Lock()
				defer mutex2.Unlock()

				allData2[data.Data] += 1

				return nil
			})

			// Wait for callbacks to register
			if !waitForCallbacks(t, q, 5*time.Second) {
				t.Fatal("timeout waiting for callbacks to register")
			}

			for _, d := range tt.data {
				data, err := json.Marshal(&d)
				if err != nil {
					t.Fatal(err)
				}

				err = q.Enqueue(context.Background(), data)
				if err != nil {
					t.Fatalf("failed to enqueue: %v", err)
				}
			}

			// Wait for all messages to be processed with timeout
			select {
			case <-allProcessed:
				// Give Handler2 a moment to complete (since it processes in parallel)
				// TODO: Replace with proper event-driven wait for Handler2
				time.Sleep(50 * time.Millisecond)
			case <-time.After(10 * time.Second):
				mutex.RLock()
				t.Fatalf("timeout waiting for messages to be processed, got %d/%d", len(allData), expectedCount)
				mutex.RUnlock()
			}

			for _, d := range tt.data {
				mutex.RLock()

				if allData[d.Data] < 1 {
					mutex.RUnlock()
					t.Fatalf("expected %v to be included, allData=%v", d.Data, allData)
				}
				if allData[d.Data] > 1 {
					mutex.RUnlock()
					t.Fatalf("expected %v to be included once, count=%v", d.Data, allData[d.Data])
				}
				mutex.RUnlock()

				mutex2.RLock()

				if d.Err != "" {
					assert.Equal(t, 2, allData2[d.Data])
				} else {
					assert.Equal(t, 1, allData2[d.Data])
				}
				mutex2.RUnlock()
			}
		})
	}
}

func SpecTestQueueSubscribeWithConsumerCount(t *testing.T, f Factory[[]byte]) {
	// Set up queue with 3 consumers

	q, err := f.GetOrCreate("subscribe-with-consumer-count-test", func(c *Config) {
		c.ConsumerCount = 3
	})

	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// Test with multiple consumers using ConsumerCount option
	numMessages := 100
	receivedMessages := make(map[string]bool)
	var mutex sync.Mutex
	allReceived := make(chan struct{})

	// Subscribe and process messages
	q.Subscribe(context.Background(), func(ctx context.Context, msg Message[[]byte]) error {
		mutex.Lock()
		receivedMessages[string(msg.Data())] = true
		if len(receivedMessages) == numMessages {
			select {
			case <-allReceived:
			default:
				close(allReceived)
			}
		}
		mutex.Unlock()
		return nil
	})

	// Wait for callbacks to register
	if !waitForCallbacks(t, q, 5*time.Second) {
		t.Fatal("timeout waiting for callbacks to register")
	}

	// Enqueue test messages
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf("msg-%d", i)
		err := q.Enqueue(context.Background(), []byte(msg))
		if err != nil {
			t.Fatalf("failed to enqueue message: %v", err)
		}
	}

	// Wait for all messages to be received with timeout
	select {
	case <-allReceived:
	case <-time.After(10 * time.Second):
		mutex.Lock()
		t.Fatalf("timeout waiting for messages, got %d/%d", len(receivedMessages), numMessages)
		mutex.Unlock()
	}

	// Verify all messages were received
	mutex.Lock()
	receivedCount := len(receivedMessages)
	mutex.Unlock()

	if receivedCount != numMessages {
		t.Errorf("expected %d messages, got %d", numMessages, receivedCount)
	}

	// Verify each message was received exactly once
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf("msg-%d", i)
		mutex.Lock()
		if !receivedMessages[msg] {
			t.Errorf("message not received: %s", msg)
		}
		mutex.Unlock()
	}
}

func SpecTestQueueTimeout(t *testing.T, f Factory[[]byte]) {
	q, err := f.GetOrCreate("timeout-test")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Dequeue from empty queue should fail with timeout
	_, err = q.BDequeue(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded error, got: %v", err)
	}
}

func SpecTestQueueStressTest(t *testing.T, f Factory[[]byte]) {
	q, err := f.GetOrCreate("stress-test")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// Configure stress test parameters
	numProducers := 10
	numConsumers := 5
	messagesPerProducer := 100
	totalMessages := numProducers * messagesPerProducer

	// Track results
	var receivedCount int32
	receivedMessages := make(map[string]bool)
	var mutex sync.Mutex

	// Start consumers
	var wg sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				msg, err := q.BDequeue(ctx)
				cancel()

				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						// Check if all messages have been processed
						if int(atomic.LoadInt32(&receivedCount)) >= totalMessages {
							return
						}
						continue
					}
					t.Errorf("consumer %d dequeue error: %v", consumerID, err)
					return
				}

				mutex.Lock()
				receivedMessages[string(msg.Data())] = true
				mutex.Unlock()
				atomic.AddInt32(&receivedCount, 1)
			}
		}(i)
	}

	// Start producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				data := []byte(fmt.Sprintf("producer-%d-msg-%d", producerID, j))
				err := q.Enqueue(context.Background(), data)
				if err != nil {
					t.Errorf("producer %d enqueue error: %v", producerID, err)
					return
				}
			}
		}(i)
	}

	// Wait for all producers and consumers to complete
	wg.Wait()

	// Verify results
	if int(receivedCount) != totalMessages {
		t.Errorf("expected %d messages, got %d", totalMessages, receivedCount)
	}

	// Verify all messages were processed correctly
	mutex.Lock()
	defer mutex.Unlock()
	for i := 0; i < numProducers; i++ {
		for j := 0; j < messagesPerProducer; j++ {
			expected := fmt.Sprintf("producer-%d-msg-%d", i, j)
			if !receivedMessages[expected] {
				t.Errorf("missing message: %s", expected)
			}
		}
	}
}

func SpecTestQueueErrorHandling(t *testing.T, f Factory[[]byte]) {
	q, err := f.GetOrCreate("error-test")
	if err != nil {
		t.Fatal(err)
	}

	// Test nil data
	err = q.Enqueue(context.Background(), nil)
	assert.Equal(t, nil, err)

	// Test operations after queue closure
	q.Close()
	err = q.Enqueue(context.Background(), []byte("test"))
	if !errors.Is(err, ErrQueueClosed) {
		t.Errorf("expected ErrQueueClosed, got: %v", err)
	}
}

func SpecTestQueueBlockingOperations(t *testing.T, f Factory[[]byte]) {
	q, err := f.GetOrCreate("blocking-test")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// Test blocking dequeue on empty queue
	dequeueCompleted := make(chan struct{})
	go func() {
		msg, err := q.BDequeue(context.Background())
		if err != nil {
			t.Errorf("blocking dequeue error: %v", err)
			return
		}
		if !bytes.Equal(msg.Data(), []byte("test-data")) {
			t.Errorf("expected 'test-data', got %s", string(msg.Data()))
		}
		dequeueCompleted <- struct{}{}
	}()

	// Wait a bit to ensure dequeue is blocked
	time.Sleep(100 * time.Millisecond)

	// Now enqueue data, should unblock the dequeue
	err = q.Enqueue(context.Background(), []byte("test-data"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for dequeue to complete
	select {
	case <-dequeueCompleted:
		// Expected behavior
	case <-time.After(1 * time.Second):
		t.Error("blocking dequeue did not complete after data was available")
	}

	// Test blocking enqueue on full queue
	maxSize := q.MaxSize()
	if maxSize != UnlimitedSize {
		// Fill the queue
		for i := 0; i < maxSize; i++ {
			err := q.Enqueue(context.Background(), []byte{byte(i)})
			if err != nil {
				t.Fatal(err)
			}
		}

		enqueueCompleted := make(chan struct{})
		go func() {
			err := q.BEnqueue(context.Background(), []byte("blocked-data"))
			if err != nil {
				t.Errorf("blocking enqueue error: %v", err)
				return
			}
			enqueueCompleted <- struct{}{}
		}()

		// Wait a bit to ensure enqueue is blocked
		time.Sleep(100 * time.Millisecond)

		// Dequeue one item to make space
		_, err = q.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Wait for enqueue to complete
		select {
		case <-enqueueCompleted:
			// Expected behavior
		case <-time.After(1 * time.Second):
			t.Error("blocking enqueue did not complete after space was available")
		}
	}
}

func SpecTestQueueBlockingWithContext(t *testing.T, f Factory[[]byte]) {
	q, err := f.GetOrCreate("blocking-context-test")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// Test blocking dequeue with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	dequeueErr := make(chan error)
	go func() {
		_, err := q.BDequeue(ctx)
		dequeueErr <- err
	}()

	// Wait a bit then cancel context
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Check if dequeue was cancelled
	select {
	case err := <-dequeueErr:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("blocking dequeue was not cancelled by context")
	}

	// Test blocking enqueue with timeout context
	if q.MaxSize() != UnlimitedSize {
		// Fill the queue
		for i := 0; i < q.MaxSize(); i++ {
			err := q.Enqueue(context.Background(), []byte{byte(i)})
			if err != nil {
				t.Fatal(err)
			}
		}

		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		enqueueErr := make(chan error)
		go func() {
			err := q.BEnqueue(ctx, []byte("test"))
			enqueueErr <- err
		}()

		// Check if enqueue times out
		select {
		case err := <-enqueueErr:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded error, got: %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Error("blocking enqueue did not timeout as expected")
		}
	}
}

func SpecTestQueueBlockingMultipleConsumers(t *testing.T, f Factory[[]byte]) {
	q, err := f.GetOrCreate("blocking-multi-consumer-test")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	numConsumers := 3
	numMessages := 5
	receivedMessages := make(chan []byte, numConsumers*numMessages)

	// Start multiple consumers
	var wg sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				msg, err := q.BDequeue(context.Background())
				if err != nil {
					t.Errorf("consumer %d dequeue error: %v", consumerID, err)
					return
				}
				receivedMessages <- msg.Data()
			}
		}(i)
	}

	// Wait a bit to ensure consumers are waiting
	time.Sleep(100 * time.Millisecond)

	// Send messages
	totalMessages := numConsumers * numMessages
	for i := 0; i < totalMessages; i++ {
		err := q.BEnqueue(context.Background(), []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for all consumers to finish
	wg.Wait()
	close(receivedMessages)

	// Verify all messages were received
	received := make(map[string]bool)
	for msg := range receivedMessages {
		received[string(msg)] = true
	}

	if len(received) != totalMessages {
		t.Errorf("expected %d unique messages, got %d", totalMessages, len(received))
	}

	for i := 0; i < totalMessages; i++ {
		expected := fmt.Sprintf("msg-%d", i)
		if !received[expected] {
			t.Errorf("missing message: %s", expected)
		}
	}
}

func getMaxSize(q Queue[[]byte]) int {
	maxSize := q.MaxSize()
	if maxSize == UnlimitedSize {
		maxSize = 10
	}

	return maxSize
}
