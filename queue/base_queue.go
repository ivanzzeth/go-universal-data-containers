package queue

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/ivanzzeth/go-universal-data-containers/metrics"
)

var (
	_ Queue[any] = &BaseQueue[any]{}
)

type BaseQueue[T any] struct {
	locker locker.SyncLocker
	name   string

	defaultMsg Message[T]
	config     *Config

	// callbacks stored using atomic.Value for lock-free read access
	// The stored value is []Handler[T]
	callbacksAtomic atomic.Value

	// callbacksMu protects write operations to callbacksAtomic
	callbacksMu sync.Mutex

	msgBuffer   chan struct{}
	exitChannel chan struct{}

	dlq DLQ[T]

	// Graceful shutdown support
	closing    atomic.Bool
	inflightWg sync.WaitGroup
}

func NewBaseQueue[T any](name string, defaultMsg Message[T], options ...Option) (*BaseQueue[T], error) {
	ops := DefaultOptions
	for _, op := range options {
		op(&ops)
	}

	locker, err := ops.LockerGenerator.CreateSyncLocker(fmt.Sprintf("queue-locker-%v", name))
	if err != nil {
		return nil, err
	}
	q := &BaseQueue[T]{
		locker:      locker,
		name:        name,
		defaultMsg:  defaultMsg,
		config:      &ops,
		msgBuffer:   make(chan struct{}, ops.ConsumerCount),
		exitChannel: make(chan struct{}),
	}

	return q, nil
}

func (q *BaseQueue[T]) Close() {
	select {
	case <-q.exitChannel:
		// Already closed
		return
	default:
		close(q.exitChannel)
	}
}

// GracefulClose performs a graceful shutdown:
// 1. Marks as closing to prevent new enqueues
// 2. Waits for all in-flight messages to be processed
// 3. Closes the exit channel
func (q *BaseQueue[T]) GracefulClose() {
	q.closing.Store(true)
	q.inflightWg.Wait()
	q.Close()
}

// IsClosing returns true if the queue is in the process of closing
func (q *BaseQueue[T]) IsClosing() bool {
	return q.closing.Load()
}

// AddInflight increments the in-flight counter
func (q *BaseQueue[T]) AddInflight() {
	q.inflightWg.Add(1)
}

// DoneInflight decrements the in-flight counter
func (q *BaseQueue[T]) DoneInflight() {
	q.inflightWg.Done()
}

func (q *BaseQueue[T]) IsClosed() bool {
	select {
	case <-q.exitChannel:
		return true
	default:
		return false
	}
}

func (q *BaseQueue[T]) ExitChannel() <-chan struct{} {
	return q.exitChannel
}

func (q *BaseQueue[T]) Kind() Kind {
	return KindFIFO
}

func (q *BaseQueue[T]) Name() string {
	return q.name
}

func (q *BaseQueue[T]) GetConfig() *Config {
	return q.config
}

func (q *BaseQueue[T]) GetLocker() locker.SyncLocker {
	return q.locker
}

func (q *BaseQueue[T]) MaxSize() int {
	return q.config.MaxSize
}

func (q *BaseQueue[T]) MaxHandleFailures() int {
	return q.config.MaxHandleFailures
}

func (q *BaseQueue[T]) Enqueue(ctx context.Context, data T) error {
	return common.ErrNotImplemented
}

func (q *BaseQueue[T]) BEnqueue(ctx context.Context, data T) error {
	return common.ErrNotImplemented
}

func (q *BaseQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	return nil, common.ErrNotImplemented
}

func (q *BaseQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	return nil, common.ErrNotImplemented
}

// Subscribe adds a callback handler. Thread-safe using atomic operations.
func (q *BaseQueue[T]) Subscribe(ctx context.Context, cb Handler[T]) {
	q.callbacksMu.Lock()
	defer q.callbacksMu.Unlock()

	callbacks := q.GetCallbacks()
	newCallbacks := make([]Handler[T], len(callbacks)+1)
	copy(newCallbacks, callbacks)
	newCallbacks[len(callbacks)] = cb
	q.callbacksAtomic.Store(newCallbacks)
}

// GetCallbacks returns the current callbacks slice. Lock-free read.
func (q *BaseQueue[T]) GetCallbacks() []Handler[T] {
	v := q.callbacksAtomic.Load()
	if v == nil {
		return nil
	}
	return v.([]Handler[T])
}

// HasCallbacks returns true if there are registered callbacks. Lock-free.
func (q *BaseQueue[T]) HasCallbacks() bool {
	return len(q.GetCallbacks()) > 0
}

func (q *BaseQueue[T]) TriggerCallbacks(ctx context.Context, msg Message[T]) {
	q.msgBuffer <- struct{}{}

	go func() {
		defer func() { <-q.msgBuffer }()

		callbacks := q.GetCallbacks()

		if !q.config.CallbackParallelExecution {
			// Sequential mode (default)
			for _, cb := range callbacks {
				callbackCtx := ctx
				if q.config.CallbackTimeout > 0 {
					var cancel context.CancelFunc
					callbackCtx, cancel = context.WithTimeout(ctx, q.config.CallbackTimeout)
					defer cancel()
				}
				_ = cb(callbackCtx, msg) // Error isolation: one callback failure does not affect other callbacks
			}
			return
		}

		// Parallel mode
		var wg sync.WaitGroup
		errors := make(chan error, len(callbacks))

		for _, cb := range callbacks {
			wg.Add(1)
			go func(callback Handler[T]) {
				defer wg.Done()

				callbackCtx := ctx
				if q.config.CallbackTimeout > 0 {
					var cancel context.CancelFunc
					callbackCtx, cancel = context.WithTimeout(ctx, q.config.CallbackTimeout)
					defer cancel()
				}

				done := make(chan error, 1)
				go func() {
					done <- callback(callbackCtx, msg)
				}()

				select {
				case err := <-done:
					if err != nil {
						errors <- err
					}
				case <-callbackCtx.Done():
					errors <- fmt.Errorf("callback timeout after %v", q.config.CallbackTimeout)
				}
			}(cb)
		}

		wg.Wait()
		close(errors)

		// Collect all errors (can be used for logging, but does not affect message processing status)
		// Error isolation: one callback failure does not affect other callbacks
		for err := range errors {
			_ = err // Error collected, can be used for subsequent logging
		}
	}()
}

func (q *BaseQueue[T]) ValidateQueueClosed() error {
	select {
	case <-q.exitChannel:
		return ErrQueueClosed
	default:
	}

	return nil
}

func (q *BaseQueue[T]) GetQueueKey() string {
	return fmt.Sprintf("%v::%v", Namespace, q.name)
}

func (q *BaseQueue[T]) GetRetryQueueName() string {
	return fmt.Sprintf("%v::retry", q.name)
}

func (q *BaseQueue[T]) GetRetryQueueKey() string {
	return fmt.Sprintf("%v::%v", Namespace, q.GetRetryQueueName())
}

func (q *BaseQueue[T]) GetDeadletterQueueName() string {
	return fmt.Sprintf("%v::DLQ", q.name)
}

func (q *BaseQueue[T]) GetDeadletterQueueKey() string {
	return fmt.Sprintf("%v::%v", Namespace, q.GetDeadletterQueueName())
}

func (q *BaseQueue[T]) SetDLQ(dlq DLQ[T]) {
	q.dlq = dlq
}

func (q *BaseQueue[T]) DLQ() (DLQ[T], error) {
	if q.dlq == nil {
		return nil, common.ErrNotImplemented
	}

	return q.dlq, nil
}

// ShouldSendToDLQ checks if the message should be sent to DLQ based on retry count.
// If yes, it sends the message to DLQ and records the metric.
// Returns true if the message was sent to DLQ, false otherwise.
func (q *BaseQueue[T]) ShouldSendToDLQ(ctx context.Context, msg Message[T]) (bool, error) {
	if msg.RetryCount() >= q.config.MaxHandleFailures {
		err := q.dlq.Enqueue(ctx, msg.Data())
		if err != nil {
			return false, err
		}

		// Increment DLQ messages counter
		metrics.MetricQueueDLQMessagesTotal.WithLabelValues(q.name).Inc()

		return true, nil
	}
	return false, nil
}

func (q *BaseQueue[T]) Pack(data T) ([]byte, error) {
	msg, err := q.NewMessage(data)
	if err != nil {
		return nil, err
	}

	packedData, err := msg.Pack()
	if err != nil {
		return nil, err
	}

	// Make sure the packed data is the same as the original data
	msgToTest, err := q.NewMessage(data)
	if err != nil {
		return nil, err
	}

	err = msgToTest.Unpack(packedData)
	if err != nil {
		return nil, err
	}

	return packedData, nil
}

func (q *BaseQueue[T]) Unpack(data []byte) (Message[T], error) {
	msg, err := q.NewMessage(reflect.New(reflect.TypeOf(q.defaultMsg.Data())).Elem().Interface().(T))
	if err != nil {
		return nil, err
	}

	err = msg.Unpack(data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// UnpackMessage unpacks data and resets retry count if needed.
// This eliminates the duplicate unpackMessage logic from MemoryQueue and RedisQueue.
func (q *BaseQueue[T]) UnpackMessage(packedData []byte) (Message[T], error) {
	msg, err := q.Unpack(packedData)
	if err != nil {
		return nil, err
	}

	if msg.RetryCount() > q.config.MaxHandleFailures {
		msg.RefreshRetryCount()
	}

	return msg, nil
}

func (q *BaseQueue[T]) NewMessage(data T) (Message[T], error) {
	if q.defaultMsg == nil {
		panic("message type is not set")
	}

	msg := reflect.New(reflect.TypeOf(q.defaultMsg).Elem()).Interface().(Message[T])

	err := msg.SetData(data)
	if err != nil {
		return nil, err
	}

	id, err := q.config.MessageIDGenerator()
	if err != nil {
		return nil, err
	}

	err = msg.SetID(id)
	if err != nil {
		return nil, err
	}

	err = msg.SetMetadata(map[string]interface{}{
		"retry_count": 0,
		"created_at":  time.Now(),
		"updated_at":  time.Now(),
	})

	if err != nil {
		return nil, err
	}

	return msg, nil
}
