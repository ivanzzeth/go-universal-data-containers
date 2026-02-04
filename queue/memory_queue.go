package queue

import (
	"context"
	"sync"
	"time"
)

var (
	_ Factory[any]            = &MemoryFactory[any]{}
	_ Queue[any]              = &MemoryQueue[any]{}
	_ RecoverableQueue[any]   = &MemoryQueue[any]{}
	_ Purgeable               = &MemoryQueue[any]{}
	_ DLQer[any]              = &MemoryQueue[any]{}
	_ StatsProvider           = &MemoryQueue[any]{}
	_ RetryQueueEnqueuer[any] = &MemoryQueue[any]{}
)

type MemoryFactory[T any] struct {
	m          sync.Mutex
	defaultMsg Message[T]
	table      map[string]SafeQueue[T]
}

func NewMemoryFactory[T any](defaultMsg Message[T]) *MemoryFactory[T] {
	return &MemoryFactory[T]{
		defaultMsg: defaultMsg,
		table:      make(map[string]SafeQueue[T]),
	}
}

func (f *MemoryFactory[T]) GetOrCreate(name string, options ...Option) (Queue[T], error) {
	var queue Queue[T]
	safeQueue, err := f.GetOrCreateSafe(name, options...)
	if err != nil {
		return nil, err
	}

	queue = safeQueue
	return queue, nil
}

func (f *MemoryFactory[T]) GetOrCreateSafe(name string, options ...Option) (SafeQueue[T], error) {
	f.m.Lock()
	defer f.m.Unlock()
	if _, ok := f.table[name]; !ok {
		mq, err := NewMemoryQueue[T](name, f.defaultMsg, options...)
		if err != nil {
			return nil, err
		}

		q, err := NewSimpleQueue(mq)
		if err != nil {
			return nil, err
		}
		f.table[name] = q
	}

	return f.table[name], nil
}

type MemoryQueue[T any] struct {
	*BaseQueue[T]

	// Main data channel - FIFO queue (lock-free)
	dataChan chan []byte

	// Retry queue channel - for Recover, processed with higher priority
	// Ensures FIFO semantics: retry messages are processed before new messages
	retryChan chan []byte
}

func NewMemoryQueue[T any](name string, defaultMsg Message[T], options ...Option) (*MemoryQueue[T], error) {
	baseQueue, err := NewBaseQueue(name, defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	// Calculate channel capacity
	capacity := baseQueue.config.MaxSize
	if capacity == UnlimitedSize {
		capacity = baseQueue.config.UnlimitedCapacity
	}

	retryCapacity := baseQueue.config.RetryQueueCapacity
	if retryCapacity <= 0 {
		retryCapacity = DefaultRetryQueueCapacity
	}

	q := &MemoryQueue[T]{
		BaseQueue: baseQueue,
		dataChan:  make(chan []byte, capacity),
		retryChan: make(chan []byte, retryCapacity),
	}

	// Create DLQ
	dlqBaseQueue, err := NewBaseQueue(q.GetDeadletterQueueName(), defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	dlqCapacity := dlqBaseQueue.config.MaxSize
	if dlqCapacity == UnlimitedSize {
		dlqCapacity = dlqBaseQueue.config.UnlimitedCapacity
	}

	dlq := &MemoryQueue[T]{
		BaseQueue: dlqBaseQueue,
		dataChan:  make(chan []byte, dlqCapacity),
		retryChan: make(chan []byte, retryCapacity),
	}

	DLQ, err := newBaseDLQ(q, dlq)
	if err != nil {
		return nil, err
	}

	q.SetDLQ(DLQ)

	go q.run()
	go dlq.run()

	return q, nil
}

func (q *MemoryQueue[T]) Close() {
	q.BaseQueue.GracefulClose()
}

func (q *MemoryQueue[T]) Kind() Kind {
	return KindFIFO
}

func (q *MemoryQueue[T]) Name() string {
	return q.name
}

func (q *MemoryQueue[T]) Enqueue(ctx context.Context, data T) error {
	if q.IsClosing() {
		return ErrQueueClosed
	}

	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	// Non-blocking send to check if queue is full
	select {
	case q.dataChan <- packedData:
		return nil
	default:
		return ErrQueueFull
	}
}

func (q *MemoryQueue[T]) BEnqueue(ctx context.Context, data T) error {
	if q.IsClosing() {
		return ErrQueueClosed
	}

	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	// Blocking send with context support
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.ExitChannel():
		return ErrQueueClosed
	case q.dataChan <- packedData:
		return nil
	}
}

func (q *MemoryQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	// Priority: retry queue first, then main queue
	// Non-blocking receive
	select {
	case packedData := <-q.retryChan:
		return q.UnpackMessage(packedData)
	default:
	}

	select {
	case packedData := <-q.dataChan:
		return q.UnpackMessage(packedData)
	default:
		return nil, ErrQueueEmpty
	}
}

func (q *MemoryQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	// Blocking receive with priority for retry queue
	for {
		// First, try non-blocking read from retry queue
		select {
		case packedData := <-q.retryChan:
			return q.UnpackMessage(packedData)
		default:
		}

		// Then block on both channels with context support
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.ExitChannel():
			return nil, ErrQueueClosed
		case packedData := <-q.retryChan:
			return q.UnpackMessage(packedData)
		case packedData := <-q.dataChan:
			return q.UnpackMessage(packedData)
		}
	}
}


func (q *MemoryQueue[T]) run() {
	ctx := context.Background()

	for {
		// Check if queue is closed or closing.
		// IMPORTANT: Must check IsClosing() to prevent WaitGroup race condition.
		// When GracefulClose() is called, it sets closing=true then calls Wait().
		// If we don't check IsClosing() here, we might call AddInflight() while
		// Wait() is already running, causing "WaitGroup is reused before previous
		// Wait has returned" panic.
		select {
		case <-q.ExitChannel():
			return
		default:
			if q.IsClosing() {
				return
			}
		}

		// Wait for callbacks to be registered
		// TODO: Optimize to use event-driven notification instead of time.Sleep
		if !q.HasCallbacks() {
			// Brief sleep to avoid busy-waiting when no callbacks
			select {
			case <-q.ExitChannel():
				return
			case <-time.After(DefaultCallbackWaitInterval):
				continue
			}
		}

		// Priority: retry queue first, then main queue
		// Use select with priority pattern
		var packedData []byte
		var ok bool

		// First, try non-blocking read from retry queue
		select {
		case packedData, ok = <-q.retryChan:
			if !ok {
				return
			}
		default:
			// Retry queue empty, block on both channels
			select {
			case <-q.ExitChannel():
				return
			case packedData, ok = <-q.retryChan:
				if !ok {
					return
				}
			case packedData, ok = <-q.dataChan:
				if !ok {
					return
				}
			}
		}

		msg, err := q.UnpackMessage(packedData)
		if err != nil {
			continue
		}

		// AddInflight checks IsClosing() atomically to prevent race condition
		// with GracefulClose().Wait(). If it returns false, the queue is closing.
		if !q.AddInflight() {
			return
		}
		q.TriggerCallbacks(ctx, msg)
		q.DoneInflight()
	}
}

func (q *MemoryQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	sentToDLQ, err := q.ShouldSendToDLQ(ctx, msg)
	if err != nil {
		return err
	}
	if sentToDLQ {
		return nil
	}

	msg.AddRetryCount()
	msg.RefreshUpdatedAt()

	packedData, err := msg.Pack()
	if err != nil {
		return err
	}

	// Send to retry queue (higher priority)
	// Non-blocking first, then blocking if retry queue is full
	select {
	case q.retryChan <- packedData:
		return nil
	default:
		// Retry queue full, block with context
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.ExitChannel():
			return ErrQueueClosed
		case q.retryChan <- packedData:
			return nil
		}
	}
}

func (q *MemoryQueue[T]) Purge(ctx context.Context) error {
	// Drain both channels
	for {
		select {
		case <-q.retryChan:
		case <-q.dataChan:
		default:
			return nil
		}
	}
}

// EnqueueToRetryQueue adds a message directly to the retry queue.
// Used by DLQ.Redrive to ensure recovered messages are processed with priority.
func (q *MemoryQueue[T]) EnqueueToRetryQueue(ctx context.Context, data T) error {
	if q.IsClosing() {
		return ErrQueueClosed
	}

	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.ExitChannel():
		return ErrQueueClosed
	case q.retryChan <- packedData:
		return nil
	}
}

// Stats returns the current queue statistics.
// Implements the StatsProvider interface.
func (q *MemoryQueue[T]) Stats() QueueStats {
	mainDepth := int64(len(q.dataChan))
	retryDepth := int64(len(q.retryChan))

	// Calculate capacity
	capacity := q.config.MaxSize
	if capacity == UnlimitedSize {
		capacity = q.config.UnlimitedCapacity
	}

	retryCapacity := q.config.RetryQueueCapacity
	if retryCapacity <= 0 {
		retryCapacity = DefaultRetryQueueCapacity
	}

	return QueueStats{
		Depth:         mainDepth,
		RetryDepth:    retryDepth,
		ConsumerCount: len(q.GetCallbacks()),
		Capacity:      capacity,
		RetryCapacity: retryCapacity,
	}
}
