package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	_ Factory[any]          = &MemoryFactory[any]{}
	_ Queue[any]            = &MemoryQueue[any]{}
	_ RecoverableQueue[any] = &MemoryQueue[any]{}
	_ Purgeable             = &MemoryQueue[any]{}
	_ DLQer[any]            = &MemoryQueue[any]{}
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
	queue [][]byte
}

func NewMemoryQueue[T any](name string, defaultMsg Message[T], options ...Option) (*MemoryQueue[T], error) {
	baseQueue, err := NewBaseQueue(name, defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	q := &MemoryQueue[T]{
		BaseQueue: baseQueue,
	}

	dlqBaseQueue, err := NewBaseQueue(q.GetDeadletterQueueName(), defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	dlq := &MemoryQueue[T]{
		BaseQueue: dlqBaseQueue,
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
	close(q.exitChannel)
}

func (q *MemoryQueue[T]) Kind() Kind {
	return KindFIFO
}

func (q *MemoryQueue[T]) Name() string {
	return q.name
}

func (q *MemoryQueue[T]) Enqueue(ctx context.Context, data T) error {
	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	err = q.GetLocker().Lock(ctx)
	if err != nil {
		return err
	}

	defer q.GetLocker().Unlock(ctx)

	if q.MaxSize() > 0 && len(q.queue) >= q.MaxSize() {
		return ErrQueueFull
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	q.queue = append(q.queue, packedData)
	return nil
}

func (q *MemoryQueue[T]) BEnqueue(ctx context.Context, data T) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := q.Enqueue(ctx, data)
			if err != nil {
				if errors.Is(err, ErrQueueFull) {
					time.Sleep(q.config.PollInterval)
					continue
				}

				return err
			}

			return nil
		}
	}
}

func (q *MemoryQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	err := q.GetLocker().Lock(ctx)
	if err != nil {
		return nil, err
	}

	defer q.GetLocker().Unlock(ctx)

	if len(q.queue) > 0 {
		packedData := q.queue[0]
		q.queue = q.queue[1:]

		msg, err := q.Unpack(packedData)
		if err != nil {
			return nil, err
		}

		if msg.RetryCount() > q.config.MaxHandleFailures {
			msg.RefreshRetryCount()
		}

		return msg, nil
	}

	return nil, ErrQueueEmpty
}

func (q *MemoryQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			msg, err := q.Dequeue(ctx)
			if err != nil {
				if errors.Is(err, ErrQueueEmpty) {
					time.Sleep(q.config.PollInterval)
					continue
				}

				return nil, err
			}

			return msg, nil
		}
	}
}

func (q *MemoryQueue[T]) run() {
Loop:
	for {
		select {
		case <-q.exitChannel:
			break Loop
		default:
			q.locker.Lock(context.Background())
			if q.callbacks == nil {
				q.locker.Unlock(context.Background())
				time.Sleep(100 * time.Millisecond)
				continue
			}
			q.locker.Unlock(context.Background())

			msg, err := q.Dequeue(context.TODO())
			if err != nil {
				time.Sleep(q.config.PollInterval)
				continue Loop
			}

			q.TriggerCallbacks(msg)

			time.Sleep(q.config.PollInterval)
		}
	}
}

func (q *MemoryQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	if msg.RetryCount() >= q.config.MaxHandleFailures {
		err := q.dlq.Enqueue(ctx, msg.Data())
		if err != nil {
			return err
		}

		return nil
	}

	msg.AddRetryCount()
	msg.RefreshUpdatedAt()

	// fmt.Printf("Recover data: MaxHandleFailures: %v,%+v\n", q.config.MaxHandleFailures, msg)

	packedData, err := msg.Pack()
	if err != nil {
		return err
	}

	err = q.GetLocker().Lock(ctx)
	if err != nil {
		return err
	}

	defer q.GetLocker().Unlock(ctx)

	q.queue = append([][]byte{packedData}, q.queue...)
	return nil
}

func (q *MemoryQueue[T]) Purge(ctx context.Context) error {
	err := q.GetLocker().Lock(ctx)
	if err != nil {
		return err
	}

	defer q.GetLocker().Unlock(ctx)
	q.queue = nil
	return nil
}
