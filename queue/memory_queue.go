package queue

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

var (
	_ Factory          = &MemoryFactory{}
	_ Queue            = &MemoryQueue{}
	_ RecoverableQueue = &MemoryQueue{}
	_ Purgeable        = &MemoryQueue{}
)

type MemoryFactory struct {
	m     sync.Mutex
	table map[string]SafeQueue
}

func NewMemoryFactory() *MemoryFactory {
	return &MemoryFactory{
		table: make(map[string]SafeQueue),
	}
}

func (f *MemoryFactory) GetOrCreate(name string, options ...Option) (Queue, error) {
	var queue Queue
	safeQueue, err := f.GetOrCreateSafe(name, options...)
	if err != nil {
		return nil, err
	}

	queue = safeQueue
	return queue, nil
}

func (f *MemoryFactory) GetOrCreateSafe(name string, options ...Option) (SafeQueue, error) {
	ops := DefaultOptions
	for _, op := range options {
		op(&ops)
	}

	f.m.Lock()
	defer f.m.Unlock()
	if _, ok := f.table[name]; !ok {
		mq, err := NewMemoryQueue(name, &ops)
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

type MemoryQueue struct {
	*BaseQueue
	queue     [][]byte
	callbacks []Handler
}

func NewMemoryQueue(name string, options *Config) (*MemoryQueue, error) {
	baseQueue, err := NewBaseQueue(name, options)
	if err != nil {
		return nil, err
	}

	q := &MemoryQueue{
		BaseQueue: baseQueue,
	}

	go q.run()

	return q, nil
}

func (q *MemoryQueue) Close() {
	close(q.exitChannel)
}

func (q *MemoryQueue) Kind() Kind {
	return KindFIFO
}

func (q *MemoryQueue) Name() string {
	return q.name
}

func (q *MemoryQueue) Enqueue(ctx context.Context, data []byte) error {
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

func (q *MemoryQueue) Dequeue(ctx context.Context) (Message, error) {
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

func (q *MemoryQueue) Subscribe(cb Handler) {
	q.callbacks = append(q.callbacks, cb)
}

func (q *MemoryQueue) run() {
Loop:
	for {
		select {
		case <-q.exitChannel:
			break Loop
		default:
			if q.callbacks == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Randomly pick up one.
			index := rand.Intn(len(q.callbacks))
			cb := q.callbacks[index]

			msg, err := q.Dequeue(context.TODO())
			if err != nil {
				time.Sleep(q.config.PollInterval)
				continue Loop
			}

			cb(msg)

			time.Sleep(q.config.PollInterval)
		}
	}
}

func (q *MemoryQueue) Recover(ctx context.Context, msg Message) error {
	if msg.RetryCount() > q.config.MaxHandleFailures {
		// Just ignore it for now
		return nil
	}

	msg.AddRetryCount()
	msg.RefreshUpdatedAt()

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

func (q *MemoryQueue) Purge(ctx context.Context) error {
	err := q.GetLocker().Lock(ctx)
	if err != nil {
		return err
	}

	defer q.GetLocker().Unlock(ctx)
	q.queue = nil
	return nil
}
