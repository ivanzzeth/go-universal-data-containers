package queue

import (
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
	table map[string]Queue
}

func NewMemoryFactory() *MemoryFactory {
	return &MemoryFactory{
		table: make(map[string]Queue),
	}
}

func (f *MemoryFactory) GetOrCreate(name string, options ...Option) (Queue, error) {
	ops := DefaultOptions
	for _, op := range options {
		op(&ops)
	}

	f.m.Lock()
	defer f.m.Unlock()
	if _, ok := f.table[name]; !ok {
		q, err := NewSafeQueue(NewMemoryQueue(name, &ops))
		if err != nil {
			return nil, err
		}
		f.table[name] = q
	}

	return f.table[name], nil
}

type MemoryQueue struct {
	*BaseQueue
	queue [][]byte
	cb    Handler
}

func NewMemoryQueue(name string, options *Config) *MemoryQueue {
	q := &MemoryQueue{
		BaseQueue: NewBaseQueue(name, options),
	}

	go q.run()

	return q
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

func (q *MemoryQueue) Enqueue(data []byte) error {
	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	q.m.Lock()
	defer q.m.Unlock()

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

func (q *MemoryQueue) Dequeue() (Message, error) {
	q.m.Lock()
	defer q.m.Unlock()
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
	q.cb = cb
}

func (q *MemoryQueue) run() {
Loop:
	for {
		select {
		case <-q.exitChannel:
			break Loop
		default:
			if q.cb == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			msg, err := q.Dequeue()
			if err != nil {
				time.Sleep(q.config.PollInterval)
				continue Loop
			}

			q.cb(msg)
		}
	}
}

func (q *MemoryQueue) Recover(msg Message) error {
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

	q.m.Lock()
	defer q.m.Unlock()
	q.queue = append([][]byte{packedData}, q.queue...)
	return nil
}

func (q *MemoryQueue) Purge() error {
	q.m.Lock()
	defer q.m.Unlock()
	q.queue = nil
	return nil
}
