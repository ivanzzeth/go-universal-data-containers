package queue

import (
	"sync"
	"time"
)

var (
	_ Factory          = &MemoryFactory{}
	_ Queue            = &MemoryQueue{}
	_ RecoverableQueue = &MemoryQueue{}
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

func (f *MemoryFactory) GetOrCreate(name string, options ...func(*QueueOptions)) (Queue, error) {
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

func NewMemoryQueue(name string, options *QueueOptions) *MemoryQueue {
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
	err := q.BaseQueue.Enqueue(data)
	if err != nil {
		return err
	}

	q.m.Lock()
	defer q.m.Unlock()

	if q.MaxSize() > 0 && len(q.queue) >= q.MaxSize() {
		return ErrQueueFull
	}

	q.queue = append(q.queue, data)
	return nil
}

func (q *MemoryQueue) Dequeue() ([]byte, error) {
	q.m.Lock()
	defer q.m.Unlock()
	if len(q.queue) > 0 {
		data := q.queue[0]
		q.queue = q.queue[1:]
		return data, nil
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

			msgBytes, err := q.Dequeue()
			if err != nil {
				time.Sleep(q.options.PollInterval)
				continue Loop
			}

			q.cb(msgBytes)
		}
	}
}

func (q *MemoryQueue) Recover(b []byte) error {
	q.m.Lock()
	defer q.m.Unlock()
	q.queue = append([][]byte{b}, q.queue...)
	return nil
}
