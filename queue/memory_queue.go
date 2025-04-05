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

func (f *MemoryFactory) GetOrCreate(name string, maxSize int) Queue {
	f.m.Lock()
	defer f.m.Unlock()
	if _, ok := f.table[name]; !ok {
		f.table[name] = NewSafeQueue(NewMemoryQueue(maxSize, DefaultPollInterval))
	}

	return f.table[name]
}

type MemoryQueue struct {
	m            sync.Mutex
	maxSize      int
	pollInterval time.Duration
	queue        [][]byte
	cb           Handler
	exitChannel  chan int
}

func NewMemoryQueue(maxSize int, pollInterval time.Duration) *MemoryQueue {
	q := &MemoryQueue{
		maxSize:      maxSize,
		pollInterval: pollInterval,
		exitChannel:  make(chan int),
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

func (q *MemoryQueue) MaxSize() int {
	return q.maxSize
}

func (q *MemoryQueue) Enqueue(data []byte) error {
	q.m.Lock()
	defer q.m.Unlock()

	select {
	case <-q.exitChannel:
		return ErrQueueClosed
	default:
	}

	if q.maxSize > 0 && len(q.queue) >= q.maxSize {
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
				time.Sleep(q.pollInterval)
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
