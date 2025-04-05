package queue

import (
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/common"
)

var (
	_ Queue = &BaseQueue{}
)

type BaseQueue struct {
	m           sync.Mutex
	name        string
	options     *QueueOptions
	cb          Handler
	exitChannel chan int
}

func NewBaseQueue(name string, options *QueueOptions) *BaseQueue {
	q := &BaseQueue{
		name:        name,
		options:     options,
		exitChannel: make(chan int),
	}

	return q
}

func (q *BaseQueue) Close() {
	close(q.exitChannel)
}

func (q *BaseQueue) Kind() Kind {
	return KindFIFO
}

func (q *BaseQueue) Name() string {
	return q.name
}

func (q *BaseQueue) MaxSize() int {
	return q.options.MaxSize
}

func (q *BaseQueue) Enqueue(data []byte) error {
	select {
	case <-q.exitChannel:
		return ErrQueueClosed
	default:
	}

	return nil
}

func (q *BaseQueue) Dequeue() ([]byte, error) {
	return nil, common.ErrNotImplemented
}

func (q *BaseQueue) Subscribe(cb Handler) {
	q.cb = cb
}
