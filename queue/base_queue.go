package queue

import (
	"reflect"
	"sync"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
)

var (
	_ Queue = &BaseQueue{}
)

type BaseQueue struct {
	m           sync.Mutex
	name        string
	config      *Config
	cb          Handler
	exitChannel chan int
}

func NewBaseQueue(name string, options *Config) *BaseQueue {
	q := &BaseQueue{
		name:        name,
		config:      options,
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
	return q.config.MaxSize
}

func (q *BaseQueue) MaxHandleFailures() int {
	return q.config.MaxHandleFailures
}

func (q *BaseQueue) Enqueue(data []byte) error {
	return common.ErrNotImplemented
}

func (q *BaseQueue) Dequeue() (Message, error) {
	return nil, common.ErrNotImplemented
}

func (q *BaseQueue) Subscribe(cb Handler) {
	q.cb = cb
}

func (q *BaseQueue) ValidateQueueClosed() error {
	select {
	case <-q.exitChannel:
		return ErrQueueClosed
	default:
	}

	return nil
}

func (q *BaseQueue) Pack(data []byte) ([]byte, error) {
	msg, err := q.NewMessage(data)
	if err != nil {
		return nil, err
	}

	packedData, err := msg.Pack()
	if err != nil {
		return nil, err
	}
	return packedData, nil
}

func (q *BaseQueue) Unpack(data []byte) (Message, error) {
	msg, err := q.NewMessage(data)
	if err != nil {
		return nil, err
	}

	err = msg.Unpack(data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (q *BaseQueue) NewMessage(data []byte) (Message, error) {
	if q.config.Message == nil {
		panic("message type is not set in config")
	}

	msg := reflect.New(reflect.TypeOf(q.config.Message).Elem()).Interface().(Message)

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
