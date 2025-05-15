package queue

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	_ Queue = &BaseQueue{}
)

type BaseQueue struct {
	locker      locker.SyncLocker
	name        string
	config      *Config
	cb          Handler
	exitChannel chan int
}

func NewBaseQueue(name string, options *Config) (*BaseQueue, error) {
	locker, err := options.LockerGenerator.CreateSyncLocker(fmt.Sprintf("queue-locker-%v", name))
	if err != nil {
		return nil, err
	}
	q := &BaseQueue{
		locker:      locker,
		name:        name,
		config:      options,
		exitChannel: make(chan int),
	}

	return q, nil
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

func (q *BaseQueue) GetLocker() locker.SyncLocker {
	return q.locker
}

func (q *BaseQueue) MaxSize() int {
	return q.config.MaxSize
}

func (q *BaseQueue) MaxHandleFailures() int {
	return q.config.MaxHandleFailures
}

func (q *BaseQueue) Enqueue(ctx context.Context, data []byte) error {
	return common.ErrNotImplemented
}

func (q *BaseQueue) Dequeue(ctx context.Context) (Message, error) {
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
