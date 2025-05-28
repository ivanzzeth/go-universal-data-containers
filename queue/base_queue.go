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
	_ Queue[any] = &BaseQueue[any]{}
)

type BaseQueue[T any] struct {
	locker locker.SyncLocker
	name   string

	defaultMsg  Message[T]
	config      *Config
	cb          Handler[T]
	exitChannel chan int
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
		exitChannel: make(chan int),
	}

	return q, nil
}

func (q *BaseQueue[T]) Close() {
	close(q.exitChannel)
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

func (q *BaseQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	return nil, common.ErrNotImplemented
}

func (q *BaseQueue[T]) Subscribe(cb Handler[T]) {
	q.cb = cb
}

func (q *BaseQueue[T]) ValidateQueueClosed() error {
	select {
	case <-q.exitChannel:
		return ErrQueueClosed
	default:
	}

	return nil
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
