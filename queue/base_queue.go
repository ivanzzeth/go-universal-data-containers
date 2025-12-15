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

	defaultMsg Message[T]
	config     *Config
	callbacks  []Handler[T]

	msgBuffer   chan struct{}
	exitChannel chan int

	dlq DLQ[T]
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
		msgBuffer:   make(chan struct{}, ops.ConsumerCount),
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

func (q *BaseQueue[T]) BEnqueue(ctx context.Context, data T) error {
	return common.ErrNotImplemented
}

func (q *BaseQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	return nil, common.ErrNotImplemented
}

func (q *BaseQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	return nil, common.ErrNotImplemented
}

func (q *BaseQueue[T]) Subscribe(cb Handler[T]) {
	q.locker.Lock(context.Background())
	defer q.locker.Unlock(context.Background())

	q.callbacks = append(q.callbacks, cb)
}

func (q *BaseQueue[T]) TriggerCallbacks(msg Message[T]) {
	q.msgBuffer <- struct{}{}

	go func() {
		q.locker.Lock(context.Background())
		callbacks := q.callbacks
		q.locker.Unlock(context.Background())

		for _, cb := range callbacks {
			cb(msg)
		}

		<-q.msgBuffer
	}()
}

func (q *BaseQueue[T]) ValidateQueueClosed() error {
	select {
	case <-q.exitChannel:
		return ErrQueueClosed
	default:
	}

	return nil
}

func (q *BaseQueue[T]) GetQueueKey() string {
	return fmt.Sprintf("%v::%v", Namespace, q.name)
}

func (q *BaseQueue[T]) GetDeadletterQueueName() string {
	return fmt.Sprintf("%v::DLQ", q.name)
}

func (q *BaseQueue[T]) GetDeadletterQueueKey() string {
	return fmt.Sprintf("%v::%v", Namespace, q.GetDeadletterQueueName())
}

func (q *BaseQueue[T]) SetDLQ(dlq DLQ[T]) {
	q.dlq = dlq
}

func (q *BaseQueue[T]) DLQ() (DLQ[T], error) {
	if q.dlq == nil {
		return nil, common.ErrNotImplemented
	}

	return q.dlq, nil
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

	// Make sure the packed data is the same as the original data
	msgToTest, err := q.NewMessage(data)
	if err != nil {
		return nil, err
	}

	err = msgToTest.Unpack(packedData)
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
