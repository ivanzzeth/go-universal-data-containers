package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/adjust/rmq/v5"
	"github.com/redis/go-redis/v9"
)

var (
	_ Queue[any]            = (*RedisQueue[any])(nil)
	_ RecoverableQueue[any] = (*RedisQueue[any])(nil)
	_ Factory[any]          = (*RedisQueueFactory[any])(nil)
)

const (
	redisQueueTestMsg = "test-message-facgasdffadspoiubsf"
)

type RedisQueueFactory[T any] struct {
	rmqConn    rmq.Connection
	defaultMsg Message[T]
}

func NewRedisQueueFactory[T any](redisClient redis.Cmdable, defaultMsg Message[T]) *RedisQueueFactory[T] {
	errChan := make(chan error, 100)
	go func() {
		for err := range errChan {
			if err != nil {
				// TODO: logging
				// fmt.Printf("redis queue got err: %v\n", err)
			}
		}
	}()

	rmqConn, err := rmq.OpenConnectionWithRedisClient("rmq", redisClient, errChan)
	if err != nil {
		panic(fmt.Errorf("failed to open rmq connection: %v", err))
	}

	return &RedisQueueFactory[T]{
		rmqConn:    rmqConn,
		defaultMsg: defaultMsg,
	}
}

func (f *RedisQueueFactory[T]) GetOrCreate(name string, options ...Option) (Queue[T], error) {
	q, err := NewRedisQueue(f.rmqConn, name, f.defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	return NewSimpleQueue[T](q)
}

func (f *RedisQueueFactory[T]) GetOrCreateSafe(name string, options ...Option) (SafeQueue[T], error) {
	q, err := NewRedisQueue(f.rmqConn, name, f.defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	return NewSimpleQueue(q)
}

type RedisQueue[T any] struct {
	*BaseQueue[T]
	q rmq.Queue
}

func NewRedisQueue[T any](conn rmq.Connection, name string, defaultMsg Message[T], options ...Option) (*RedisQueue[T], error) {
	baseQueue, err := NewBaseQueue(name, defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	queue, err := conn.OpenQueue(name)
	if err != nil {
		return nil, err
	}

	err = queue.StartConsuming(1, baseQueue.config.PollInterval)
	if err != nil {
		return nil, err
	}

	// FIXME: First message will be lost, then publish a test message to avoid this.
	err = queue.PublishBytes([]byte(redisQueueTestMsg))
	if err != nil {
		return nil, err
	}

	// go func() {
	// 	for {
	// 		time.Sleep(options.PollInterval)
	// 		select {
	// 		case <-baseQueue.exitChannel:
	// 			return
	// 		default:
	// 		}
	// unackedReturned, err := queue.ReturnUnacked(10)
	// if err != nil {
	// TODO: logging
	// 	fmt.Printf("redis queue got err: %v\n", err)
	// 	continue
	// }

	// if unackedReturned > 0 {
	// TODO: logging
	// fmt.Printf("redis queue returned %d unacked messages\n", unackedReturned)
	// }

	// rejectedReturned, err := queue.ReturnRejected(10)
	// if err != nil {
	// 	// TODO: logging
	// 	if !errors.Is(err, rmq.ErrorNotFound) {
	// 		// fmt.Printf("redis queue got err: %v\n", err)

	// 		continue
	// 	}
	// }

	// if rejectedReturned > 0 {
	// 	// TODO: logging
	// 	// fmt.Printf("redis queue returned %d rejected messages\n", rejectedReturned)
	// }
	// }
	// }()

	return &RedisQueue[T]{
		BaseQueue: baseQueue,
		q:         queue,
	}, nil
}

func (q *RedisQueue[T]) MaxSize() int {
	return UnlimitedSize
}

func (q *RedisQueue[T]) Enqueue(ctx context.Context, data T) error {
	// fmt.Printf("Enqueue %v\n", data)
	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	// q.GetLocker().Lock(ctx)
	// defer q.GetLocker().Unlock(ctx)

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	return q.q.PublishBytes(packedData)
}

func (q *RedisQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	data, err := q.q.Drain(1)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrQueueEmpty
		}

		return nil, err
	}

	// bytes := [][]byte{}
	// for _, d := range data {
	// 	bytes = append(bytes, []byte(d))
	// }
	// fmt.Printf("Dequeue %v\n", bytes)

	if len(data) != 1 {
		return nil, fmt.Errorf("expected 1 message, got %d", len(data))
	}

	if data[0] == redisQueueTestMsg {
		return q.Dequeue(ctx)
	}

	packedData := []byte(data[0])
	msg, err := q.Unpack(packedData)
	if err != nil {
		// Consider the message is test message, just ignore it and dequeue again
		// fmt.Printf("failed to unpack message: %v\n", err)
		return q.Dequeue(ctx)
	}

	if msg.RetryCount() > q.config.MaxHandleFailures {
		msg.RefreshRetryCount()
	}

	return msg, nil
}

func (q *RedisQueue[T]) Subscribe(cb Handler[T]) {
	if q.cb == nil {
		q.cb = cb

		for i := 0; i < q.config.ConsumerCount; i++ {
			consumerName := fmt.Sprintf("%v-consumer-%v", q.Name(), i)
			q.q.AddConsumerFunc(consumerName, func(delivery rmq.Delivery) {
				var (
					err error
					msg Message[T]
				)
				data := delivery.Payload()
				// fmt.Printf("Subscribe %v\n", data)
				if data != redisQueueTestMsg {
					// fmt.Printf("Handle %v\n", data)
					msg, err = q.Unpack([]byte(data))
					if err != nil {
						// TODO: logging
						return
					}
				}

				defer func() {
					for i := 0; i < q.config.MaxRetries; i++ {
						err = delivery.Ack()
						if err != nil {
							// TODO: logging
							continue
						}

						return
					}
				}()

				if msg != nil {
					err = cb(msg)
					if err != nil {
						// fmt.Printf("Handle failed %v: %v\n", data, err)
						return
					}
				}
			})
		}
	}
}

func (q *RedisQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	if msg.RetryCount() >= q.config.MaxHandleFailures {
		// Just ignore it for now
		return nil
	}
	msg.AddRetryCount()
	msg.RefreshUpdatedAt()

	packedData, err := msg.Pack()
	if err != nil {
		return err
	}

	err = q.q.PublishBytes(packedData)
	if err != nil {
		return err
	}

	return nil
}
