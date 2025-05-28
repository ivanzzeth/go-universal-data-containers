package queue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	_ Queue[any]            = (*RedisQueue[any])(nil)
	_ RecoverableQueue[any] = (*RedisQueue[any])(nil)
	_ Factory[any]          = (*RedisQueueFactory[any])(nil)
)

type RedisQueueFactory[T any] struct {
	redisClient redis.Cmdable
	defaultMsg  Message[T]
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

	return &RedisQueueFactory[T]{
		redisClient: redisClient,
		defaultMsg:  defaultMsg,
	}
}

func (f *RedisQueueFactory[T]) GetOrCreate(name string, options ...Option) (Queue[T], error) {
	q, err := NewRedisQueue(f.redisClient, name, f.defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	return NewSimpleQueue[T](q)
}

func (f *RedisQueueFactory[T]) GetOrCreateSafe(name string, options ...Option) (SafeQueue[T], error) {
	q, err := NewRedisQueue(f.redisClient, name, f.defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	return NewSimpleQueue(q)
}

type RedisQueue[T any] struct {
	*BaseQueue[T]
	redisClient redis.Cmdable

	name string
}

func NewRedisQueue[T any](redisClient redis.Cmdable, name string, defaultMsg Message[T], options ...Option) (*RedisQueue[T], error) {
	baseQueue, err := NewBaseQueue(name, defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	q := &RedisQueue[T]{
		BaseQueue:   baseQueue,
		redisClient: redisClient,
		name:        name,
	}

	go q.run()

	return q, nil
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

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	return q.redisClient.RPush(ctx, q.GetQueueKey(), packedData).Err()
}

func (q *RedisQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	data, err := q.redisClient.LPop(ctx, q.GetQueueKey()).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrQueueEmpty
	}

	packedData := []byte(data)
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

func (q *RedisQueue[T]) run() {
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

			msg, err := q.Dequeue(context.TODO())
			if err != nil {
				time.Sleep(q.config.PollInterval)
				continue Loop
			}

			q.TriggerCallbacks(msg)

			time.Sleep(q.config.PollInterval)
		}
	}
}

func (q *RedisQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	if msg.RetryCount() >= q.config.MaxHandleFailures {
		// Just ignore it for now
		return nil
	}

	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	msg.AddRetryCount()
	msg.RefreshUpdatedAt()

	packedData, err := msg.Pack()
	if err != nil {
		return err
	}

	return q.redisClient.RPush(ctx, q.GetQueueKey(), packedData).Err()
}
