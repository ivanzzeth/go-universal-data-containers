package queue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	_ Queue[any]   = (*RedisQueue[any])(nil)
	_ Factory[any] = (*RedisQueueFactory[any])(nil)

	_ RecoverableQueue[any] = &RedisQueue[any]{}
	_ Purgeable             = &RedisQueue[any]{}
	_ DLQer[any]            = &RedisQueue[any]{}
)

type RedisQueueFactory[T any] struct {
	redisClient redis.Cmdable
	defaultMsg  Message[T]
}

func NewRedisQueueFactory[T any](redisClient redis.Cmdable, defaultMsg Message[T]) *RedisQueueFactory[T] {
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
}

func NewRedisQueue[T any](redisClient redis.Cmdable, name string, defaultMsg Message[T], options ...Option) (*RedisQueue[T], error) {
	baseQueue, err := NewBaseQueue(name, defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	q := &RedisQueue[T]{
		BaseQueue:   baseQueue,
		redisClient: redisClient,
	}

	dlqBaseQueue, err := NewBaseQueue(q.GetDeadletterQueueName(), defaultMsg, options...)
	if err != nil {
		return nil, err
	}

	dlq := &RedisQueue[T]{
		BaseQueue:   dlqBaseQueue,
		redisClient: redisClient,
	}

	DLQ, err := newBaseDLQ(q, dlq)
	if err != nil {
		return nil, err
	}

	q.SetDLQ(DLQ)

	go q.run()
	go dlq.run()

	return q, nil
}

// func (q *RedisQueue[T]) MaxSize() int {
// 	return UnlimitedSize
// }

func (q *RedisQueue[T]) Enqueue(ctx context.Context, data T) error {
	// fmt.Printf("Enqueue %v\n", data)
	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	len, err := q.redisClient.LLen(ctx, q.GetQueueKey()).Result()
	if err != nil {
		return err
	}

	if q.MaxSize() > 0 && int(len) >= q.MaxSize() {
		return ErrQueueFull
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	return q.redisClient.RPush(ctx, q.GetQueueKey(), packedData).Err()
}

func (q *RedisQueue[T]) BEnqueue(ctx context.Context, data T) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := q.Enqueue(ctx, data)
			if err != nil {
				if errors.Is(err, ErrQueueFull) {
					time.Sleep(q.config.PollInterval)
					continue
				}

				return err
			}
		}
	}
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

func (q *RedisQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			msg, err := q.Dequeue(ctx)
			if err != nil {
				if errors.Is(err, ErrQueueEmpty) {
					time.Sleep(q.config.PollInterval)
					continue
				}

				return nil, err
			}

			return msg, nil
		}
	}
}

func (q *RedisQueue[T]) Purge(ctx context.Context) error {
	return q.redisClient.Del(ctx, q.GetQueueKey()).Err()
}

func (q *RedisQueue[T]) run() {
	ctx := context.Background()
Loop:
	for {
		select {
		case <-q.exitChannel:
			break Loop
		default:
			q.locker.Lock(ctx)
			callbacks := q.callbacks
			q.locker.Unlock(ctx)

			if callbacks == nil {
				time.Sleep(q.config.PollInterval)
				continue
			}

			msg, err := q.Dequeue(ctx)
			if err != nil {
				time.Sleep(q.config.PollInterval)
				continue Loop
			}

			q.TriggerCallbacks(ctx, msg)

			// time.Sleep(q.config.PollInterval)
		}
	}
}

func (q *RedisQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	if msg.RetryCount() >= q.config.MaxHandleFailures {
		err := q.dlq.Enqueue(ctx, msg.Data())
		if err != nil {
			return err
		}

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
