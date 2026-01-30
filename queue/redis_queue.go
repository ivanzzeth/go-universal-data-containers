package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

	return NewSimpleQueue(q)
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

	// Tracks in-flight messages for graceful shutdown
	inflightWg sync.WaitGroup

	// Indicates queue is closing, prevents new enqueues
	closing atomic.Bool
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

func (q *RedisQueue[T]) Close() {
	// Mark as closing to prevent new enqueues
	q.closing.Store(true)

	// Wait for all in-flight messages to be processed
	q.inflightWg.Wait()

	// Now close the exit channel
	q.BaseQueue.Close()
}

func (q *RedisQueue[T]) Enqueue(ctx context.Context, data T) error {
	if q.closing.Load() {
		return ErrQueueClosed
	}

	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	length, err := q.redisClient.LLen(ctx, q.GetQueueKey()).Result()
	if err != nil {
		return err
	}

	if q.MaxSize() > 0 && int(length) >= q.MaxSize() {
		return ErrQueueFull
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	return q.redisClient.RPush(ctx, q.GetQueueKey(), packedData).Err()
}

func (q *RedisQueue[T]) BEnqueue(ctx context.Context, data T) error {
	// Redis does not have blocking push, so we use polling
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.ExitChannel():
			return ErrQueueClosed
		default:
			err := q.Enqueue(ctx, data)
			if err != nil {
				if errors.Is(err, ErrQueueFull) {
					time.Sleep(q.config.PollInterval)
					continue
				}

				return err
			}

			return nil
		}
	}
}

func (q *RedisQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	// Priority: retry queue first, then main queue
	// Non-blocking pop from retry queue
	data, err := q.redisClient.LPop(ctx, q.GetRetryQueueKey()).Result()
	if err == nil {
		return q.unpackMessage([]byte(data))
	}
	if !errors.Is(err, redis.Nil) {
		return nil, err
	}

	// Retry queue empty, try main queue
	data, err = q.redisClient.LPop(ctx, q.GetQueueKey()).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrQueueEmpty
	}
	if err != nil {
		return nil, err
	}

	return q.unpackMessage([]byte(data))
}

func (q *RedisQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.ExitChannel():
			return nil, ErrQueueClosed
		default:
		}

		// Use BLPOP with timeout for blocking dequeue
		// Priority: retry queue first, then main queue
		// BLPOP returns the first non-empty key, so we put retry queue first
		result, err := q.redisClient.BLPop(ctx, time.Second, q.GetRetryQueueKey(), q.GetQueueKey()).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// Timeout, continue waiting
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, ctx.Err()
			}
			// Network error or other, retry after short delay
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// result[0] is the key name, result[1] is the value
		if len(result) < 2 {
			continue
		}

		return q.unpackMessage([]byte(result[1]))
	}
}

func (q *RedisQueue[T]) unpackMessage(packedData []byte) (Message[T], error) {
	msg, err := q.Unpack(packedData)
	if err != nil {
		return nil, err
	}

	if msg.RetryCount() > q.config.MaxHandleFailures {
		msg.RefreshRetryCount()
	}

	return msg, nil
}

func (q *RedisQueue[T]) Purge(ctx context.Context) error {
	// Delete both main queue and retry queue
	err := q.redisClient.Del(ctx, q.GetQueueKey()).Err()
	if err != nil {
		return err
	}
	return q.redisClient.Del(ctx, q.GetRetryQueueKey()).Err()
}

func (q *RedisQueue[T]) run() {
	ctx := context.Background()

	for {
		// Check if queue is closed
		select {
		case <-q.ExitChannel():
			return
		default:
		}

		// Wait for callbacks to be registered
		// TODO: Optimize to use event-driven notification instead of time.Sleep
		if !q.HasCallbacks() {
			select {
			case <-q.ExitChannel():
				return
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		// Use BLPOP for event-driven message processing
		// Priority: retry queue first, then main queue
		result, err := q.redisClient.BLPop(ctx, time.Second, q.GetRetryQueueKey(), q.GetQueueKey()).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// Timeout, continue waiting
				continue
			}
			// Check if queue is closing
			select {
			case <-q.ExitChannel():
				return
			default:
			}
			// Network error or other, retry after short delay
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// result[0] is the key name, result[1] is the value
		if len(result) < 2 {
			continue
		}

		msg, err := q.unpackMessage([]byte(result[1]))
		if err != nil {
			continue
		}

		q.inflightWg.Add(1)
		q.TriggerCallbacks(ctx, msg)
		q.inflightWg.Done()
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

	// Push to retry queue (LPUSH for FIFO priority - will be processed first)
	return q.redisClient.LPush(ctx, q.GetRetryQueueKey(), packedData).Err()
}

// EnqueueToRetryQueue adds a message directly to the retry queue.
// Used by DLQ.Redrive to ensure recovered messages are processed with priority.
func (q *RedisQueue[T]) EnqueueToRetryQueue(ctx context.Context, data T) error {
	if q.closing.Load() {
		return ErrQueueClosed
	}

	err := q.BaseQueue.ValidateQueueClosed()
	if err != nil {
		return err
	}

	packedData, err := q.Pack(data)
	if err != nil {
		return err
	}

	// Push to retry queue (RPUSH to maintain order)
	return q.redisClient.RPush(ctx, q.GetRetryQueueKey(), packedData).Err()
}
