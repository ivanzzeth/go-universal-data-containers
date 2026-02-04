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

	_ RecoverableQueue[any]   = &RedisQueue[any]{}
	_ Purgeable               = &RedisQueue[any]{}
	_ DLQer[any]              = &RedisQueue[any]{}
	_ StatsProvider           = &RedisQueue[any]{}
	_ RetryQueueEnqueuer[any] = &RedisQueue[any]{}
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
	q.BaseQueue.GracefulClose()
}

func (q *RedisQueue[T]) Enqueue(ctx context.Context, data T) error {
	if q.IsClosing() {
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
		return q.UnpackMessage([]byte(data))
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

	return q.UnpackMessage([]byte(data))
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
		result, err := q.redisClient.BLPop(ctx, DefaultBlockingTimeout, q.GetRetryQueueKey(), q.GetQueueKey()).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// Timeout, continue waiting
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, ctx.Err()
			}
			// Network error or other, retry after short delay
			time.Sleep(DefaultNetworkRetryDelay)
			continue
		}

		// result[0] is the key name, result[1] is the value
		if len(result) < 2 {
			continue
		}

		return q.UnpackMessage([]byte(result[1]))
	}
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
		// Check if queue is closed or closing.
		// IMPORTANT: Must check IsClosing() to prevent WaitGroup race condition.
		// When GracefulClose() is called, it sets closing=true then calls Wait().
		// If we don't check IsClosing() here, we might call AddInflight() while
		// Wait() is already running, causing "WaitGroup is reused before previous
		// Wait has returned" panic.
		select {
		case <-q.ExitChannel():
			return
		default:
			if q.IsClosing() {
				return
			}
		}

		// Wait for callbacks to be registered
		// TODO: Optimize to use event-driven notification instead of time.Sleep
		if !q.HasCallbacks() {
			select {
			case <-q.ExitChannel():
				return
			case <-time.After(DefaultCallbackWaitInterval):
				continue
			}
		}

		// Use BLPOP for event-driven message processing
		// Priority: retry queue first, then main queue
		result, err := q.redisClient.BLPop(ctx, DefaultBlockingTimeout, q.GetRetryQueueKey(), q.GetQueueKey()).Result()
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
				if q.IsClosing() {
					return
				}
			}
			// Network error or other, retry after short delay
			time.Sleep(DefaultNetworkRetryDelay)
			continue
		}

		// result[0] is the key name, result[1] is the value
		if len(result) < 2 {
			continue
		}

		msg, err := q.UnpackMessage([]byte(result[1]))
		if err != nil {
			continue
		}

		// AddInflight checks IsClosing() atomically to prevent race condition
		// with GracefulClose().Wait(). If it returns false, the queue is closing.
		if !q.AddInflight() {
			return
		}
		q.TriggerCallbacks(ctx, msg)
		q.DoneInflight()
	}
}

func (q *RedisQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	sentToDLQ, err := q.ShouldSendToDLQ(ctx, msg)
	if err != nil {
		return err
	}
	if sentToDLQ {
		return nil
	}

	err = q.BaseQueue.ValidateQueueClosed()
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
	if q.IsClosing() {
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

// Stats returns the current queue statistics.
// Implements the StatsProvider interface.
func (q *RedisQueue[T]) Stats() QueueStats {
	ctx := context.Background()

	// Get main queue depth
	mainDepth, err := q.redisClient.LLen(ctx, q.GetQueueKey()).Result()
	if err != nil {
		mainDepth = 0
	}

	// Get retry queue depth
	retryDepth, err := q.redisClient.LLen(ctx, q.GetRetryQueueKey()).Result()
	if err != nil {
		retryDepth = 0
	}

	// Calculate capacity
	capacity := q.config.MaxSize
	retryCapacity := q.config.RetryQueueCapacity
	if retryCapacity <= 0 {
		retryCapacity = DefaultRetryQueueCapacity
	}

	return QueueStats{
		Depth:         mainDepth,
		RetryDepth:    retryDepth,
		ConsumerCount: len(q.GetCallbacks()),
		Capacity:      capacity,
		RetryCapacity: retryCapacity,
	}
}
