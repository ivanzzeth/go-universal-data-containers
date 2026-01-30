package queue

import (
	"context"
	"fmt"
	"math"
	"runtime/debug"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/metrics"
	"github.com/rs/zerolog"
)

var (
	_ Queue[any]     = &SimpleQueue[any]{}
	_ SafeQueue[any] = &SimpleQueue[any]{}
)

// SimpleQueueConfig holds configuration for SimpleQueue
type SimpleQueueConfig struct {
	Logger *zerolog.Logger
}

// SimpleQueueOption is a function option for configuring SimpleQueue
type SimpleQueueOption func(*SimpleQueueConfig)

// WithLogger sets the logger for SimpleQueue
func WithLogger(logger *zerolog.Logger) SimpleQueueOption {
	return func(cfg *SimpleQueueConfig) {
		cfg.Logger = logger
	}
}

// SafeQueue provides ability to put message back to queue when handler encounters panic
// and makes sure all function calls are safe.
// e.g, Returns ErrNotImplemented if calling Recover and it is not implemented
type SafeQueue[T any] interface {
	Queue[T]

	Recoverable[T]
	IsRecoverable() bool

	Purgeable
	IsPurgeable() bool

	DLQer[T]
	IsDLQSupported() bool
}

type SimpleQueue[T any] struct {
	queue  Queue[T]
	logger *zerolog.Logger
}

func NewSimpleQueue[T any](queue Queue[T], opts ...SimpleQueueOption) (*SimpleQueue[T], error) {
	cfg := &SimpleQueueConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	q := &SimpleQueue[T]{
		queue:  queue,
		logger: cfg.Logger,
	}

	// Initialize capacity metrics
	q.initializeCapacityMetrics()

	// Initialize config info metric
	q.initializeConfigInfoMetric()

	return q, nil
}

// logIfEnabled is a helper method to check if logger is set
func (q *SimpleQueue[T]) logIfEnabled() *zerolog.Logger {
	return q.logger
}

// kindString returns a string representation of the queue kind
func (q *SimpleQueue[T]) kindString() string {
	switch q.Kind() {
	case KindFIFO:
		return "FIFO"
	case KindStandard:
		return "Standard"
	default:
		return fmt.Sprintf("Unknown(%d)", q.Kind())
	}
}

func (q *SimpleQueue[T]) Unwrap() Queue[T] {
	return q.queue
}

func (q *SimpleQueue[T]) Kind() Kind {
	return q.queue.Kind()
}

func (q *SimpleQueue[T]) Name() string {
	return q.queue.Name()
}

func (q *SimpleQueue[T]) Close() {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Closing queue")
	}
	q.queue.Close()
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Msg("Queue closed successfully")
	}
}

func (q *SimpleQueue[T]) MaxSize() int {
	return q.queue.MaxSize()
}

func (q *SimpleQueue[T]) MaxHandleFailures() int {
	return q.queue.MaxHandleFailures()
}

func (q *SimpleQueue[T]) Enqueue(ctx context.Context, data T) error {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Enqueuing message")
	}

	startTime := time.Now()
	metrics.MetricQueueEnqueueTotal.WithLabelValues(q.Name()).Inc()
	err := q.queue.Enqueue(ctx, data)
	duration := time.Since(startTime)
	metrics.MetricQueueEnqueueDuration.WithLabelValues(q.Name()).Observe(duration.Seconds())

	if err != nil {
		metrics.MetricQueueEnqueueErrorTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Error().
				Err(err).
				Str("queue_name", q.Name()).
				Str("queue_kind", q.kindString()).
				Msg("Failed to enqueue message")
		}
		return err
	}

	// Update depth metrics after successful enqueue
	q.updateDepthMetrics()

	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Message enqueued successfully")
	}

	return nil
}

func (q *SimpleQueue[T]) BEnqueue(ctx context.Context, data T) error {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Blocking enqueue: waiting for queue space")
	}

	startTime := time.Now()
	metrics.MetricQueueEnqueueTotal.WithLabelValues(q.Name()).Inc()
	err := q.queue.BEnqueue(ctx, data)
	duration := time.Since(startTime)
	metrics.MetricQueueEnqueueDuration.WithLabelValues(q.Name()).Observe(duration.Seconds())

	if err != nil {
		metrics.MetricQueueEnqueueErrorTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Error().
				Err(err).
				Str("queue_name", q.Name()).
				Str("queue_kind", q.kindString()).
				Msg("Failed to blocking enqueue message")
		}
		return err
	}

	// Update depth metrics after successful enqueue
	q.updateDepthMetrics()

	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Message blocking enqueued successfully")
	}

	return nil
}

func (q *SimpleQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Dequeuing message")
	}

	startTime := time.Now()
	metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
	data, err := q.queue.Dequeue(ctx)
	duration := time.Since(startTime)
	metrics.MetricQueueDequeueDuration.WithLabelValues(q.Name()).Observe(duration.Seconds())

	if err != nil {
		metrics.MetricQueueDequeueErrorTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Warn().
				Err(err).
				Str("queue_name", q.Name()).
				Str("queue_kind", q.kindString()).
				Msg("Failed to dequeue message (queue may be empty)")
		}
		return nil, err
	}

	// Update depth metrics after successful dequeue
	q.updateDepthMetrics()

	// Record message age (time since creation)
	messageAge := time.Since(data.CreatedAt())
	metrics.MetricQueueMessageAge.WithLabelValues(q.Name()).Observe(messageAge.Seconds())

	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Int("retry_count", data.RetryCount()).
			Int("total_retry_count", data.TotalRetryCount()).
			Time("created_at", data.CreatedAt()).
			Msg("Message dequeued successfully")
	}

	return data, nil
}

func (q *SimpleQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Blocking dequeue: waiting for message")
	}

	startTime := time.Now()
	metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
	data, err := q.queue.BDequeue(ctx)
	duration := time.Since(startTime)
	metrics.MetricQueueDequeueDuration.WithLabelValues(q.Name()).Observe(duration.Seconds())

	if err != nil {
		metrics.MetricQueueDequeueErrorTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Error().
				Err(err).
				Str("queue_name", q.Name()).
				Str("queue_kind", q.kindString()).
				Msg("Failed to blocking dequeue message")
		}
		return nil, err
	}

	// Update depth metrics after successful dequeue
	q.updateDepthMetrics()

	// Record message age (time since creation)
	messageAge := time.Since(data.CreatedAt())
	metrics.MetricQueueMessageAge.WithLabelValues(q.Name()).Observe(messageAge.Seconds())

	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Int("retry_count", data.RetryCount()).
			Int("total_retry_count", data.TotalRetryCount()).
			Time("created_at", data.CreatedAt()).
			Msg("Message blocking dequeued successfully")
	}

	return data, nil
}

func (q *SimpleQueue[T]) Subscribe(ctx context.Context, cb Handler[T]) {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Str("queue_kind", q.kindString()).
			Msg("Subscribing to queue")
	}

	q.queue.Subscribe(ctx, func(ctx context.Context, msg Message[T]) error {
		startTime := time.Now()

		// Increment inflight counter when starting to process
		metrics.MetricQueueInflight.WithLabelValues(q.Name()).Inc()

		// Record message age (time since creation)
		messageAge := time.Since(msg.CreatedAt())
		metrics.MetricQueueMessageAge.WithLabelValues(q.Name()).Observe(messageAge.Seconds())

		if logger := q.logIfEnabled(); logger != nil {
			logger.Debug().
				Str("queue_name", q.Name()).
				Int("retry_count", msg.RetryCount()).
				Int("total_retry_count", msg.TotalRetryCount()).
				Time("created_at", msg.CreatedAt()).
				Msg("Processing message from subscription")
		}

		defer func() {
			duration := time.Since(startTime)
			metrics.MetricQueueHandleDuration.WithLabelValues(q.Name()).Observe(duration.Seconds())

			// Decrement inflight counter when done processing
			metrics.MetricQueueInflight.WithLabelValues(q.Name()).Dec()

			// Update depth metrics after processing
			q.updateDepthMetrics()

			if logger := q.logIfEnabled(); logger != nil {
				logger.Debug().
					Str("queue_name", q.Name()).
					Dur("duration", duration).
					Msg("Message processing completed")
			}
		}()

		err := q.handle(ctx, msg, cb)
		if err != nil {
			metrics.MetricQueueHandleErrorTotal.WithLabelValues(q.Name()).Inc()
			if logger := q.logIfEnabled(); logger != nil {
				logger.Error().
					Err(err).
					Str("queue_name", q.Name()).
					Int("retry_count", msg.RetryCount()).
					Int("total_retry_count", msg.TotalRetryCount()).
					Msg("Message handling failed")
			}
			return err
		}

		metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
		metrics.MetricQueueHandleSuccessulTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Debug().
				Str("queue_name", q.Name()).
				Int("retry_count", msg.RetryCount()).
				Int("total_retry_count", msg.TotalRetryCount()).
				Msg("Message handled successfully")
		}
		return nil
	})

	// Update consumers active metric after subscription
	q.updateConsumersMetric()

	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Msg("Subscription registered successfully")
	}
}

func (q *SimpleQueue[T]) handle(ctx context.Context, msg Message[T], cb Handler[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrQueueRecovered
			stackTrace := string(debug.Stack())
			if logger := q.logIfEnabled(); logger != nil {
				logger.Error().
					Interface("panic_value", r).
					Str("stack_trace", stackTrace).
					Str("queue_name", q.Name()).
					Int("retry_count", msg.RetryCount()).
					Int("total_retry_count", msg.TotalRetryCount()).
					Msg("Recovered from panic in message handler")
			}
			metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

			if queue, ok := q.queue.(RecoverableQueue[T]); ok {
				if logger := q.logIfEnabled(); logger != nil {
					logger.Debug().
						Str("queue_name", q.Name()).
						Int("max_retries", DefaultMaxRetries).
						Msg("Attempting to recover message after panic")
				}

				var recoverErr error
				for i := 0; i < DefaultMaxRetries; i++ {
					recoverErr = queue.Recover(ctx, msg)
					if recoverErr != nil {
						backoff := time.Duration(math.Pow(2, float64(i))) * 10 * time.Millisecond
						if logger := q.logIfEnabled(); logger != nil {
							logger.Debug().
								Err(recoverErr).
								Str("queue_name", q.Name()).
								Int("retry_attempt", i+1).
								Int("max_retries", DefaultMaxRetries).
								Dur("backoff", backoff).
								Msg("Recovery attempt failed, retrying with backoff")
						}
						time.Sleep(backoff)
						continue
					}

					if logger := q.logIfEnabled(); logger != nil {
						logger.Debug().
							Str("queue_name", q.Name()).
							Int("retry_attempt", i+1).
							Msg("Message recovered successfully after panic")
					}
					break
				}

				if recoverErr != nil {
					metrics.MetricQueueRecoverErrorTotal.WithLabelValues(q.Name()).Inc()
					if logger := q.logIfEnabled(); logger != nil {
						logger.Error().
							Err(recoverErr).
							Str("queue_name", q.Name()).
							Int("max_retries", DefaultMaxRetries).
							Msg("Failed to recover message after all retry attempts")
					}
				}
			} else {
				if logger := q.logIfEnabled(); logger != nil {
					logger.Warn().
						Str("queue_name", q.Name()).
						Msg("Queue does not support recovery, message will be lost")
				}
			}
		}
	}()

	err = cb(ctx, msg)
	if err != nil {
		if logger := q.logIfEnabled(); logger != nil {
			logger.Warn().
				Err(err).
				Str("queue_name", q.Name()).
				Int("retry_count", msg.RetryCount()).
				Int("total_retry_count", msg.TotalRetryCount()).
				Msg("Handler returned error, attempting recovery")
		}

		if q.IsRecoverable() {
			if logger := q.logIfEnabled(); logger != nil {
				logger.Debug().
					Str("queue_name", q.Name()).
					Int("max_retries", DefaultMaxRetries).
					Msg("Attempting to recover message after handler error")
			}

			var recoverErr error
			for i := 0; i < DefaultMaxRetries; i++ {
				recoverErr = q.Recover(ctx, msg)
				if recoverErr != nil {
					backoff := time.Duration(math.Pow(2, float64(i))) * 10 * time.Millisecond
					if logger := q.logIfEnabled(); logger != nil {
						logger.Debug().
							Err(recoverErr).
							Str("queue_name", q.Name()).
							Int("retry_attempt", i+1).
							Int("max_retries", DefaultMaxRetries).
							Dur("backoff", backoff).
							Msg("Recovery attempt failed, retrying with backoff")
					}
					time.Sleep(backoff)
					continue
				}

				if logger := q.logIfEnabled(); logger != nil {
					logger.Debug().
						Str("queue_name", q.Name()).
						Int("retry_attempt", i+1).
						Msg("Message recovered successfully after handler error")
				}
				break
			}

			if recoverErr != nil {
				metrics.MetricQueueRecoverErrorTotal.WithLabelValues(q.Name()).Inc()
				if logger := q.logIfEnabled(); logger != nil {
					logger.Error().
						Err(recoverErr).
						Str("queue_name", q.Name()).
						Int("max_retries", DefaultMaxRetries).
						Msg("Failed to recover message after all retry attempts")
				}
			}
		} else {
			if logger := q.logIfEnabled(); logger != nil {
				logger.Warn().
					Str("queue_name", q.Name()).
					Msg("Queue does not support recovery, message will be lost")
			}
		}

		return err
	}

	return nil
}

func (q *SimpleQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Int("retry_count", msg.RetryCount()).
			Int("total_retry_count", msg.TotalRetryCount()).
			Time("created_at", msg.CreatedAt()).
			Msg("Recovering message")
	}

	metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

	if queue, ok := q.queue.(RecoverableQueue[T]); ok {
		err := queue.Recover(ctx, msg)
		if err != nil {
			metrics.MetricQueueRecoverErrorTotal.WithLabelValues(q.Name()).Inc()
			if logger := q.logIfEnabled(); logger != nil {
				logger.Error().
					Err(err).
					Str("queue_name", q.Name()).
					Int("retry_count", msg.RetryCount()).
					Int("total_retry_count", msg.TotalRetryCount()).
					Msg("Failed to recover message")
			}
			return err
		}
		metrics.MetricQueueRecoverSuccessulTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Debug().
				Str("queue_name", q.Name()).
				Int("retry_count", msg.RetryCount()).
				Int("total_retry_count", msg.TotalRetryCount()).
				Msg("Message recovered successfully")
		}
		return nil
	} else {
		if logger := q.logIfEnabled(); logger != nil {
			logger.Warn().
				Str("queue_name", q.Name()).
				Msg("Recover operation not supported by underlying queue")
		}
		return common.ErrNotImplemented
	}
}

func (q *SimpleQueue[T]) IsRecoverable() bool {
	_, ok := q.queue.(RecoverableQueue[T])
	return ok
}

func (q *SimpleQueue[T]) Purge(ctx context.Context) error {
	if logger := q.logIfEnabled(); logger != nil {
		logger.Debug().
			Str("queue_name", q.Name()).
			Msg("Purging queue")
	}

	metrics.MetricQueuePurgeTotal.WithLabelValues(q.Name()).Inc()

	purgeable, ok := q.queue.(Purgeable)
	if ok {
		err := purgeable.Purge(ctx)
		if err != nil {
			metrics.MetricQueuePurgeErrorTotal.WithLabelValues(q.Name()).Inc()
			if logger := q.logIfEnabled(); logger != nil {
				logger.Error().
					Err(err).
					Str("queue_name", q.Name()).
					Msg("Failed to purge queue")
			}
			return err
		}
		metrics.MetricQueuePurgeSuccessulTotal.WithLabelValues(q.Name()).Inc()
		if logger := q.logIfEnabled(); logger != nil {
			logger.Debug().
				Str("queue_name", q.Name()).
				Msg("Queue purged successfully")
		}
		return nil
	}

	if logger := q.logIfEnabled(); logger != nil {
		logger.Warn().
			Str("queue_name", q.Name()).
			Msg("Purge operation not supported by underlying queue")
	}
	return common.ErrNotImplemented
}

func (q *SimpleQueue[T]) IsPurgeable() bool {
	_, ok := q.queue.(Purgeable)
	return ok
}

func (q *SimpleQueue[T]) DLQ() (DLQ[T], error) {
	if dlqer, ok := q.queue.(DLQer[T]); ok {
		return dlqer.DLQ()
	}

	return nil, common.ErrNotImplemented
}

func (q *SimpleQueue[T]) IsDLQSupported() bool {
	_, ok := q.queue.(DLQer[T])
	return ok
}

// Stats returns the current queue statistics if the underlying queue supports it.
// Returns zero-valued QueueStats if not supported.
func (q *SimpleQueue[T]) Stats() QueueStats {
	if sp, ok := q.queue.(StatsProvider); ok {
		return sp.Stats()
	}
	return QueueStats{}
}

// IsStatsProvider returns true if the underlying queue implements StatsProvider.
func (q *SimpleQueue[T]) IsStatsProvider() bool {
	_, ok := q.queue.(StatsProvider)
	return ok
}

// initializeCapacityMetrics sets the capacity gauge metrics on queue creation.
func (q *SimpleQueue[T]) initializeCapacityMetrics() {
	if sp, ok := q.queue.(StatsProvider); ok {
		stats := sp.Stats()
		metrics.MetricQueueCapacity.WithLabelValues(q.Name(), "main").Set(float64(stats.Capacity))
		metrics.MetricQueueCapacity.WithLabelValues(q.Name(), "retry").Set(float64(stats.RetryCapacity))
	}
}

// initializeConfigInfoMetric sets the config info gauge metric on queue creation.
func (q *SimpleQueue[T]) initializeConfigInfoMetric() {
	maxSize := fmt.Sprintf("%d", q.MaxSize())
	maxHandleFailures := fmt.Sprintf("%d", q.MaxHandleFailures())
	consumerCount := "0"
	callbackParallel := "false"

	// Try to get config from the underlying queue
	if bq, ok := q.queue.(interface{ GetConfig() *Config }); ok {
		cfg := bq.GetConfig()
		if cfg != nil {
			consumerCount = fmt.Sprintf("%d", cfg.ConsumerCount)
			if cfg.CallbackParallelExecution {
				callbackParallel = "true"
			}
		}
	}

	metrics.MetricQueueConfigInfo.WithLabelValues(q.Name(), maxSize, maxHandleFailures, consumerCount, callbackParallel).Set(1)
}

// updateDepthMetrics updates the queue depth gauge metrics.
func (q *SimpleQueue[T]) updateDepthMetrics() {
	if sp, ok := q.queue.(StatsProvider); ok {
		stats := sp.Stats()
		metrics.MetricQueueDepth.WithLabelValues(q.Name(), "main").Set(float64(stats.Depth))
		metrics.MetricQueueDepth.WithLabelValues(q.Name(), "retry").Set(float64(stats.RetryDepth))
	}
}

// updateConsumersMetric updates the consumers active gauge metric.
func (q *SimpleQueue[T]) updateConsumersMetric() {
	if sp, ok := q.queue.(StatsProvider); ok {
		stats := sp.Stats()
		metrics.MetricQueueConsumersActive.WithLabelValues(q.Name()).Set(float64(stats.ConsumerCount))
	}
}
