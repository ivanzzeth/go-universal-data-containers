package queue

import (
	"context"
	"math"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/metrics"
)

var (
	_ Queue[any]     = &SimpleQueue[any]{}
	_ SafeQueue[any] = &SimpleQueue[any]{}
)

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
	queue Queue[T]
}

func NewSimpleQueue[T any](queue Queue[T]) (*SimpleQueue[T], error) {
	return &SimpleQueue[T]{
		queue: queue,
	}, nil
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
	q.queue.Close()
}

func (q *SimpleQueue[T]) MaxSize() int {
	return q.queue.MaxSize()
}

func (q *SimpleQueue[T]) MaxHandleFailures() int {
	return q.queue.MaxHandleFailures()
}

func (q *SimpleQueue[T]) Enqueue(ctx context.Context, data T) error {
	metrics.MetricQueueEnqueueTotal.WithLabelValues(q.Name()).Inc()
	err := q.queue.Enqueue(ctx, data)
	if err != nil {
		metrics.MetricQueueEnqueueErrorTotal.WithLabelValues(q.Name()).Inc()
		return err
	}

	return nil
}

func (q *SimpleQueue[T]) Dequeue(ctx context.Context) (Message[T], error) {
	metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
	data, err := q.queue.Dequeue(ctx)
	if err != nil {
		metrics.MetricQueueDequeueErrorTotal.WithLabelValues(q.Name()).Inc()
		return nil, err
	}

	return data, nil
}

func (q *SimpleQueue[T]) BDequeue(ctx context.Context) (Message[T], error) {
	metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
	data, err := q.queue.BDequeue(ctx)
	if err != nil {
		metrics.MetricQueueDequeueErrorTotal.WithLabelValues(q.Name()).Inc()
		return nil, err
	}

	return data, nil
}

func (q *SimpleQueue[T]) Subscribe(cb Handler[T]) {
	q.queue.Subscribe(func(msg Message[T]) error {
		startTime := time.Now()
		defer func() {
			metrics.MetricQueueHandleDuration.WithLabelValues(q.Name()).Observe(time.Since(startTime).Seconds())
		}()

		err := q.handle(msg, cb)
		if err != nil {
			metrics.MetricQueueHandleErrorTotal.WithLabelValues(q.Name()).Inc()
			return err
		}

		metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
		metrics.MetricQueueHandleSuccessulTotal.WithLabelValues(q.Name()).Inc()
		return nil
	})
}

func (q *SimpleQueue[T]) handle(msg Message[T], cb Handler[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrQueueRecovered
			// log.Error("Recovered from subscribe queue panic", "err", r)
			metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

			if queue, ok := q.queue.(RecoverableQueue[T]); ok {
				var recoverErr error
				for i := 0; i < DefaultMaxRetries; i++ {
					// fmt.Printf("Recover %v\n", b)

					recoverErr = queue.Recover(context.TODO(), msg)
					if recoverErr != nil {
						time.Sleep(time.Duration(math.Pow(2, float64(i))) * 10 * time.Millisecond)
						continue
					}

					break
				}

				if recoverErr != nil {
					metrics.MetricQueueRecoverErrorTotal.WithLabelValues(q.Name()).Inc()
				}
			}
		}
	}()

	err = cb(msg)
	if err != nil {
		if q.IsRecoverable() {
			var recoverErr error
			for i := 0; i < DefaultMaxRetries; i++ {
				// fmt.Printf("Recover2 %v\n", b)
				recoverErr = q.Recover(context.TODO(), msg)
				if recoverErr != nil {
					time.Sleep(time.Duration(math.Pow(2, float64(i))) * 10 * time.Millisecond)
					continue
				}

				break
			}

			if recoverErr != nil {
				metrics.MetricQueueRecoverErrorTotal.WithLabelValues(q.Name()).Inc()
			}
		}

		return err
	}

	return nil
}

func (q *SimpleQueue[T]) Recover(ctx context.Context, msg Message[T]) error {
	// log.Debug("SafeQueue recover", "data", fmt.Sprintf("0x%x", b))
	metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

	if queue, ok := q.queue.(RecoverableQueue[T]); ok {
		err := queue.Recover(ctx, msg)
		if err != nil {
			metrics.MetricQueueRecoverErrorTotal.WithLabelValues(q.Name()).Inc()
			return err
		}
		metrics.MetricQueueRecoverSuccessulTotal.WithLabelValues(q.Name()).Inc()
		return nil
	} else {
		return common.ErrNotImplemented
	}
}

func (q *SimpleQueue[T]) IsRecoverable() bool {
	_, ok := q.queue.(RecoverableQueue[T])
	return ok
}

func (q *SimpleQueue[T]) Purge(ctx context.Context) error {
	metrics.MetricQueuePurgeTotal.WithLabelValues(q.Name()).Inc()

	purgeable, ok := q.queue.(Purgeable)
	if ok {
		err := purgeable.Purge(ctx)
		if err != nil {
			metrics.MetricQueuePurgeErrorTotal.WithLabelValues(q.Name()).Inc()
			return err
		}
		metrics.MetricQueuePurgeSuccessulTotal.WithLabelValues(q.Name()).Inc()
		return nil
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
