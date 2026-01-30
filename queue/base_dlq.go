package queue

import (
	"context"
	"errors"

	"github.com/ivanzzeth/go-universal-data-containers/metrics"
)

var (
	_ DLQ[any] = (*BaseDLQ[any])(nil)
)

type BaseDLQ[T any] struct {
	associatedQueue Queue[T]
	Queue[T]
}

func newBaseDLQ[T any](q, dlq Queue[T]) (*BaseDLQ[T], error) {
	return &BaseDLQ[T]{
		associatedQueue: q,
		Queue:           dlq,
	}, nil
}

func (q *BaseDLQ[T]) Redrive(ctx context.Context, items int) error {
	associatedQueueName := q.associatedQueue.Name()
	metrics.MetricQueueRedriveTotal.WithLabelValues(associatedQueueName).Inc()

	for i := 0; i < items; i++ {
		msg, err := q.Dequeue(ctx)
		if err != nil {
			if !errors.Is(err, ErrQueueEmpty) {
				metrics.MetricQueueRedriveErrorTotal.WithLabelValues(associatedQueueName).Inc()
				return err
			}

			// No more messages to redrive - this is success
			metrics.MetricQueueRedriveSuccessfulTotal.WithLabelValues(associatedQueueName).Inc()
			return nil
		}

		// Try to enqueue to retry queue for priority processing
		// If the associated queue supports RetryQueueEnqueuer, use it
		if retryEnqueuer, ok := q.associatedQueue.(RetryQueueEnqueuer[T]); ok {
			err = retryEnqueuer.EnqueueToRetryQueue(ctx, msg.Data())
		} else {
			// Fallback to normal enqueue
			err = q.associatedQueue.Enqueue(ctx, msg.Data())
		}

		if err != nil {
			metrics.MetricQueueRedriveErrorTotal.WithLabelValues(associatedQueueName).Inc()
			return err
		}
	}

	metrics.MetricQueueRedriveSuccessfulTotal.WithLabelValues(associatedQueueName).Inc()
	return nil
}

func (q *BaseDLQ[T]) AssociatedQueue() Queue[T] {
	return q.associatedQueue
}
