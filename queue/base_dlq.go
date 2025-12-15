package queue

import (
	"context"
	"errors"
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
	for i := 0; i < items; i++ {
		msg, err := q.Dequeue(ctx)
		if err != nil {
			if !errors.Is(err, ErrQueueEmpty) {
				return err
			}

			return nil
		}

		err = q.associatedQueue.Enqueue(ctx, msg.Data())
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *BaseDLQ[T]) AssociatedQueue() Queue[T] {
	return q.associatedQueue
}
