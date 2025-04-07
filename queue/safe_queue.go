package queue

import (
	"math"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/metrics"
)

var (
	_ Queue     = &SimpleQueue{}
	_ SafeQueue = &SimpleQueue{}
)

// SafeQueue provides ability to put message back to queue when handler encounters panic
// and makes sure all function calls are safe.
// e.g, Returns ErrNotImplemented if calling Recover and it is not implemented
type SafeQueue interface {
	Queue

	Recoverable
	IsRecoverable() bool

	Purgeable
	IsPurgeable() bool

	DLQer
	IsDLQSupported() bool
}

type SimpleQueue struct {
	queue Queue
}

func NewSimpleQueue(queue Queue) (*SimpleQueue, error) {
	return &SimpleQueue{
		queue: queue,
	}, nil
}

func (q *SimpleQueue) Unwrap() Queue {
	return q.queue
}

func (q *SimpleQueue) Kind() Kind {
	return q.queue.Kind()
}

func (q *SimpleQueue) Name() string {
	return q.queue.Name()
}

func (q *SimpleQueue) Close() {
	q.queue.Close()
}

func (q *SimpleQueue) MaxSize() int {
	return q.queue.MaxSize()
}

func (q *SimpleQueue) MaxHandleFailures() int {
	return q.queue.MaxHandleFailures()
}

func (q *SimpleQueue) Enqueue(data []byte) error {
	metrics.MetricQueueEnqueueTotal.WithLabelValues(q.Name()).Inc()
	err := q.queue.Enqueue(data)
	if err != nil {
		metrics.MetricQueueEnqueueErrorTotal.WithLabelValues(q.Name()).Inc()
		return err
	}

	return nil
}

func (q *SimpleQueue) Dequeue() (Message, error) {
	metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
	data, err := q.queue.Dequeue()
	if err != nil {
		metrics.MetricQueueDequeueErrorTotal.WithLabelValues(q.Name()).Inc()
		return nil, err
	}

	return data, nil
}

func (q *SimpleQueue) Subscribe(cb Handler) {
	q.queue.Subscribe(func(msg Message) error {
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

func (q *SimpleQueue) handle(msg Message, cb Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrQueueRecovered
			// log.Error("Recovered from subscribe queue panic", "err", r)
			metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

			if queue, ok := q.queue.(RecoverableQueue); ok {
				var recoverErr error
				for i := 0; i < DefaultMaxRetries; i++ {
					// fmt.Printf("Recover %v\n", b)

					recoverErr = queue.Recover(msg)
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
				recoverErr = q.Recover(msg)
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

func (q *SimpleQueue) Recover(msg Message) error {
	// log.Debug("SafeQueue recover", "data", fmt.Sprintf("0x%x", b))
	metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

	if queue, ok := q.queue.(RecoverableQueue); ok {
		err := queue.Recover(msg)
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

func (q *SimpleQueue) IsRecoverable() bool {
	_, ok := q.queue.(RecoverableQueue)
	return ok
}

func (q *SimpleQueue) Purge() error {
	metrics.MetricQueuePurgeTotal.WithLabelValues(q.Name()).Inc()

	purgeable, ok := q.queue.(Purgeable)
	if ok {
		err := purgeable.Purge()
		if err != nil {
			metrics.MetricQueuePurgeErrorTotal.WithLabelValues(q.Name()).Inc()
			return err
		}
		metrics.MetricQueuePurgeSuccessulTotal.WithLabelValues(q.Name()).Inc()
		return nil
	}

	return common.ErrNotImplemented
}

func (q *SimpleQueue) IsPurgeable() bool {
	_, ok := q.queue.(Purgeable)
	return ok
}

func (q *SimpleQueue) DLQ() (DLQ, error) {
	if dlqer, ok := q.queue.(DLQer); ok {
		return dlqer.DLQ()
	}

	return nil, common.ErrNotImplemented
}

func (q *SimpleQueue) IsDLQSupported() bool {
	_, ok := q.queue.(DLQer)
	return ok
}
