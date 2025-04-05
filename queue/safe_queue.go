package queue

import (
	"math"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/metrics"
)

// SafeQueue provides ability to put message back to queue when handler encounters panic
var (
	_ Queue            = &SafeQueue{}
	_ RecoverableQueue = &SafeQueue{}
)

type SafeQueue struct {
	queue Queue
}

func NewSafeQueue(queue Queue) *SafeQueue {
	return &SafeQueue{
		queue: queue,
	}
}

func (q *SafeQueue) Kind() Kind {
	return q.queue.Kind()
}

func (q *SafeQueue) Name() string {
	return q.queue.Name()
}

func (q *SafeQueue) MaxSize() int {
	return q.queue.MaxSize()
}

func (q *SafeQueue) Enqueue(data []byte) error {
	metrics.MetricQueueEnqueueTotal.WithLabelValues(q.Name()).Inc()
	return q.queue.Enqueue(data)
}

func (q *SafeQueue) Dequeue() ([]byte, error) {
	metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
	return q.queue.Dequeue()
}

func (q *SafeQueue) Subscribe(cb Handler) {
	q.queue.Subscribe(func(b []byte) error {
		startTime := time.Now()
		defer func() {
			metrics.MetricQueueHandleDuration.WithLabelValues(q.Name()).Observe(time.Since(startTime).Seconds())
		}()

		err := q.handle(b, cb)
		if err != nil {
			metrics.MetricQueueHandleErrorTotal.WithLabelValues(q.Name()).Inc()
			return err
		}

		metrics.MetricQueueDequeueTotal.WithLabelValues(q.Name()).Inc()
		metrics.MetricQueueHandleSuccessulTotal.WithLabelValues(q.Name()).Inc()
		return nil
	})
}

func (q *SafeQueue) handle(b []byte, cb Handler) error {
	defer func() {
		if r := recover(); r != nil {
			// log.Error("Recovered from subscribe queue panic", "err", r)
			metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

			if queue, ok := q.queue.(RecoverableQueue); ok {
				var recoverErr error
				for i := 0; i < 10; i++ {
					recoverErr = queue.Recover(b)
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

	err := cb(b)
	if err != nil {
		if q.IsRecoverable() {
			var recoverErr error
			for i := 0; i < 10; i++ {
				recoverErr = q.Recover(b)
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

func (q *SafeQueue) Recover(b []byte) error {
	// log.Debug("SafeQueue recover", "data", fmt.Sprintf("0x%x", b))
	metrics.MetricQueueRecoverTotal.WithLabelValues(q.Name()).Inc()

	if queue, ok := q.queue.(RecoverableQueue); ok {
		return queue.Recover(b)
	} else {
		panic("Underlying queue of SafeQueue is not recoverable")
	}
}

func (q *SafeQueue) IsRecoverable() bool {
	_, ok := q.queue.(RecoverableQueue)
	return ok
}
