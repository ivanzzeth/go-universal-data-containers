package queue

import (
	"math"
	"time"
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

func (q *SafeQueue) MaxSize() int {
	return q.queue.MaxSize()
}

func (q *SafeQueue) Enqueue(data []byte) error {
	return q.queue.Enqueue(data)
}

func (q *SafeQueue) Dequeue() ([]byte, error) {
	return q.queue.Dequeue()
}

func (q *SafeQueue) Subscribe(cb Handler) {
	q.queue.Subscribe(func(b []byte) error {
		return q.handle(b, cb)
	})
}

func (q *SafeQueue) handle(b []byte, cb Handler) error {
	defer func() {
		if r := recover(); r != nil {
			// log.Error("Recovered from subscribe queue panic", "err", r)
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
					// log.Error("Failed to recover from subscribe queue panic", "err", recoverErr, "data", fmt.Sprintf("0x%x", b))
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
				// log.Error("Failed to recover from subscribe queue panic", "err", recoverErr, "data", fmt.Sprintf("0x%x", b))
			}
		}

		return err
	}

	return nil
}

func (q *SafeQueue) Recover(b []byte) error {
	// log.Debug("SafeQueue recover", "data", fmt.Sprintf("0x%x", b))

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
