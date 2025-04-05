package queue

import (
	"fmt"
	"math"
	"time"

	"github.com/adjust/rmq/v5"
)

var (
	_ Queue            = (*RedisQueue)(nil)
	_ RecoverableQueue = (*RedisQueue)(nil)
)

type RedisQueue struct {
	*BaseQueue
	q rmq.Queue
}

func NewRedisQueue(conn rmq.Connection, name string, options *QueueOptions) (*RedisQueue, error) {
	queue, err := conn.OpenQueue(name)
	if err != nil {
		return nil, err
	}

	err = queue.StartConsuming(1, 10*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return &RedisQueue{
		BaseQueue: NewBaseQueue(name, options),
		q:         queue,
	}, nil
}

func (q *RedisQueue) MaxSize() int {
	return UnlimitedMaxSize
}

func (q *RedisQueue) Enqueue(data []byte) error {
	err := q.BaseQueue.Enqueue(data)
	if err != nil {
		return err
	}

	return q.q.PublishBytes(data)
}

func (q *RedisQueue) Dequeue() ([]byte, error) {
	data, err := q.q.Drain(1)
	if err != nil {
		return nil, err
	}

	if len(data) != 1 {
		return nil, fmt.Errorf("expected 1 message, got %d", len(data))
	}

	return []byte(data[0]), nil
}

func (q *RedisQueue) Subscribe(cb Handler) {
	if q.cb == nil {
		q.cb = cb

		q.q.AddConsumerFunc("", func(delivery rmq.Delivery) {
			var err error

			defer func() {
				if err != nil {
					for i := 0; i < q.options.MaxRetries; i++ {
						if rejectErr := delivery.Reject(); rejectErr == nil {
							break
						} else {
							// TODO: logging
							time.Sleep(time.Duration(math.Pow(2, float64(i))) * 10 * time.Millisecond)
						}
					}
				}
			}()

			err = cb([]byte(delivery.Payload()))
			if err != nil {
				return
			}

			err = delivery.Ack()
			if err != nil {
				return
			}
		})
	}
}

func (q *RedisQueue) Recover(b []byte) error {
	return nil
}
