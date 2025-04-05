package queue

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/adjust/rmq/v5"
	"github.com/redis/go-redis/v9"
)

var (
	_ Queue            = (*RedisQueue)(nil)
	_ RecoverableQueue = (*RedisQueue)(nil)
	_ Factory          = (*RedisQueueFactory)(nil)
)

const (
	redisQueueTestMsg = "test-message-facgasdffadspoiubsf"
)

type RedisQueueFactory struct {
	rmqConn rmq.Connection
}

func NewRedisQueueFactory(redisClient redis.Cmdable) *RedisQueueFactory {
	errChan := make(chan error, 100)
	go func() {
		for err := range errChan {
			if err != nil {
				// TODO: logging
				// fmt.Printf("redis queue got err: %v\n", err)
			}
		}
	}()

	rmqConn, err := rmq.OpenConnectionWithRedisClient("rmq", redisClient, errChan)
	if err != nil {
		panic(fmt.Errorf("failed to open rmq connection: %v", err))
	}

	return &RedisQueueFactory{
		rmqConn: rmqConn,
	}
}

func (f *RedisQueueFactory) GetOrCreate(name string, options ...func(*QueueOptions)) (Queue, error) {
	ops := DefaultOptions
	for _, op := range options {
		op(&ops)
	}

	q, err := NewRedisQueue(f.rmqConn, name, &ops)
	if err != nil {
		return nil, err
	}

	return NewSafeQueue(q)
}

type RedisQueue struct {
	*BaseQueue
	q rmq.Queue
}

func NewRedisQueue(conn rmq.Connection, name string, options *QueueOptions) (*RedisQueue, error) {
	baseQueue := NewBaseQueue(name, options)
	queue, err := conn.OpenQueue(name)
	if err != nil {
		return nil, err
	}

	err = queue.StartConsuming(1, options.PollInterval)
	if err != nil {
		return nil, err
	}

	// FIXME: First message will be lost, then publish a test message to avoid this.
	err = queue.PublishBytes([]byte(redisQueueTestMsg))
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			time.Sleep(options.PollInterval)
			select {
			case <-baseQueue.exitChannel:
				return
			default:
			}
			// unackedReturned, err := queue.ReturnUnacked(10)
			// if err != nil {
			// 	// TODO: logging
			// 	fmt.Printf("redis queue got err: %v\n", err)
			// 	continue
			// }

			// if unackedReturned > 0 {
			// 	// TODO: logging
			// 	fmt.Printf("redis queue returned %d unacked messages\n", unackedReturned)
			// }

			rejectedReturned, err := queue.ReturnRejected(10)
			if err != nil {
				// TODO: logging
				if !errors.Is(err, rmq.ErrorNotFound) {
					// fmt.Printf("redis queue got err: %v\n", err)

					continue
				}
			}

			if rejectedReturned > 0 {
				// TODO: logging
				// fmt.Printf("redis queue returned %d rejected messages\n", rejectedReturned)
			}
		}
	}()

	return &RedisQueue{
		BaseQueue: baseQueue,
		q:         queue,
	}, nil
}

func (q *RedisQueue) MaxSize() int {
	return UnlimitedMaxSize
}

func (q *RedisQueue) Enqueue(data []byte) error {
	// fmt.Printf("Enqueue %v\n", data)

	err := q.BaseQueue.Enqueue(data)
	if err != nil {
		return err
	}

	return q.q.PublishBytes(data)
}

func (q *RedisQueue) Dequeue() ([]byte, error) {
	data, err := q.q.Drain(1)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrQueueEmpty
		}

		return nil, err
	}

	bytes := [][]byte{}
	for _, d := range data {
		bytes = append(bytes, []byte(d))
	}
	// fmt.Printf("Dequeue %v\n", bytes)

	if len(data) != 1 {
		return nil, fmt.Errorf("expected 1 message, got %d", len(data))
	}

	if data[0] == redisQueueTestMsg {
		return q.Dequeue()
	}

	return []byte(data[0]), nil
}

func (q *RedisQueue) Subscribe(cb Handler) {
	if q.cb == nil {
		q.cb = cb

		q.q.AddConsumerFunc("", func(delivery rmq.Delivery) {
			var err error
			data := delivery.Payload()

			defer func() {
				if err != nil {
					// fmt.Printf("Reject %v reason %v\n", data, err)

					for i := 0; i < q.options.MaxRetries; i++ {
						if rejectErr := delivery.Reject(); rejectErr == nil {
							break
						} else {
							// TODO: logging
							fmt.Printf("failed to reject delivery: %v\n", rejectErr)
							time.Sleep(time.Duration(math.Pow(2, float64(i))) * 10 * time.Millisecond)
						}
					}
				}
			}()

			// fmt.Printf("Subscribe %v\n", data)
			if data != redisQueueTestMsg {
				// fmt.Printf("Handle %v\n", data)
				err = cb([]byte(data))
				if err != nil {
					// fmt.Printf("Handle failed %v: %v\n", data, err)
					return
				}
			}

			// fmt.Printf("Ack %v\n", data)

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
