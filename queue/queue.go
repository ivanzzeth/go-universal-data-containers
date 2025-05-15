package queue

import (
	"context"
	"errors"
)

const (
	UnlimitedSize = -1
)

type Handler func(msg Message) error

type Kind uint8

const (
	KindFIFO Kind = iota + 1
	KindStandard
)

// Errors
var (
	ErrQueueClosed    = errors.New("queue is closed")
	ErrQueueFull      = errors.New("queue is full")
	ErrQueueEmpty     = errors.New("queue is empty")
	ErrQueueRecovered = errors.New("queue recovered")
)

type Factory interface {
	// Create a new queue if name does not exist
	// If name already exists, return the existing queue
	GetOrCreate(name string, options ...Option) (Queue, error)

	// Same as GetOrCreate but returns SafeQueue
	GetOrCreateSafe(name string, options ...Option) (SafeQueue, error)
}

// The interface of queue
// The implementation of queue should be thread-safe
type Queue interface {
	Kind() Kind

	Name() string

	// Reports max size of queue
	// -1 for unlimited
	MaxSize() int

	// Reports max handle failures
	// Messages will be discarded after this many failures, or
	// pushed to DLQ if DLQ is supported
	MaxHandleFailures() int

	// Push data to end of queue
	// Failed if queue is full or closed
	Enqueue(context.Context, []byte) error

	// Pop data from beginning of queue without message confirmation
	// Failed if queue is empty

	// The implementation MUST set the retryCount of the message to 0 if its retryCount > MaxHandleFailures,
	// in the case, the message is from DLQ redriving.
	Dequeue(context.Context) (Message, error)

	// Subscribe queue with message confirmation.
	// Once handler returns error, it'll automatically put message back to queue using `Recover` mechanism internally.
	Subscribe(h Handler)

	Close()
}

type RecoverableQueue interface {
	Queue

	Recoverable
}

type Recoverable interface {
	// Recover providers the ability to put message back to queue when handler returns error or encounters panic.

	// If the queue supports `visibility window` like AWS SQS, the message will be put back to queue atomically without calling `Recover`.
	// It's useful if the panic is from outside of the queue handler.
	// But it's recommended to use `Recover` if the panic is from inside the queue handler for retrying the message fast.
	Recover(context.Context, Message) error
}

type Purgeable interface {
	// Clean up the queue
	Purge(context.Context) error
}

type DLQer interface {
	DLQ() (DLQ, error)
}

type DLQ interface {
	Queue

	// Push `items` of messages to associated Queue
	Redrive(ctx context.Context, items int) error

	AssociatedQueue() Queue
}
