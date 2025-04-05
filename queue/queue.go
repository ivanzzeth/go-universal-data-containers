package queue

import (
	"errors"
	"time"
)

var (
	DefaultPollInterval = 20 * time.Millisecond
	DefaultMaxRetries   = 10
)

type Handler func([]byte) error

type Kind uint8

const (
	KindFIFO Kind = iota + 1
	KindStandard
)

// Errors
var (
	ErrQueueClosed = errors.New("queue is closed")
	ErrQueueFull   = errors.New("queue is full")
	ErrQueueEmpty  = errors.New("queue is empty")
)

type Factory interface {
	// Create a new queue if name does not exist
	// If name already exists, return the existing queue
	GetOrCreate(name string, maxSize int) Queue
}

// The interface of queue
// The implementation of queue should be thread-safe
type Queue interface {
	Kind() Kind

	Name() string

	// Reports max size of queue
	MaxSize() int

	// Push data to end of queue
	// Failed if queue is full or closed
	Enqueue([]byte) error

	// Pop data from beginning of queue without message confirmation
	// Failed if queue is empty
	Dequeue() ([]byte, error)

	// Subscribe queue with message confirmation.
	// Once handler returns error, it'll automatically put message back to queue using `Recover` mechanism internally.
	Subscribe(h Handler)
}

type RecoverableQueue interface {
	Queue

	// Recover providers the ability to put message back to queue when handler returns error or encounters panic.
	// Message will be located at:
	// 1. the end of queue if queue is standard queue or
	// 2. the beginning of queue if queue is fifo queue

	// If the queue supports `visibility window` like AWS SQS, the message will be put back to queue atomically without calling `Recover`.
	// Then, no need to really put the message back to queue using `Recover`, just implement it like below:

	// func Recover(data []byte) error { return nil }

	// or just does not implement it
	Recover([]byte) error
}
