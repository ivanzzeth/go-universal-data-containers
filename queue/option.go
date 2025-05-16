package queue

import (
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/ivanzzeth/go-universal-data-containers/message"
)

var (
	DefaultPollInterval = 10 * time.Millisecond
	DefaultMaxRetries   = 10
	DefaultOptions      = Config{
		LockerGenerator: locker.NewMemoryLockerGenerator(),

		MaxSize: UnlimitedSize,

		MaxHandleFailures: 10,

		PollInterval: DefaultPollInterval,
		MaxRetries:   DefaultMaxRetries,

		ConsumerCount: 1,

		MessageIDGenerator: message.GenerateRandomID,
	}
)

type Option func(*Config)

// Configuarable options here, but some implementations of queue may not support all options
type Config struct {
	LockerGenerator locker.SyncLockerGenerator

	MaxSize int

	// Messages will be discarded after this many failures, or
	// pushed to DLQ if DLQ is supported
	MaxHandleFailures int

	PollInterval time.Duration

	// Used for internal retrying, not for message retrying
	MaxRetries int

	// Specify how many consumers are consuming the queue using `Subscribe`.
	// Be aware that too many consumers can cause order of messages to be changed.
	// If you want to ensure the order of messages, please use FIFO queue and set ConsumerCount to 1
	ConsumerCount int

	MessageIDGenerator message.MessageIDGenerator
}

func WithMaxSize(maxSize int) func(*Config) {
	return func(o *Config) {
		o.MaxSize = maxSize
	}
}

func WithMaxHandleFailures(maxHandleFailures int) func(*Config) {
	return func(o *Config) {
		o.MaxHandleFailures = maxHandleFailures
	}
}

func WithPollInterval(pollInterval time.Duration) func(*Config) {
	return func(o *Config) {
		o.PollInterval = pollInterval
	}
}

func WithMaxRetries(maxRetries int) func(*Config) {
	return func(o *Config) {
		o.MaxRetries = maxRetries
	}
}

func WithConsumerCount(consumerCount int) func(*Config) {
	return func(o *Config) {
		o.ConsumerCount = consumerCount
	}
}

func WithMessageIDGenerator(generator message.MessageIDGenerator) func(*Config) {
	return func(o *Config) {
		o.MessageIDGenerator = generator
	}
}
