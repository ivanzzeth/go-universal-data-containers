package queue

import (
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/message"
)

var (
	DefaultPollInterval = 10 * time.Millisecond
	DefaultMaxRetries   = 10
	DefaultOptions      = Config{
		MaxSize: UnlimitedMaxSize,

		MaxHandleFailures: 10,

		PollInterval: DefaultPollInterval,
		MaxRetries:   DefaultMaxRetries,

		ConsumerCount: 1,

		Message: &JsonMessage{},

		MessageIDGenerator: message.GenerateRandomID,
	}
)

type Option func(*Config)

// Configuarable options here, but some implementations of queue may not support all options
type Config struct {
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

	// Specify standard message
	// Use json message if nil
	Message            Message
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

func WithMessage(message Message) func(*Config) {
	return func(o *Config) {
		o.Message = message
	}
}

func WithMessageIDGenerator(generator message.MessageIDGenerator) func(*Config) {
	return func(o *Config) {
		o.MessageIDGenerator = generator
	}
}
