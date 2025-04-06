package queue

import "time"

type Option func(*Config)

// Configuarable options here, but some implementations of queue may not support all options
type Config struct {
	MaxSize      int
	PollInterval time.Duration
	MaxRetries   int

	// Specify how many consumers are consuming the queue using `Subscribe`.
	// Be aware that too many consumers can cause order of messages to be changed.
	// If you want to ensure the order of messages, please use FIFO queue and set ConsumerCount to 1
	ConsumerCount int
}

func WithMaxSize(maxSize int) func(*Config) {
	return func(o *Config) {
		o.MaxSize = maxSize
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
