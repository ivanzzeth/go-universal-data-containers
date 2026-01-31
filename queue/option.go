package queue

import (
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/ivanzzeth/go-universal-data-containers/message"
)

var (
	DefaultPollInterval       = 10 * time.Millisecond
	DefaultMaxRetries         = 10
	DefaultUnlimitedCapacity  = 1000000
	DefaultRetryQueueCapacity = 10000

	// DefaultCallbackWaitInterval is the interval for checking if callbacks are registered.
	// Used by both MemoryQueue and RedisQueue in their run() loops.
	DefaultCallbackWaitInterval = 10 * time.Millisecond

	// DefaultBlockingTimeout is the timeout for blocking operations like BLPOP.
	// Used by RedisQueue for BDequeue and run() loops.
	DefaultBlockingTimeout = time.Second

	// DefaultNetworkRetryDelay is the delay before retrying after a network error.
	// Used by RedisQueue when encountering transient errors.
	DefaultNetworkRetryDelay = 100 * time.Millisecond
	DefaultOptions            = Config{
		LockerGenerator: locker.NewMemoryLockerGenerator(),

		MaxSize: UnlimitedSize,

		MaxHandleFailures: 10,

		// PollInterval is used by RedisQueue for BEnqueue polling when queue is full.
		// MemoryQueue uses channel-based blocking and does not use this for normal operations.
		PollInterval: DefaultPollInterval,
		MaxRetries:   DefaultMaxRetries,

		ConsumerCount: 1,

		CallbackParallelExecution: false,
		CallbackTimeout:           0,

		MessageIDGenerator: message.GenerateRandomID,

		// UnlimitedCapacity is the channel buffer size when MaxSize is UnlimitedSize.
		// Only applies to MemoryQueue.
		UnlimitedCapacity: DefaultUnlimitedCapacity,

		// RetryQueueCapacity is the buffer size for retry queue (used by Recover).
		RetryQueueCapacity: DefaultRetryQueueCapacity,
	}
)

type Option func(*Config)

// Configurable options here, but some implementations of queue may not support all options
type Config struct {
	LockerGenerator locker.SyncLockerGenerator

	MaxSize int

	// Messages will be discarded after this many failures, or
	// pushed to DLQ if DLQ is supported
	MaxHandleFailures int

	// PollInterval is used by RedisQueue for BEnqueue polling when queue is full.
	// MemoryQueue uses channel-based event-driven model and does not rely on polling.
	PollInterval time.Duration

	// Used for internal retrying, not for message retrying
	MaxRetries int

	// Specify how many consumers are consuming the queue using `Subscribe`.
	// Be aware that too many consumers can cause order of messages to be changed.
	// If you want to ensure the order of messages, please use FIFO queue and set ConsumerCount to 1
	ConsumerCount int

	// Enable parallel execution of callbacks. When true, all callbacks for a message
	// will be executed concurrently in separate goroutines, improving throughput for
	// slow handlers. When false (default), callbacks are executed sequentially.
	CallbackParallelExecution bool

	// Timeout for each callback execution. If set to 0 (default), no timeout is applied.
	// When a callback exceeds this timeout, it will be cancelled and an error will be returned.
	CallbackTimeout time.Duration

	MessageIDGenerator message.MessageIDGenerator

	// UnlimitedCapacity is the channel buffer size when MaxSize is UnlimitedSize.
	// Only applies to MemoryQueue. Default is 1000000.
	UnlimitedCapacity int

	// RetryQueueCapacity is the buffer size for retry queue (used by Recover).
	// Messages in retry queue are processed with higher priority than normal queue.
	// Default is 10000.
	RetryQueueCapacity int
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

func WithCallbackParallelExecution(enable bool) func(*Config) {
	return func(o *Config) {
		o.CallbackParallelExecution = enable
	}
}

func WithCallbackTimeout(timeout time.Duration) func(*Config) {
	return func(o *Config) {
		o.CallbackTimeout = timeout
	}
}

func WithUnlimitedCapacity(capacity int) func(*Config) {
	return func(o *Config) {
		o.UnlimitedCapacity = capacity
	}
}

func WithRetryQueueCapacity(capacity int) func(*Config) {
	return func(o *Config) {
		o.RetryQueueCapacity = capacity
	}
}
