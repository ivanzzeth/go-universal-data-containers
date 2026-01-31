package pubsub

import (
	"context"

	"github.com/ivanzzeth/go-universal-data-containers/message"
)

// Publisher publishes messages to topics
type Publisher interface {
	// Publish sends a message to all subscribers of the topic.
	// Returns nil if there are no subscribers (message is silently dropped).
	Publish(ctx context.Context, topic string, msg message.Message[any]) error

	// PublishBatch sends multiple messages to all subscribers of the topic.
	// All messages are sent atomically if the backend supports it.
	PublishBatch(ctx context.Context, topic string, msgs []message.Message[any]) error
}

// Subscriber subscribes to topics and receives messages
type Subscriber interface {
	// Subscribe returns a channel that receives all messages published to the topic.
	// The channel is closed when:
	// - The context is cancelled
	// - Unsubscribe is called on the returned Subscription
	// - The PubSub instance is closed
	//
	// Each call to Subscribe creates a new independent subscription.
	// All subscriptions receive all messages (fan-out).
	Subscribe(ctx context.Context, topic string) (Subscription, error)

	// SubscribeWithHandler registers a handler for the topic.
	// The handler is called for each message received.
	// This is a blocking call that returns when:
	// - The context is cancelled
	// - The PubSub instance is closed
	// - An unrecoverable error occurs
	//
	// Handler errors are logged but do not stop the subscription.
	SubscribeWithHandler(ctx context.Context, topic string, handler Handler) error
}

// Handler processes messages from a subscription
type Handler func(ctx context.Context, msg message.Message[any]) error

// Subscription represents an active subscription to a topic
type Subscription interface {
	// ID returns the unique identifier of this subscription
	ID() string

	// Topic returns the topic this subscription is listening to
	Topic() string

	// Messages returns the channel that receives messages
	// The channel is closed when the subscription ends
	Messages() <-chan message.Message[any]

	// Unsubscribe cancels this subscription and closes the message channel
	Unsubscribe() error
}

// PubSub combines Publisher and Subscriber interfaces
type PubSub interface {
	Publisher
	Subscriber

	// Topics returns a list of all topics with active subscriptions
	Topics() []string

	// SubscriberCount returns the number of active subscribers for a topic
	SubscriberCount(topic string) int

	// Close gracefully shuts down the PubSub instance.
	// All subscriptions are closed and no more messages can be published.
	Close() error
}

// TypedPublisher provides type-safe publishing
type TypedPublisher[T any] interface {
	// Publish sends a typed message to all subscribers
	Publish(ctx context.Context, topic string, data T) error

	// PublishBatch sends multiple typed messages
	PublishBatch(ctx context.Context, topic string, data []T) error
}

// TypedSubscriber provides type-safe subscription
type TypedSubscriber[T any] interface {
	// Subscribe returns a channel that receives typed messages
	Subscribe(ctx context.Context, topic string) (TypedSubscription[T], error)

	// SubscribeWithHandler registers a typed handler
	SubscribeWithHandler(ctx context.Context, topic string, handler TypedHandler[T]) error
}

// TypedHandler processes typed messages
type TypedHandler[T any] func(ctx context.Context, msg message.Message[T]) error

// TypedSubscription provides type-safe message access
type TypedSubscription[T any] interface {
	ID() string
	Topic() string
	Messages() <-chan message.Message[T]
	Unsubscribe() error
}

// TypedPubSub combines typed Publisher and Subscriber
type TypedPubSub[T any] interface {
	TypedPublisher[T]
	TypedSubscriber[T]
	Topics() []string
	SubscriberCount(topic string) int
	Close() error
}
