package pubsub

import "errors"

var (
	// ErrPubSubClosed is returned when operating on a closed PubSub instance
	ErrPubSubClosed = errors.New("pubsub: closed")

	// ErrUnknownBackend is returned when the specified backend is not registered
	ErrUnknownBackend = errors.New("pubsub: unknown backend")

	// ErrInvalidConfig is returned when the configuration is invalid
	ErrInvalidConfig = errors.New("pubsub: invalid config")

	// ErrSubscriptionClosed is returned when operating on a closed subscription
	ErrSubscriptionClosed = errors.New("pubsub: subscription closed")

	// ErrTopicEmpty is returned when topic is empty
	ErrTopicEmpty = errors.New("pubsub: topic cannot be empty")

	// ErrNilMessage is returned when message is nil
	ErrNilMessage = errors.New("pubsub: message cannot be nil")

	// ErrNilHandler is returned when handler is nil
	ErrNilHandler = errors.New("pubsub: handler cannot be nil")
)
