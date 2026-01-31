package pubsub

import (
	"context"

	"github.com/ivanzzeth/go-universal-data-containers/message"
)

// sendResult represents the result of a send operation
type sendResult struct {
	sent   bool
	ctxErr error
}

// sendWithOverflow sends a message to a channel with overflow handling.
// It handles both OverflowDrop (non-blocking) and OverflowBlock (blocking) policies.
// Returns whether the message was sent and any context error.
func sendWithOverflow(
	ctx context.Context,
	policy OverflowPolicy,
	ch chan<- message.Message[any],
	done <-chan struct{},
	msg message.Message[any],
	onDrop func(),
) sendResult {
	switch policy {
	case OverflowDrop:
		select {
		case ch <- msg:
			return sendResult{sent: true}
		case <-done:
			return sendResult{}
		case <-ctx.Done():
			return sendResult{ctxErr: ctx.Err()}
		default:
			// channel full, drop message
			if onDrop != nil {
				onDrop()
			}
			return sendResult{sent: true} // dropped but considered "handled"
		}

	case OverflowBlock:
		select {
		case ch <- msg:
			return sendResult{sent: true}
		case <-done:
			return sendResult{}
		case <-ctx.Done():
			return sendResult{ctxErr: ctx.Err()}
		}
	}
	return sendResult{}
}

// subscribeWithHandlerImpl implements the common SubscribeWithHandler logic.
// This extracts the duplicate code from memoryPubSub and redisPubSub.
func subscribeWithHandlerImpl(
	ctx context.Context,
	subscriber Subscriber,
	topic string,
	handler Handler,
	onError func(topic string),
) error {
	if handler == nil {
		return ErrNilHandler
	}

	sub, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	for msg := range sub.Messages() {
		if err := handler(ctx, msg); err != nil {
			if onError != nil {
				onError(topic)
			}
		}
	}

	return nil
}
