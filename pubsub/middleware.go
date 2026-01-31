package pubsub

import (
	"context"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/message"
)

// PublisherMiddleware wraps a Publisher with additional functionality
type PublisherMiddleware func(Publisher) Publisher

// SubscriberMiddleware wraps a Subscriber with additional functionality
type SubscriberMiddleware func(Subscriber) Subscriber

// HandlerMiddleware wraps a Handler with additional functionality
type HandlerMiddleware func(Handler) Handler

// ChainPublisher chains multiple middleware around a Publisher
// Middleware is applied in the order provided (first middleware is outermost)
func ChainPublisher(p Publisher, mws ...PublisherMiddleware) Publisher {
	for i := len(mws) - 1; i >= 0; i-- {
		p = mws[i](p)
	}
	return p
}

// ChainHandler chains multiple middleware around a Handler
// Middleware is applied in the order provided (first middleware is outermost)
func ChainHandler(h Handler, mws ...HandlerMiddleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// WithRetry returns a PublisherMiddleware that retries failed publishes
func WithRetry(maxRetries int, backoff time.Duration) PublisherMiddleware {
	return func(next Publisher) Publisher {
		return &retryPublisher{
			next:       next,
			maxRetries: maxRetries,
			backoff:    backoff,
		}
	}
}

type retryPublisher struct {
	next       Publisher
	maxRetries int
	backoff    time.Duration
}

func (r *retryPublisher) Publish(ctx context.Context, topic string, msg message.Message[any]) error {
	var lastErr error
	for i := 0; i <= r.maxRetries; i++ {
		if err := r.next.Publish(ctx, topic, msg); err != nil {
			lastErr = err
			if i < r.maxRetries {
				select {
				case <-time.After(r.backoff * time.Duration(i+1)):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (r *retryPublisher) PublishBatch(ctx context.Context, topic string, msgs []message.Message[any]) error {
	var lastErr error
	for i := 0; i <= r.maxRetries; i++ {
		if err := r.next.PublishBatch(ctx, topic, msgs); err != nil {
			lastErr = err
			if i < r.maxRetries {
				select {
				case <-time.After(r.backoff * time.Duration(i+1)):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}

// WithTimeout returns a PublisherMiddleware that adds a timeout to publish operations
func WithTimeout(timeout time.Duration) PublisherMiddleware {
	return func(next Publisher) Publisher {
		return &timeoutPublisher{
			next:    next,
			timeout: timeout,
		}
	}
}

type timeoutPublisher struct {
	next    Publisher
	timeout time.Duration
}

func (t *timeoutPublisher) Publish(ctx context.Context, topic string, msg message.Message[any]) error {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.next.Publish(ctx, topic, msg)
}

func (t *timeoutPublisher) PublishBatch(ctx context.Context, topic string, msgs []message.Message[any]) error {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.next.PublishBatch(ctx, topic, msgs)
}

// WithLogging returns a HandlerMiddleware that logs message processing
type LogFunc func(format string, args ...any)

func WithLogging(logFn LogFunc) HandlerMiddleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg message.Message[any]) error {
			start := time.Now()
			err := next(ctx, msg)
			duration := time.Since(start)

			if err != nil {
				logFn("message processing failed: id=%s, duration=%v, error=%v",
					string(msg.ID()), duration, err)
			} else {
				logFn("message processed: id=%s, duration=%v",
					string(msg.ID()), duration)
			}
			return err
		}
	}
}

// WithRecovery returns a HandlerMiddleware that recovers from panics
func WithRecovery(onPanic func(recovered any)) HandlerMiddleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg message.Message[any]) (err error) {
			defer func() {
				if r := recover(); r != nil {
					if onPanic != nil {
						onPanic(r)
					}
					// Don't return error, let the subscription continue
				}
			}()
			return next(ctx, msg)
		}
	}
}

// WithHandlerTimeout returns a HandlerMiddleware that adds a timeout to handler execution
func WithHandlerTimeout(timeout time.Duration) HandlerMiddleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg message.Message[any]) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- next(ctx, msg)
			}()

			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
