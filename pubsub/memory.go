package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/ivanzzeth/go-universal-data-containers/message"
)

var _ PubSub = (*memoryPubSub)(nil)

// memoryPubSub implements PubSub using in-memory channels
type memoryPubSub struct {
	mu         sync.RWMutex
	subs       map[string]map[string]*memorySubscription // topic -> subID -> subscription
	bufferSize int
	onFull     OverflowPolicy
	closed     atomic.Bool
	metrics    *Metrics
}

// memorySubscription represents a single subscription
type memorySubscription struct {
	id        string
	topic     string
	ch        chan message.Message[any]
	done      chan struct{}
	closed    atomic.Bool
	cancelFn  context.CancelFunc
	closeOnce sync.Once
	sendMu    sync.RWMutex // protects send operations vs channel close
}

func newMemoryPubSub(cfg Config) (PubSub, error) {
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 100
	}

	return &memoryPubSub{
		subs:       make(map[string]map[string]*memorySubscription),
		bufferSize: bufSize,
		onFull:     cfg.OnFull,
		metrics:    NewMetrics("memory"),
	}, nil
}

func (m *memoryPubSub) Publish(ctx context.Context, topic string, msg message.Message[any]) error {
	if m.closed.Load() {
		return ErrPubSubClosed
	}

	if topic == "" {
		return ErrTopicEmpty
	}

	if msg == nil {
		return ErrNilMessage
	}

	m.metrics.PublishTotal.WithLabelValues(topic).Inc()

	m.mu.RLock()
	topicSubs, exists := m.subs[topic]
	if !exists || len(topicSubs) == 0 {
		m.mu.RUnlock()
		return nil // no subscribers, silently drop
	}

	// Copy subscriptions to avoid holding lock during send
	subscriptions := make([]*memorySubscription, 0, len(topicSubs))
	for _, sub := range topicSubs {
		if !sub.closed.Load() {
			subscriptions = append(subscriptions, sub)
		}
	}
	m.mu.RUnlock()

	if len(subscriptions) == 0 {
		return nil
	}

	// safeSend sends a message to a subscription safely
	// Returns true if sent or dropped, false if subscription closed or context done
	safeSend := func(sub *memorySubscription) (ok bool, ctxErr error) {
		// Acquire read lock to prevent channel close during send
		sub.sendMu.RLock()
		defer sub.sendMu.RUnlock()

		if sub.closed.Load() {
			return false, nil
		}

		switch m.onFull {
		case OverflowDrop:
			select {
			case sub.ch <- msg:
				return true, nil
			case <-sub.done:
				return false, nil
			case <-ctx.Done():
				return false, ctx.Err()
			default:
				// channel full, drop message
				m.metrics.DroppedTotal.WithLabelValues(topic).Inc()
				return true, nil // dropped but ok
			}

		case OverflowBlock:
			select {
			case sub.ch <- msg:
				return true, nil
			case <-sub.done:
				return false, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		return false, nil
	}

	// Fan-out: send to all subscribers
	var delivered int
	for _, sub := range subscriptions {
		ok, ctxErr := safeSend(sub)
		if ctxErr != nil {
			return ctxErr
		}
		if ok {
			delivered++
		}
	}

	m.metrics.DeliveredTotal.WithLabelValues(topic).Add(float64(delivered))
	return nil
}

func (m *memoryPubSub) PublishBatch(ctx context.Context, topic string, msgs []message.Message[any]) error {
	for _, msg := range msgs {
		if err := m.Publish(ctx, topic, msg); err != nil {
			return err
		}
	}
	return nil
}

func (m *memoryPubSub) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	if m.closed.Load() {
		return nil, ErrPubSubClosed
	}

	if topic == "" {
		return nil, ErrTopicEmpty
	}

	subCtx, cancel := context.WithCancel(ctx)

	sub := &memorySubscription{
		id:       uuid.New().String(),
		topic:    topic,
		ch:       make(chan message.Message[any], m.bufferSize),
		done:     make(chan struct{}),
		cancelFn: cancel,
	}

	m.mu.Lock()
	if m.subs[topic] == nil {
		m.subs[topic] = make(map[string]*memorySubscription)
	}
	m.subs[topic][sub.id] = sub
	count := len(m.subs[topic])
	m.mu.Unlock()

	m.metrics.SubscribersGauge.WithLabelValues(topic).Set(float64(count))
	m.metrics.SubscribeTotal.WithLabelValues(topic).Inc()

	// Cleanup goroutine
	go func() {
		select {
		case <-subCtx.Done():
		case <-sub.done:
		}
		m.removeSubscription(sub)
	}()

	return sub, nil
}

func (m *memoryPubSub) SubscribeWithHandler(ctx context.Context, topic string, handler Handler) error {
	if handler == nil {
		return ErrNilHandler
	}

	sub, err := m.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	for msg := range sub.Messages() {
		if err := handler(ctx, msg); err != nil {
			m.metrics.HandlerErrorTotal.WithLabelValues(topic).Inc()
			// Log error but continue processing
		}
	}

	return nil
}

func (m *memoryPubSub) removeSubscription(sub *memorySubscription) {
	if sub.closed.Swap(true) {
		return // already closed
	}

	close(sub.done)

	m.mu.Lock()
	if topicSubs, exists := m.subs[sub.topic]; exists {
		delete(topicSubs, sub.id)
		if len(topicSubs) == 0 {
			delete(m.subs, sub.topic)
			m.metrics.SubscribersGauge.WithLabelValues(sub.topic).Set(0)
		} else {
			m.metrics.SubscribersGauge.WithLabelValues(sub.topic).Set(float64(len(topicSubs)))
		}
	}
	m.mu.Unlock()

	// Acquire write lock to ensure no sends are in progress before closing channel
	sub.sendMu.Lock()
	defer sub.sendMu.Unlock()

	// Close channel using closeOnce to ensure it's closed only once
	sub.closeOnce.Do(func() {
		close(sub.ch)
	})
}

func (m *memoryPubSub) Topics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]string, 0, len(m.subs))
	for topic := range m.subs {
		topics = append(topics, topic)
	}
	return topics
}

func (m *memoryPubSub) SubscriberCount(topic string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if topicSubs, exists := m.subs[topic]; exists {
		return len(topicSubs)
	}
	return 0
}

func (m *memoryPubSub) Close() error {
	if m.closed.Swap(true) {
		return nil // already closed
	}

	m.mu.Lock()
	// Collect subscriptions to close
	var subsToClose []*memorySubscription
	for _, topicSubs := range m.subs {
		for _, sub := range topicSubs {
			subsToClose = append(subsToClose, sub)
		}
	}
	m.subs = nil
	m.mu.Unlock()

	// Close all subscriptions outside the lock
	for _, sub := range subsToClose {
		if !sub.closed.Swap(true) {
			// Signal done first
			close(sub.done)

			// Cancel context
			if sub.cancelFn != nil {
				sub.cancelFn()
			}

			// Acquire write lock to ensure no sends are in progress
			sub.sendMu.Lock()
			// Close message channel using closeOnce
			sub.closeOnce.Do(func() {
				close(sub.ch)
			})
			sub.sendMu.Unlock()
		}
	}

	return nil
}

// memorySubscription implements Subscription
func (s *memorySubscription) ID() string {
	return s.id
}

func (s *memorySubscription) Topic() string {
	return s.topic
}

func (s *memorySubscription) Messages() <-chan message.Message[any] {
	return s.ch
}

func (s *memorySubscription) Unsubscribe() error {
	if s.closed.Load() {
		return ErrSubscriptionClosed
	}

	if s.cancelFn != nil {
		s.cancelFn()
	}
	return nil
}

func (s *memorySubscription) String() string {
	return fmt.Sprintf("Subscription{id=%s, topic=%s}", s.id, s.topic)
}
