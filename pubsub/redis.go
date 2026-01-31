package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/ivanzzeth/go-universal-data-containers/message"
	"github.com/redis/go-redis/v9"
)

var _ PubSub = (*redisPubSub)(nil)

// RedisClient is an interface that supports both publishing and subscribing
type RedisClient interface {
	redis.Cmdable
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	PSubscribe(ctx context.Context, channels ...string) *redis.PubSub
}

// redisPubSub implements PubSub using Redis Pub/Sub
type redisPubSub struct {
	client       RedisClient
	prefix       string
	bufferSize   int
	batchSizeMax int
	onFull       OverflowPolicy
	closed       atomic.Bool
	metrics      *Metrics
	bufferPool   *sync.Pool // Pool for reusing byte slices

	mu   sync.RWMutex
	subs map[string]map[string]*redisSubscription // topic -> subID -> subscription
}

// redisSubscription represents a single Redis subscription
type redisSubscription struct {
	id          string
	topic       string
	ch          chan message.Message[any]
	done        chan struct{}
	exited      chan struct{} // signals that receiveMessages has exited
	closed      atomic.Bool
	cancelFn    context.CancelFunc
	redisPubSub *redis.PubSub
	closeOnce   sync.Once // ensures ch is closed only once
}

func newRedisPubSub(cfg Config) (PubSub, error) {
	opts := RedisOptionsFromMap(cfg.Options)

	// Check if a client was provided in options
	var client RedisClient
	if c, ok := cfg.Options["client"].(RedisClient); ok {
		client = c
	} else if c, ok := cfg.Options["client"].(*redis.Client); ok {
		client = c
	} else {
		// Create a new Redis client
		client = redis.NewClient(&redis.Options{
			Addr:         opts.Addr,
			Password:     opts.Password,
			DB:           opts.DB,
			PoolSize:     opts.PoolSize,
			ReadTimeout:  opts.ReadTimeout,
			WriteTimeout: opts.WriteTimeout,
		})
	}

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 100
	}

	batchSizeMax := cfg.BatchSizeMax
	if batchSizeMax <= 0 {
		batchSizeMax = DefaultBatchSizeMax
	}

	return &redisPubSub{
		client:       client,
		prefix:       opts.Prefix,
		bufferSize:   bufSize,
		batchSizeMax: batchSizeMax,
		onFull:       cfg.OnFull,
		metrics:      NewMetrics("redis"),
		subs:         make(map[string]map[string]*redisSubscription),
		bufferPool: &sync.Pool{
			New: func() any {
				// Pre-allocate 4KB buffer for message serialization
				buf := make([]byte, 0, 4096)
				return &buf
			},
		},
	}, nil
}

func (r *redisPubSub) channelName(topic string) string {
	return r.prefix + topic
}

func (r *redisPubSub) Publish(ctx context.Context, topic string, msg message.Message[any]) error {
	if r.closed.Load() {
		return ErrPubSubClosed
	}

	if topic == "" {
		return ErrTopicEmpty
	}

	if msg == nil {
		return ErrNilMessage
	}

	r.metrics.PublishTotal.WithLabelValues(topic).Inc()

	// Serialize message
	data, err := msg.Pack()
	if err != nil {
		return fmt.Errorf("pack message: %w", err)
	}

	// Publish to Redis
	channel := r.channelName(topic)
	result := r.client.Publish(ctx, channel, data)
	if err := result.Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}

	// Track delivered count (Redis returns number of subscribers that received the message)
	delivered := result.Val()
	r.metrics.DeliveredTotal.WithLabelValues(topic).Add(float64(delivered))

	return nil
}

func (r *redisPubSub) PublishBatch(ctx context.Context, topic string, msgs []message.Message[any]) error {
	if r.closed.Load() {
		return ErrPubSubClosed
	}

	if topic == "" {
		return ErrTopicEmpty
	}

	if len(msgs) == 0 {
		return nil
	}

	channel := r.channelName(topic)

	// Pre-serialize all messages to catch errors early
	serializedMsgs := make([][]byte, 0, len(msgs))
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		data, err := msg.Pack()
		if err != nil {
			return fmt.Errorf("pack message: %w", err)
		}
		serializedMsgs = append(serializedMsgs, data)
	}

	if len(serializedMsgs) == 0 {
		return nil
	}

	// Process in batches using configurable batch size
	batchSize := r.batchSizeMax
	var totalDelivered int64
	for i := 0; i < len(serializedMsgs); i += batchSize {
		end := min(i+batchSize, len(serializedMsgs))
		batch := serializedMsgs[i:end]

		delivered, err := r.publishPipelineBatch(ctx, channel, topic, batch)
		if err != nil {
			return err
		}
		totalDelivered += delivered
	}

	r.metrics.DeliveredTotal.WithLabelValues(topic).Add(float64(totalDelivered))
	return nil
}

// publishPipelineBatch publishes a batch of serialized messages using Redis pipeline
func (r *redisPubSub) publishPipelineBatch(ctx context.Context, channel, topic string, data [][]byte) (int64, error) {
	// Record batch size
	r.metrics.BatchSize.WithLabelValues(topic).Observe(float64(len(data)))

	pipe := r.client.Pipeline()

	// Queue all publish commands
	cmds := make([]*redis.IntCmd, len(data))
	for i, d := range data {
		cmds[i] = pipe.Publish(ctx, channel, d)
	}

	// Execute pipeline with timing
	start := time.Now()
	_, err := pipe.Exec(ctx)
	elapsed := time.Since(start)
	r.metrics.PipelineLatency.WithLabelValues(topic).Observe(elapsed.Seconds())

	if err != nil {
		r.metrics.PipelineErrorTotal.WithLabelValues(topic).Inc()
		return 0, fmt.Errorf("redis pipeline exec: %w", err)
	}

	// Collect results and count delivered messages
	var delivered int64
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			delivered += cmd.Val()
		}
	}

	r.metrics.PublishTotal.WithLabelValues(topic).Add(float64(len(data)))
	return delivered, nil
}

func (r *redisPubSub) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	if r.closed.Load() {
		return nil, ErrPubSubClosed
	}

	if topic == "" {
		return nil, ErrTopicEmpty
	}

	subCtx, cancel := context.WithCancel(ctx)
	channel := r.channelName(topic)

	// Create Redis subscription
	redisSub := r.client.Subscribe(subCtx, channel)

	// Wait for subscription confirmation
	_, err := redisSub.Receive(subCtx)
	if err != nil {
		cancel()
		redisSub.Close()
		return nil, fmt.Errorf("redis subscribe: %w", err)
	}

	sub := &redisSubscription{
		id:          uuid.New().String(),
		topic:       topic,
		ch:          make(chan message.Message[any], r.bufferSize),
		done:        make(chan struct{}),
		exited:      make(chan struct{}),
		cancelFn:    cancel,
		redisPubSub: redisSub,
	}

	r.mu.Lock()
	if r.subs[topic] == nil {
		r.subs[topic] = make(map[string]*redisSubscription)
	}
	r.subs[topic][sub.id] = sub
	count := len(r.subs[topic])
	r.mu.Unlock()

	r.metrics.SubscribersGauge.WithLabelValues(topic).Set(float64(count))
	r.metrics.SubscribeTotal.WithLabelValues(topic).Inc()

	// Start message receiving goroutine
	go r.receiveMessages(subCtx, sub)

	// Cleanup goroutine to handle context cancellation and unsubscribe
	go func() {
		select {
		case <-subCtx.Done():
		case <-sub.done:
		}
		r.removeSubscription(sub)
	}()

	return sub, nil
}

func (r *redisPubSub) receiveMessages(ctx context.Context, sub *redisSubscription) {
	defer close(sub.exited) // Signal that this goroutine has exited

	ch := sub.redisPubSub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.done:
			return
		case redisMsg, ok := <-ch:
			if !ok {
				return // channel closed
			}

			// Deserialize message
			msg := &message.JsonMessage[any]{}
			if err := msg.Unpack([]byte(redisMsg.Payload)); err != nil {
				// Skip invalid messages
				continue
			}

			// Send to subscriber channel
			switch r.onFull {
			case OverflowDrop:
				select {
				case sub.ch <- msg:
				case <-sub.done:
					return
				case <-ctx.Done():
					return
				default:
					// channel full, drop message
					r.metrics.DroppedTotal.WithLabelValues(sub.topic).Inc()
				}

			case OverflowBlock:
				select {
				case sub.ch <- msg:
				case <-sub.done:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (r *redisPubSub) SubscribeWithHandler(ctx context.Context, topic string, handler Handler) error {
	if handler == nil {
		return ErrNilHandler
	}

	sub, err := r.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	for msg := range sub.Messages() {
		if err := handler(ctx, msg); err != nil {
			r.metrics.HandlerErrorTotal.WithLabelValues(topic).Inc()
			// Log error but continue processing
		}
	}

	return nil
}

func (r *redisPubSub) removeSubscription(sub *redisSubscription) {
	if sub.closed.Swap(true) {
		return // already closed
	}

	// Close done channel first to signal receiveMessages to stop
	close(sub.done)

	// Close Redis subscription - this will close the Redis channel
	// and cause receiveMessages to exit
	if sub.redisPubSub != nil {
		sub.redisPubSub.Close()
	}

	r.mu.Lock()
	if topicSubs, exists := r.subs[sub.topic]; exists {
		delete(topicSubs, sub.id)
		if len(topicSubs) == 0 {
			delete(r.subs, sub.topic)
			r.metrics.SubscribersGauge.WithLabelValues(sub.topic).Set(0)
		} else {
			r.metrics.SubscribersGauge.WithLabelValues(sub.topic).Set(float64(len(topicSubs)))
		}
	}
	r.mu.Unlock()

	r.metrics.UnsubscribeTotal.WithLabelValues(sub.topic).Inc()

	// Close the message channel using sync.Once to ensure it's closed only once
	sub.closeOnce.Do(func() {
		close(sub.ch)
	})
}

func (r *redisPubSub) Topics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topics := make([]string, 0, len(r.subs))
	for topic := range r.subs {
		topics = append(topics, topic)
	}
	return topics
}

func (r *redisPubSub) SubscriberCount(topic string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if topicSubs, exists := r.subs[topic]; exists {
		return len(topicSubs)
	}
	return 0
}

func (r *redisPubSub) Close() error {
	if r.closed.Swap(true) {
		return nil // already closed
	}

	r.mu.Lock()
	// Collect all subscriptions to close
	var subsToClose []*redisSubscription
	for _, topicSubs := range r.subs {
		for _, sub := range topicSubs {
			subsToClose = append(subsToClose, sub)
		}
	}
	r.subs = nil
	r.mu.Unlock()

	// Close all subscriptions outside the lock
	for _, sub := range subsToClose {
		if !sub.closed.Swap(true) {
			// Signal receiveMessages to stop
			close(sub.done)

			// Close Redis subscription to unblock Channel()
			if sub.redisPubSub != nil {
				sub.redisPubSub.Close()
			}

			// Cancel context
			if sub.cancelFn != nil {
				sub.cancelFn()
			}

			// Wait for receiveMessages to exit before closing the channel
			<-sub.exited

			// Now it's safe to close the message channel
			sub.closeOnce.Do(func() {
				close(sub.ch)
			})
		}
	}

	return nil
}

// redisSubscription implements Subscription
func (s *redisSubscription) ID() string {
	return s.id
}

func (s *redisSubscription) Topic() string {
	return s.topic
}

func (s *redisSubscription) Messages() <-chan message.Message[any] {
	return s.ch
}

func (s *redisSubscription) Unsubscribe() error {
	if s.closed.Load() {
		return ErrSubscriptionClosed
	}

	if s.cancelFn != nil {
		s.cancelFn()
	}
	return nil
}

func (s *redisSubscription) String() string {
	return fmt.Sprintf("RedisSubscription{id=%s, topic=%s}", s.id, s.topic)
}
