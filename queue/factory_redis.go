package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RedisFactory is a non-generic factory for creating Redis-backed queues.
// Unlike RedisQueueFactory[T], this factory doesn't require a type parameter
// at construction time. The type is specified when creating queues.
// This allows RedisFactory to implement the Discoverable interface directly.
type RedisFactory struct {
	redisClient redis.Cmdable

	// cache stores created queues by name for reuse
	cache   map[string]any
	cacheMu sync.Mutex
}

// Verify RedisFactory implements Discoverable
var _ Discoverable = (*RedisFactory)(nil)

// NewRedisFactory creates a new non-generic Redis factory.
// The factory can create queues of any message type using GetOrCreateSafe.
func NewRedisFactory(redisClient redis.Cmdable) (*RedisFactory, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("redis client is required")
	}

	return &RedisFactory{
		redisClient: redisClient,
		cache:       make(map[string]any),
	}, nil
}

// GetOrCreateSafe creates or returns a cached SafeQueue with the given name and message type.
// This is a generic function that allows creating queues of different types from the same factory.
//
// Example:
//
//	factory, _ := queue.NewRedisFactory(redisClient)
//	bytesQueue, _ := queue.RedisGetOrCreateSafe[[]byte](factory, "bytes-queue", queue.NewJsonMessage([]byte{}))
//	myTypeQueue, _ := queue.RedisGetOrCreateSafe[MyType](factory, "mytype-queue", queue.NewJsonMessage(MyType{}))
func RedisGetOrCreateSafe[T any](f *RedisFactory, name string, defaultMsg Message[T], options ...Option) (SafeQueue[T], error) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	cacheKey := name

	// Return cached queue if exists and type matches
	if cached, ok := f.cache[cacheKey]; ok {
		if q, ok := cached.(SafeQueue[T]); ok {
			return q, nil
		}
		// Type mismatch - queue exists with different type
		return nil, fmt.Errorf("queue %q already exists with a different message type", name)
	}

	// Create queue
	q, err := NewRedisQueue(f.redisClient, name, defaultMsg, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis queue: %w", err)
	}

	// Wrap in SimpleQueue
	safeQ, err := NewSimpleQueue(q)
	if err != nil {
		return nil, fmt.Errorf("failed to create SimpleQueue: %w", err)
	}

	// Cache and return
	f.cache[cacheKey] = safeQ
	return safeQ, nil
}

// RedisGetOrCreate creates or returns a cached Queue with the given name and message type.
// This is a convenience wrapper around RedisGetOrCreateSafe.
func RedisGetOrCreate[T any](f *RedisFactory, name string, defaultMsg Message[T], options ...Option) (Queue[T], error) {
	return RedisGetOrCreateSafe(f, name, defaultMsg, options...)
}

// =============================================================================
// Discoverable Implementation
// =============================================================================

// DiscoverQueues scans Redis for all queue keys and returns queue information.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
// An empty pattern matches all queues.
// By default, returns only main queues (excludes retry and DLQ queues).
func (f *RedisFactory) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	allKeys, err := f.scanKeys(ctx, scanPattern)
	if err != nil {
		return nil, err
	}

	// Parse keys and build QueueInfo list
	// Use a map to deduplicate and group by base queue name
	queueMap := make(map[string]*QueueInfo)

	for _, key := range allKeys {
		if !strings.HasPrefix(key, keyPrefix) {
			continue
		}
		name := strings.TrimPrefix(key, keyPrefix)

		// Determine queue type and extract base name
		queueType := QueueInfoTypeMain
		baseName := name

		if strings.HasSuffix(name, "::retry") {
			queueType = QueueInfoTypeRetry
			baseName = strings.TrimSuffix(name, "::retry")
		} else if strings.HasSuffix(name, "::DLQ") {
			queueType = QueueInfoTypeDLQ
			baseName = strings.TrimSuffix(name, "::DLQ")
		}

		// Get or create queue info
		info, exists := queueMap[baseName]
		if !exists {
			info = &QueueInfo{
				Name: baseName,
				Key:  keyPrefix + baseName,
				Type: QueueInfoTypeMain,
			}
			queueMap[baseName] = info
		}

		// If this is the main queue, get its depth
		if queueType == QueueInfoTypeMain {
			depth, err := f.redisClient.LLen(ctx, key).Result()
			if err == nil {
				info.Depth = depth
			}
		}
	}

	// Convert map to slice
	result := make([]QueueInfo, 0, len(queueMap))
	for _, info := range queueMap {
		result = append(result, *info)
	}

	return result, nil
}

// DiscoverAllQueues returns detailed information about all queues including retry and DLQ.
func (f *RedisFactory) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	allKeys, err := f.scanKeys(ctx, scanPattern)
	if err != nil {
		return nil, err
	}

	result := make([]QueueInfo, 0, len(allKeys))

	for _, key := range allKeys {
		if !strings.HasPrefix(key, keyPrefix) {
			continue
		}
		name := strings.TrimPrefix(key, keyPrefix)

		queueType := QueueInfoTypeMain
		if strings.HasSuffix(name, "::retry") {
			queueType = QueueInfoTypeRetry
		} else if strings.HasSuffix(name, "::DLQ") {
			queueType = QueueInfoTypeDLQ
		}

		depth, _ := f.redisClient.LLen(ctx, key).Result()

		result = append(result, QueueInfo{
			Name:  name,
			Key:   key,
			Type:  queueType,
			Depth: depth,
		})
	}

	return result, nil
}

// scanKeys scans Redis keys matching the pattern
func (f *RedisFactory) scanKeys(ctx context.Context, scanPattern string) ([]string, error) {
	var cursor uint64
	var allKeys []string

	for {
		keys, nextCursor, err := f.redisClient.Scan(ctx, cursor, scanPattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan redis keys: %w", err)
		}

		allKeys = append(allKeys, keys...)
		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}

	return allKeys, nil
}

// Client returns the underlying Redis client.
func (f *RedisFactory) Client() redis.Cmdable {
	return f.redisClient
}
