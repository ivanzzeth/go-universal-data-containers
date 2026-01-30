package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// keyPrefix is the full prefix used by GetQueueKey(): Namespace + "::"
// Queue keys are in format: container::queue::::name (Namespace + "::" + name)
const keyPrefix = Namespace + "::"

// =============================================================================
// Redis Discovery Implementation
// =============================================================================

// RedisQueueDiscovery implements Discoverable for Redis backends.
// It discovers queues by scanning Redis keys with the queue namespace prefix.
type RedisQueueDiscovery struct {
	redisClient redis.Cmdable
}

// NewRedisQueueDiscovery creates a new Redis queue discovery instance.
func NewRedisQueueDiscovery(redisClient redis.Cmdable) *RedisQueueDiscovery {
	return &RedisQueueDiscovery{
		redisClient: redisClient,
	}
}

// Verify interface compliance
var _ Discoverable = (*RedisQueueDiscovery)(nil)

// DiscoverQueues scans Redis for all queue keys and returns queue information.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
// An empty pattern matches all queues.
// By default, returns only main queues (excludes retry and DLQ queues).
func (d *RedisQueueDiscovery) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	allKeys, err := d.scanKeys(ctx, scanPattern)
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
			depth, err := d.redisClient.LLen(ctx, key).Result()
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
func (d *RedisQueueDiscovery) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	allKeys, err := d.scanKeys(ctx, scanPattern)
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

		depth, _ := d.redisClient.LLen(ctx, key).Result()

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
func (d *RedisQueueDiscovery) scanKeys(ctx context.Context, scanPattern string) ([]string, error) {
	var cursor uint64
	var allKeys []string

	for {
		keys, nextCursor, err := d.redisClient.Scan(ctx, cursor, scanPattern, 100).Result()
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

// =============================================================================
// Memory Discovery Implementation
// =============================================================================

// memoryDiscovery implements Discoverable for memory backends.
// It discovers queues from the factory's cache.
type memoryDiscovery struct {
	factory *UnifiedFactory
}

// Verify interface compliance
var _ Discoverable = (*memoryDiscovery)(nil)

// DiscoverQueues returns cached queue information for memory backends.
func (d *memoryDiscovery) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	d.factory.cacheMu.Lock()
	defer d.factory.cacheMu.Unlock()

	result := make([]QueueInfo, 0, len(d.factory.cache))
	for name := range d.factory.cache {
		if pattern != "" && pattern != "*" {
			if !matchPattern(pattern, name) {
				continue
			}
		}
		result = append(result, QueueInfo{
			Name: name,
			Key:  name,
			Type: QueueInfoTypeMain,
		})
	}
	return result, nil
}

// DiscoverAllQueues returns cached queue information for memory backends.
// Memory queues don't persist retry/DLQ separately in cache, so this returns the same as DiscoverQueues.
func (d *memoryDiscovery) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	return d.DiscoverQueues(ctx, pattern)
}

// =============================================================================
// UnifiedFactory Discovery Methods
// =============================================================================

// DiscoverQueues discovers queues from the factory's backend.
// Delegates to the backend-specific Discoverable implementation.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
func (f *UnifiedFactory) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	if f.discoverable == nil {
		return nil, fmt.Errorf("queue discovery not supported for type %q", f.config.Type)
	}
	return f.discoverable.DiscoverQueues(ctx, pattern)
}

// DiscoverAllQueues discovers all queues including retry and DLQ.
// Delegates to the backend-specific Discoverable implementation.
func (f *UnifiedFactory) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	if f.discoverable == nil {
		return nil, fmt.Errorf("queue discovery not supported for type %q", f.config.Type)
	}
	return f.discoverable.DiscoverAllQueues(ctx, pattern)
}

// SetDiscoverable sets a custom Discoverable implementation for the factory.
// This is useful for custom queue backends that want to provide their own discovery.
func (f *UnifiedFactory) SetDiscoverable(d Discoverable) {
	f.discoverable = d
}

// =============================================================================
// Pattern Matching Utilities
// =============================================================================

// matchPattern performs simple glob-style pattern matching.
// Supports * as wildcard for any characters.
func matchPattern(pattern, name string) bool {
	if pattern == "*" || pattern == "" {
		return true
	}

	// Handle patterns like "prefix*", "*suffix", "*contains*"
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		// *contains*
		substr := pattern[1 : len(pattern)-1]
		return strings.Contains(name, substr)
	}
	if strings.HasPrefix(pattern, "*") {
		// *suffix
		suffix := pattern[1:]
		return strings.HasSuffix(name, suffix)
	}
	if strings.HasSuffix(pattern, "*") {
		// prefix*
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(name, prefix)
	}

	// Exact match
	return name == pattern
}
