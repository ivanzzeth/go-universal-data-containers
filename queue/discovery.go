package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

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

// keyPrefix is the full prefix used by GetQueueKey(): Namespace + "::"
// Queue keys are in format: container::queue::::name (Namespace + "::" + name)
const keyPrefix = Namespace + "::"

// DiscoverQueues scans Redis for all queue keys and returns queue information.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
// An empty pattern matches all queues.
// By default, returns only main queues (excludes retry and DLQ queues).
func (d *RedisQueueDiscovery) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	// Build the scan pattern
	// Queue keys are in format: container::queue::::name (keyPrefix + name)
	// Retry keys: container::queue::::name::retry
	// DLQ keys: container::queue::::name::DLQ
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	// Use SCAN to iterate through keys
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

	// Parse keys and build QueueInfo list
	// Use a map to deduplicate and group by base queue name
	queueMap := make(map[string]*QueueInfo)

	for _, key := range allKeys {
		// Remove keyPrefix
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

	// Convert map to slice, returning only main queues
	result := make([]QueueInfo, 0, len(queueMap))
	for _, info := range queueMap {
		result = append(result, *info)
	}

	return result, nil
}

// DiscoverAllQueues returns detailed information about all queues including retry and DLQ.
// This is useful for queue management services that need to monitor all queue types.
func (d *RedisQueueDiscovery) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

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

// DiscoverQueues discovers queues from the factory's backend.
// For Redis backends, this scans for existing queue keys.
// For Memory backends, this returns an error as memory queues are not persistent.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
func (f *UnifiedFactory) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	switch f.config.Type {
	case QueueTypeRedis:
		if f.redisClient == nil {
			return nil, fmt.Errorf("redis client not initialized")
		}
		discovery := NewRedisQueueDiscovery(f.redisClient)
		return discovery.DiscoverQueues(ctx, pattern)

	case QueueTypeMemory:
		// Memory queues are not persistent, return cached queues
		f.cacheMu.Lock()
		defer f.cacheMu.Unlock()

		result := make([]QueueInfo, 0, len(f.cache))
		for name := range f.cache {
			// Filter by pattern if provided
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

	default:
		return nil, fmt.Errorf("queue discovery not supported for type %q", f.config.Type)
	}
}

// DiscoverAllQueues discovers all queues including retry and DLQ.
// Only supported for Redis backends.
func (f *UnifiedFactory) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	switch f.config.Type {
	case QueueTypeRedis:
		if f.redisClient == nil {
			return nil, fmt.Errorf("redis client not initialized")
		}
		discovery := NewRedisQueueDiscovery(f.redisClient)
		return discovery.DiscoverAllQueues(ctx, pattern)

	case QueueTypeMemory:
		// Memory queues - same as DiscoverQueues since we don't track retry/DLQ separately in cache
		return f.DiscoverQueues(ctx, pattern)

	default:
		return nil, fmt.Errorf("queue discovery not supported for type %q", f.config.Type)
	}
}

// matchPattern performs simple glob-style pattern matching.
// Supports * as wildcard for any characters.
func matchPattern(pattern, name string) bool {
	// Simple implementation: convert glob to check prefix/suffix/contains
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
