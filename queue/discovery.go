package queue

import (
	"context"
	"fmt"
	"strings"
)

// keyPrefix is the full prefix used by GetQueueKey(): Namespace + "::"
// Queue keys are in format: container::queue::::name (Namespace + "::" + name)
const keyPrefix = Namespace + "::"

// =============================================================================
// UnifiedFactory Discovery Methods
// =============================================================================

// DiscoverQueues discovers queues from the factory's backend.
// For Redis backends, it scans Redis keys with the queue namespace prefix.
// For Memory backends, it returns queues from the factory's cache.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
func (f *UnifiedFactory) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	switch f.config.Type {
	case QueueTypeRedis:
		return f.discoverRedisQueues(ctx, pattern)
	case QueueTypeMemory:
		return f.discoverMemoryQueues(ctx, pattern)
	default:
		return nil, fmt.Errorf("queue discovery not supported for type %q", f.config.Type)
	}
}

// DiscoverAllQueues discovers all queues including retry and DLQ.
// For Redis backends, it returns all queue keys (main, retry, DLQ).
// For Memory backends, it returns the same as DiscoverQueues (memory doesn't persist retry/DLQ separately).
func (f *UnifiedFactory) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	switch f.config.Type {
	case QueueTypeRedis:
		return f.discoverAllRedisQueues(ctx, pattern)
	case QueueTypeMemory:
		return f.discoverMemoryQueues(ctx, pattern)
	default:
		return nil, fmt.Errorf("queue discovery not supported for type %q", f.config.Type)
	}
}

// =============================================================================
// Redis Discovery Implementation
// =============================================================================

// discoverRedisQueues scans Redis for queue keys and returns main queue information.
func (f *UnifiedFactory) discoverRedisQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	allKeys, err := f.scanRedisKeys(ctx, scanPattern)
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

// discoverAllRedisQueues returns detailed information about all queues including retry and DLQ.
func (f *UnifiedFactory) discoverAllRedisQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	scanPattern := keyPrefix
	if pattern == "" || pattern == "*" {
		scanPattern += "*"
	} else {
		scanPattern += pattern
	}

	allKeys, err := f.scanRedisKeys(ctx, scanPattern)
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

// scanRedisKeys scans Redis keys matching the pattern
func (f *UnifiedFactory) scanRedisKeys(ctx context.Context, scanPattern string) ([]string, error) {
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

// =============================================================================
// Memory Discovery Implementation
// =============================================================================

// discoverMemoryQueues returns queue information from the factory's cache.
func (f *UnifiedFactory) discoverMemoryQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	result := make([]QueueInfo, 0, len(f.cache))
	for name := range f.cache {
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
