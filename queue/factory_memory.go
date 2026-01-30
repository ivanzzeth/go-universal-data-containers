package queue

import (
	"context"
	"fmt"
	"sync"
)

// MemoryQueueFactory is a non-generic factory for creating memory-backed queues.
// Unlike the generic MemoryFactory[T], this factory doesn't require a type parameter
// at construction time. The type is specified when creating queues.
// This allows MemoryQueueFactory to implement the Discoverable interface directly.
type MemoryQueueFactory struct {
	// cache stores created queues by name for reuse
	cache   map[string]any
	cacheMu sync.Mutex
}

// Verify MemoryQueueFactory implements Discoverable
var _ Discoverable = (*MemoryQueueFactory)(nil)

// NewMemoryQueueFactory creates a new non-generic memory factory.
// The factory can create queues of any message type using GetOrCreateSafe.
func NewMemoryQueueFactory() (*MemoryQueueFactory, error) {
	return &MemoryQueueFactory{
		cache: make(map[string]any),
	}, nil
}

// MemoryGetOrCreateSafe creates or returns a cached SafeQueue with the given name and message type.
// This is a generic function that allows creating queues of different types from the same factory.
//
// Example:
//
//	factory, _ := queue.NewMemoryQueueFactory()
//	bytesQueue, _ := queue.MemoryGetOrCreateSafe[[]byte](factory, "bytes-queue", queue.NewJsonMessage([]byte{}))
//	myTypeQueue, _ := queue.MemoryGetOrCreateSafe[MyType](factory, "mytype-queue", queue.NewJsonMessage(MyType{}))
func MemoryGetOrCreateSafe[T any](f *MemoryQueueFactory, name string, defaultMsg Message[T], options ...Option) (SafeQueue[T], error) {
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
	q, err := NewMemoryQueue(name, defaultMsg, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create memory queue: %w", err)
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

// MemoryGetOrCreate creates or returns a cached Queue with the given name and message type.
// This is a convenience wrapper around MemoryGetOrCreateSafe.
func MemoryGetOrCreate[T any](f *MemoryQueueFactory, name string, defaultMsg Message[T], options ...Option) (Queue[T], error) {
	return MemoryGetOrCreateSafe(f, name, defaultMsg, options...)
}

// =============================================================================
// Discoverable Implementation
// =============================================================================

// DiscoverQueues returns cached queue information for memory backends.
// The pattern parameter supports glob-style matching (e.g., "*", "orders-*").
// An empty pattern matches all queues.
func (f *MemoryQueueFactory) DiscoverQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
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

// DiscoverAllQueues returns cached queue information for memory backends.
// Memory queues don't persist retry/DLQ separately in cache, so this returns the same as DiscoverQueues.
func (f *MemoryQueueFactory) DiscoverAllQueues(ctx context.Context, pattern string) ([]QueueInfo, error) {
	return f.DiscoverQueues(ctx, pattern)
}
