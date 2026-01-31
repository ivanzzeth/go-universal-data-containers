package dag

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/redis/go-redis/v9"
)

const (
	// keyPrefixLock is the Redis key prefix for distributed locks
	keyPrefixLock = "dag:lock:"
)

// Edge represents a directed edge from one vertex to another.
type Edge[T comparable] struct {
	From T
	To   T
}

// BatchResult contains the results of a batch operation.
type BatchResult struct {
	// Succeeded is the number of operations that succeeded.
	Succeeded int
	// Failed is the number of operations that failed.
	Failed int
	// Errors contains the errors for failed operations.
	// Key is the index of the failed operation in the input slice.
	Errors map[int]error
}

// BatchOption configures batch operation behavior.
type BatchOption func(*batchOptions)

type batchOptions struct {
	// skipCycleDetection skips cycle detection for batch edge additions.
	// This is useful when you know the edges don't create cycles.
	// WARNING: Use with caution. If cycles are created, the DAG may become invalid.
	skipCycleDetection bool

	// useLock uses distributed lock for the batch operation.
	// This ensures consistency in distributed environments.
	useLock bool

	// batchSize is the number of operations to batch together in a single Redis pipeline.
	// Default is 100.
	batchSize int
}

func defaultBatchOptions() *batchOptions {
	return &batchOptions{
		skipCycleDetection: false,
		useLock:            true,
		batchSize:          100,
	}
}

// WithSkipCycleDetection skips cycle detection for batch edge additions.
// WARNING: Use with caution. If cycles are created, the DAG may become invalid.
func WithSkipCycleDetection() BatchOption {
	return func(o *batchOptions) {
		o.skipCycleDetection = true
	}
}

// WithBatchLock enables distributed locking for the batch operation.
// This ensures consistency in distributed environments.
func WithBatchLock() BatchOption {
	return func(o *batchOptions) {
		o.useLock = true
	}
}

// WithoutBatchLock disables distributed locking for the batch operation.
// Use this when you know there are no concurrent operations.
func WithoutBatchLock() BatchOption {
	return func(o *batchOptions) {
		o.useLock = false
	}
}

// WithBatchSize sets the batch size for Redis pipeline operations.
func WithBatchSize(size int) BatchOption {
	return func(o *batchOptions) {
		if size > 0 {
			o.batchSize = size
		}
	}
}

// lockKey returns the distributed lock key for this DAG.
func (d *RedisDAG[T]) lockKey() string {
	return keyPrefixLock + d.name
}

// withLock executes a function while holding the distributed lock.
func (d *RedisDAG[T]) withLock(ctx context.Context, fn func(ctx context.Context) error) error {
	if d.lockerGenerator == nil {
		return fmt.Errorf("locker generator is not configured")
	}

	lock, err := d.lockerGenerator.CreateSyncLocker(d.lockKey())
	if err != nil {
		return fmt.Errorf("failed to create locker: %w", err)
	}

	if err := lock.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Unlock(ctx)

	return fn(ctx)
}

// AddVertices adds multiple vertices to the graph in a single batch operation.
// This is more efficient than calling AddVertex multiple times.
func (d *RedisDAG[T]) AddVertices(ctx context.Context, vertices []T, opts ...BatchOption) (*BatchResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if len(vertices) == 0 {
		return &BatchResult{Succeeded: 0, Failed: 0, Errors: make(map[int]error)}, nil
	}

	options := defaultBatchOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Execute with or without lock
	if options.useLock && d.lockerGenerator != nil {
		var result *BatchResult
		err := d.withLock(ctx, func(ctx context.Context) error {
			var err error
			result, err = d.addVerticesBatch(ctx, vertices, options)
			return err
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	return d.addVerticesBatch(ctx, vertices, options)
}

func (d *RedisDAG[T]) addVerticesBatch(ctx context.Context, vertices []T, options *batchOptions) (*BatchResult, error) {
	result := &BatchResult{
		Errors: make(map[int]error),
	}

	// Process in batches
	for i := 0; i < len(vertices); i += options.batchSize {
		end := min(i+options.batchSize, len(vertices))
		batch := vertices[i:end]

		// Use Redis pipeline for batch operations
		pipe := d.client.Pipeline()

		for j, vertex := range batch {
			key, err := d.marshal(vertex)
			if err != nil {
				result.Errors[i+j] = fmt.Errorf("failed to marshal vertex: %w", err)
				result.Failed++
				continue
			}

			// HSetNX will only set if key doesn't exist
			pipe.HSetNX(ctx, d.graphKey(), key, "[]")
			pipe.HSetNX(ctx, d.inDegreeKey(), key, 0)
		}

		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			// Pipeline failed, mark remaining as failed
			for j := range batch {
				if _, exists := result.Errors[i+j]; !exists {
					result.Errors[i+j] = fmt.Errorf("pipeline exec failed: %w", err)
					result.Failed++
				}
			}
			continue
		}

		// Count successes (vertices that were not in errors)
		for j := range batch {
			if _, exists := result.Errors[i+j]; !exists {
				result.Succeeded++
			}
		}
	}

	return result, nil
}

// Lua script for batch adding edges with optional cycle detection
var luaAddEdgesBatch = redis.NewScript(`
	local graphKey = KEYS[1]
	local inDegreeKey = KEYS[2]
	local skipCycleDetection = ARGV[1] == "1"
	local numEdges = tonumber(ARGV[2])
	local succeeded = 0
	local failed = 0
	local errors = {}

	-- Helper function to check for cycle using BFS
	local function hasCycle(fromKey, toKey)
		if skipCycleDetection then
			return false
		end
		if fromKey == toKey then
			return true
		end
		local visited = {}
		local queue = {toKey}
		while #queue > 0 do
			local current = table.remove(queue, 1)
			if current == fromKey then
				return true
			end
			if not visited[current] then
				visited[current] = true
				local currentSuccessorsJson = redis.call('HGET', graphKey, current)
				if currentSuccessorsJson then
					local currentSuccessors = cjson.decode(currentSuccessorsJson)
					for _, s in ipairs(currentSuccessors) do
						if not visited[s] then
							table.insert(queue, s)
						end
					end
				end
			end
		end
		return false
	end

	-- Process each edge
	for i = 1, numEdges do
		local fromKey = ARGV[2 + (i - 1) * 2 + 1]
		local toKey = ARGV[2 + (i - 1) * 2 + 2]

		-- Skip self-loops
		if fromKey == toKey then
			succeeded = succeeded + 1
		else
			-- Ensure both vertices exist
			if redis.call('HEXISTS', graphKey, fromKey) == 0 then
				redis.call('HSET', graphKey, fromKey, '[]')
				redis.call('HSETNX', inDegreeKey, fromKey, 0)
			end
			if redis.call('HEXISTS', graphKey, toKey) == 0 then
				redis.call('HSET', graphKey, toKey, '[]')
				redis.call('HSETNX', inDegreeKey, toKey, 0)
			end

			-- Get current successors of 'from'
			local successorsJson = redis.call('HGET', graphKey, fromKey)
			local successors = cjson.decode(successorsJson)

			-- Check if edge already exists
			local edgeExists = false
			for _, s in ipairs(successors) do
				if s == toKey then
					edgeExists = true
					break
				end
			end

			if edgeExists then
				succeeded = succeeded + 1
			elseif hasCycle(fromKey, toKey) then
				failed = failed + 1
				-- Use string key to ensure cjson encodes as object not array
				errors[tostring(i)] = "cycle detected"
			else
				-- Add edge
				table.insert(successors, toKey)
				redis.call('HSET', graphKey, fromKey, cjson.encode(successors))
				redis.call('HINCRBY', inDegreeKey, toKey, 1)
				succeeded = succeeded + 1
			end
		end
	end

	return cjson.encode({succeeded = succeeded, failed = failed, errors = errors})
`)

// AddEdges adds multiple edges to the graph in a single batch operation.
// This is more efficient than calling AddEdge multiple times.
// If cycle detection is enabled (default), it will detect cycles for each edge.
func (d *RedisDAG[T]) AddEdges(ctx context.Context, edges []Edge[T], opts ...BatchOption) (*BatchResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if len(edges) == 0 {
		return &BatchResult{Succeeded: 0, Failed: 0, Errors: make(map[int]error)}, nil
	}

	options := defaultBatchOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Execute with or without lock
	if options.useLock && d.lockerGenerator != nil {
		var result *BatchResult
		err := d.withLock(ctx, func(ctx context.Context) error {
			var err error
			result, err = d.addEdgesBatch(ctx, edges, options)
			return err
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	return d.addEdgesBatch(ctx, edges, options)
}

func (d *RedisDAG[T]) addEdgesBatch(ctx context.Context, edges []Edge[T], options *batchOptions) (*BatchResult, error) {
	result := &BatchResult{
		Errors: make(map[int]error),
	}

	// Process in batches
	for i := 0; i < len(edges); i += options.batchSize {
		end := min(i+options.batchSize, len(edges))
		batch := edges[i:end]

		// Prepare arguments for Lua script
		args := make([]any, 0, 2+len(batch)*2)
		if options.skipCycleDetection {
			args = append(args, "1")
		} else {
			args = append(args, "0")
		}
		args = append(args, len(batch))

		// Marshal edges
		marshalErrors := make(map[int]error)
		for j, edge := range batch {
			fromKey, err := d.marshal(edge.From)
			if err != nil {
				marshalErrors[i+j] = fmt.Errorf("failed to marshal 'from' vertex: %w", err)
				continue
			}
			toKey, err := d.marshal(edge.To)
			if err != nil {
				marshalErrors[i+j] = fmt.Errorf("failed to marshal 'to' vertex: %w", err)
				continue
			}
			args = append(args, fromKey, toKey)
		}

		// Add marshal errors to result
		for idx, err := range marshalErrors {
			result.Errors[idx] = err
			result.Failed++
		}

		// If all edges in this batch had marshal errors, skip Lua execution
		if len(marshalErrors) == len(batch) {
			continue
		}

		// Execute Lua script
		luaResultJSON, err := luaAddEdgesBatch.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey()}, args...).Text()
		if err != nil {
			// Script failed, mark remaining as failed
			for j := range batch {
				if _, exists := result.Errors[i+j]; !exists {
					result.Errors[i+j] = fmt.Errorf("lua script failed: %w", err)
					result.Failed++
				}
			}
			continue
		}

		// Parse Lua result - errors can be empty array [] or object {}
		var luaResult struct {
			Succeeded int             `json:"succeeded"`
			Failed    int             `json:"failed"`
			Errors    json.RawMessage `json:"errors"`
		}
		if err := json.Unmarshal([]byte(luaResultJSON), &luaResult); err != nil {
			// Failed to parse result, mark all as unknown
			for j := range batch {
				if _, exists := result.Errors[i+j]; !exists {
					result.Errors[i+j] = fmt.Errorf("failed to parse lua result: %w", err)
					result.Failed++
				}
			}
			continue
		}

		// Parse errors - try as map first, if fails it's an empty array
		var errorsMap map[string]string
		if err := json.Unmarshal(luaResult.Errors, &errorsMap); err != nil {
			// Likely empty array, ignore
			errorsMap = make(map[string]string)
		}

		// Add Lua errors to result
		for idxStr, errMsg := range errorsMap {
			var idx int
			if _, err := fmt.Sscanf(idxStr, "%d", &idx); err != nil {
				continue
			}
			actualIdx := i + idx - 1 // Lua indices are 1-based
			if _, exists := result.Errors[actualIdx]; !exists {
				if errMsg == "cycle detected" {
					result.Errors[actualIdx] = ErrCycleDetected
				} else {
					result.Errors[actualIdx] = fmt.Errorf(errMsg)
				}
			}
		}

		result.Succeeded += luaResult.Succeeded
		result.Failed += luaResult.Failed
	}

	return result, nil
}

// DelVertices removes multiple vertices and their associated edges.
// This is more efficient than calling DelVertex multiple times.
func (d *RedisDAG[T]) DelVertices(ctx context.Context, vertices []T, opts ...BatchOption) (*BatchResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if len(vertices) == 0 {
		return &BatchResult{Succeeded: 0, Failed: 0, Errors: make(map[int]error)}, nil
	}

	options := defaultBatchOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Execute with or without lock
	if options.useLock && d.lockerGenerator != nil {
		var result *BatchResult
		err := d.withLock(ctx, func(ctx context.Context) error {
			var err error
			result, err = d.delVerticesBatch(ctx, vertices)
			return err
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	return d.delVerticesBatch(ctx, vertices)
}

func (d *RedisDAG[T]) delVerticesBatch(ctx context.Context, vertices []T) (*BatchResult, error) {
	result := &BatchResult{
		Errors: make(map[int]error),
	}

	// Process each vertex using the existing Lua script
	// (batch delete would require a more complex Lua script)
	for i, vertex := range vertices {
		key, err := d.marshal(vertex)
		if err != nil {
			result.Errors[i] = fmt.Errorf("failed to marshal vertex: %w", err)
			result.Failed++
			continue
		}

		// Check if vertex exists
		exists, err := d.client.HExists(ctx, d.graphKey(), key).Result()
		if err != nil {
			result.Errors[i] = fmt.Errorf("failed to check vertex existence: %w", err)
			result.Failed++
			continue
		}
		if !exists {
			result.Errors[i] = ErrVertexNotFound
			result.Failed++
			continue
		}

		// Use Lua script to atomically remove vertex
		_, err = luaDelVertex.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey(), d.queueKey()}, key).Result()
		if err != nil {
			result.Errors[i] = fmt.Errorf("failed to delete vertex: %w", err)
			result.Failed++
			continue
		}

		result.Succeeded++
	}

	return result, nil
}

// DelEdges removes multiple edges from the graph.
// This is more efficient than calling DelEdge multiple times.
func (d *RedisDAG[T]) DelEdges(ctx context.Context, edges []Edge[T], opts ...BatchOption) (*BatchResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if len(edges) == 0 {
		return &BatchResult{Succeeded: 0, Failed: 0, Errors: make(map[int]error)}, nil
	}

	options := defaultBatchOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Execute with or without lock
	if options.useLock && d.lockerGenerator != nil {
		var result *BatchResult
		err := d.withLock(ctx, func(ctx context.Context) error {
			var err error
			result, err = d.delEdgesBatch(ctx, edges)
			return err
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	return d.delEdgesBatch(ctx, edges)
}

func (d *RedisDAG[T]) delEdgesBatch(ctx context.Context, edges []Edge[T]) (*BatchResult, error) {
	result := &BatchResult{
		Errors: make(map[int]error),
	}

	// Process each edge using the existing Lua script
	for i, edge := range edges {
		fromKey, err := d.marshal(edge.From)
		if err != nil {
			result.Errors[i] = fmt.Errorf("failed to marshal 'from' vertex: %w", err)
			result.Failed++
			continue
		}
		toKey, err := d.marshal(edge.To)
		if err != nil {
			result.Errors[i] = fmt.Errorf("failed to marshal 'to' vertex: %w", err)
			result.Failed++
			continue
		}

		luaResult, err := luaDelEdge.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey()}, fromKey, toKey).Int()
		if err != nil {
			result.Errors[i] = fmt.Errorf("failed to delete edge: %w", err)
			result.Failed++
			continue
		}

		if luaResult == -1 {
			result.Errors[i] = ErrEdgeNotFound
			result.Failed++
			continue
		}

		result.Succeeded++
	}

	return result, nil
}

// SetLockerGenerator sets the locker generator for distributed locking.
// This is optional and only needed when using batch operations with locking.
func (d *RedisDAG[T]) SetLockerGenerator(gen locker.SyncLockerGenerator) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lockerGenerator = gen
}
