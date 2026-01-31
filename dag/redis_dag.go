package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Redis key prefixes
	keyPrefixGraph     = "dag:graph:"     // Hash: vertex -> JSON array of successors
	keyPrefixInDegree  = "dag:indegree:"  // Hash: vertex -> in-degree count
	keyPrefixQueue     = "dag:queue:"     // List: vertices ready to process
	keyPrefixProcessed = "dag:processed:" // Set: processed vertices (to avoid re-processing)
)

var _ DAG[string] = (*RedisDAG[string])(nil)

// RedisDAG is a Redis-backed implementation of DAG.
// It stores the graph structure in Redis, allowing distributed access.
type RedisDAG[T comparable] struct {
	name   string
	opts   *Options
	client redis.Cmdable

	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	pipeline chan T

	// Serialization functions
	marshal   func(T) (string, error)
	unmarshal func(string) (T, error)
}

// RedisDAGConfig configures RedisDAG behavior.
type RedisDAGConfig[T comparable] struct {
	// Marshal converts vertex to string for Redis storage.
	// Default uses JSON marshaling.
	Marshal func(T) (string, error)

	// Unmarshal converts string from Redis to vertex.
	// Default uses JSON unmarshaling.
	Unmarshal func(string) (T, error)
}

// NewRedisDAG creates a new Redis-backed DAG.
func NewRedisDAG[T comparable](client redis.Cmdable, name string, config *RedisDAGConfig[T], opts ...Option) (*RedisDAG[T], error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	d := &RedisDAG[T]{
		name:    name,
		opts:    options,
		client:  client,
		closeCh: make(chan struct{}),
	}

	// Set default serialization functions
	if config != nil && config.Marshal != nil {
		d.marshal = config.Marshal
	} else {
		d.marshal = func(v T) (string, error) {
			b, err := json.Marshal(v)
			return string(b), err
		}
	}

	if config != nil && config.Unmarshal != nil {
		d.unmarshal = config.Unmarshal
	} else {
		d.unmarshal = func(s string) (T, error) {
			var v T
			err := json.Unmarshal([]byte(s), &v)
			return v, err
		}
	}

	return d, nil
}

// Redis key helpers
func (d *RedisDAG[T]) graphKey() string {
	return keyPrefixGraph + d.name
}

func (d *RedisDAG[T]) inDegreeKey() string {
	return keyPrefixInDegree + d.name
}

func (d *RedisDAG[T]) queueKey() string {
	return keyPrefixQueue + d.name
}

func (d *RedisDAG[T]) processedKey() string {
	return keyPrefixProcessed + d.name
}

func (d *RedisDAG[T]) AddVertex(ctx context.Context, vertex T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return err
	}

	// Check if vertex already exists
	exists, err := d.client.HExists(ctx, d.graphKey(), key).Result()
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Add vertex with empty successor list
	pipe := d.client.Pipeline()
	pipe.HSet(ctx, d.graphKey(), key, "[]")
	pipe.HSetNX(ctx, d.inDegreeKey(), key, 0)
	_, err = pipe.Exec(ctx)

	return err
}

func (d *RedisDAG[T]) AddEdge(ctx context.Context, from, to T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	// Skip self-loops
	if from == to {
		return nil
	}

	fromKey, err := d.marshal(from)
	if err != nil {
		return err
	}
	toKey, err := d.marshal(to)
	if err != nil {
		return err
	}

	// Check for cycle using Lua script for atomicity
	script := redis.NewScript(`
		local graphKey = KEYS[1]
		local inDegreeKey = KEYS[2]
		local fromKey = ARGV[1]
		local toKey = ARGV[2]

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
		for _, s in ipairs(successors) do
			if s == toKey then
				return 0 -- Edge already exists
			end
		end

		-- Simple cycle check: BFS from 'to' to see if we can reach 'from'
		local visited = {}
		local queue = {toKey}
		while #queue > 0 do
			local current = table.remove(queue, 1)
			if current == fromKey then
				return -1 -- Cycle detected
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

		-- Add edge
		table.insert(successors, toKey)
		redis.call('HSET', graphKey, fromKey, cjson.encode(successors))
		redis.call('HINCRBY', inDegreeKey, toKey, 1)

		return 1 -- Success
	`)

	result, err := script.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey()}, fromKey, toKey).Int()
	if err != nil {
		return err
	}

	if result == -1 {
		return ErrCycleDetected
	}

	return nil
}

func (d *RedisDAG[T]) DelVertex(ctx context.Context, vertex T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return err
	}

	// Check if vertex exists
	exists, err := d.client.HExists(ctx, d.graphKey(), key).Result()
	if err != nil {
		return err
	}
	if !exists {
		return ErrVertexNotFound
	}

	// Use Lua script to atomically remove vertex and update all references
	script := redis.NewScript(`
		local graphKey = KEYS[1]
		local inDegreeKey = KEYS[2]
		local queueKey = KEYS[3]
		local vertexKey = ARGV[1]

		-- Get successors and decrement their in-degree
		local successorsJson = redis.call('HGET', graphKey, vertexKey)
		if successorsJson then
			local successors = cjson.decode(successorsJson)
			for _, s in ipairs(successors) do
				redis.call('HINCRBY', inDegreeKey, s, -1)
			end
		end

		-- Remove vertex from all predecessor lists
		local allVertices = redis.call('HKEYS', graphKey)
		for _, v in ipairs(allVertices) do
			if v ~= vertexKey then
				local vSuccessorsJson = redis.call('HGET', graphKey, v)
				if vSuccessorsJson then
					local vSuccessors = cjson.decode(vSuccessorsJson)
					local newSuccessors = {}
					for _, s in ipairs(vSuccessors) do
						if s ~= vertexKey then
							table.insert(newSuccessors, s)
						end
					end
					if #newSuccessors ~= #vSuccessors then
						redis.call('HSET', graphKey, v, cjson.encode(newSuccessors))
					end
				end
			end
		end

		-- Remove vertex
		redis.call('HDEL', graphKey, vertexKey)
		redis.call('HDEL', inDegreeKey, vertexKey)
		redis.call('LREM', queueKey, 0, vertexKey)

		return 1
	`)

	_, err = script.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey(), d.queueKey()}, key).Result()
	return err
}

func (d *RedisDAG[T]) DelEdge(ctx context.Context, from, to T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	fromKey, err := d.marshal(from)
	if err != nil {
		return err
	}
	toKey, err := d.marshal(to)
	if err != nil {
		return err
	}

	script := redis.NewScript(`
		local graphKey = KEYS[1]
		local inDegreeKey = KEYS[2]
		local fromKey = ARGV[1]
		local toKey = ARGV[2]

		-- Check if 'from' vertex exists
		local successorsJson = redis.call('HGET', graphKey, fromKey)
		if not successorsJson then
			return -1 -- Edge not found
		end

		local successors = cjson.decode(successorsJson)
		local newSuccessors = {}
		local found = false

		for _, s in ipairs(successors) do
			if s == toKey then
				found = true
			else
				table.insert(newSuccessors, s)
			end
		end

		if not found then
			return -1 -- Edge not found
		end

		redis.call('HSET', graphKey, fromKey, cjson.encode(newSuccessors))
		redis.call('HINCRBY', inDegreeKey, toKey, -1)

		return 1
	`)

	result, err := script.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey()}, fromKey, toKey).Int()
	if err != nil {
		return err
	}

	if result == -1 {
		return ErrEdgeNotFound
	}

	return nil
}

func (d *RedisDAG[T]) HasVertex(ctx context.Context, vertex T) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return false, ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return false, err
	}

	return d.client.HExists(ctx, d.graphKey(), key).Result()
}

func (d *RedisDAG[T]) HasEdge(ctx context.Context, from, to T) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return false, ErrDAGClosed
	}

	fromKey, err := d.marshal(from)
	if err != nil {
		return false, err
	}
	toKey, err := d.marshal(to)
	if err != nil {
		return false, err
	}

	successorsJson, err := d.client.HGet(ctx, d.graphKey(), fromKey).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	var successors []string
	if err := json.Unmarshal([]byte(successorsJson), &successors); err != nil {
		return false, err
	}

	for _, s := range successors {
		if s == toKey {
			return true, nil
		}
	}

	return false, nil
}

func (d *RedisDAG[T]) InDegree(ctx context.Context, vertex T) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return 0, err
	}

	// Check if vertex exists
	exists, err := d.client.HExists(ctx, d.graphKey(), key).Result()
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, ErrVertexNotFound
	}

	inDegree, err := d.client.HGet(ctx, d.inDegreeKey(), key).Int()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}

	return inDegree, nil
}

func (d *RedisDAG[T]) OutDegree(ctx context.Context, vertex T) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return 0, err
	}

	successorsJson, err := d.client.HGet(ctx, d.graphKey(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrVertexNotFound
		}
		return 0, err
	}

	var successors []string
	if err := json.Unmarshal([]byte(successorsJson), &successors); err != nil {
		return 0, err
	}

	return len(successors), nil
}

func (d *RedisDAG[T]) VertexCount(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	count, err := d.client.HLen(ctx, d.graphKey()).Result()
	return int(count), err
}

func (d *RedisDAG[T]) EdgeCount(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	allSuccessors, err := d.client.HVals(ctx, d.graphKey()).Result()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, successorsJson := range allSuccessors {
		var successors []string
		if err := json.Unmarshal([]byte(successorsJson), &successors); err != nil {
			continue
		}
		count += len(successors)
	}

	return count, nil
}

func (d *RedisDAG[T]) Vertices(ctx context.Context) ([]T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	keys, err := d.client.HKeys(ctx, d.graphKey()).Result()
	if err != nil {
		return nil, err
	}

	vertices := make([]T, 0, len(keys))
	for _, k := range keys {
		v, err := d.unmarshal(k)
		if err != nil {
			continue
		}
		vertices = append(vertices, v)
	}

	return vertices, nil
}

func (d *RedisDAG[T]) Successors(ctx context.Context, vertex T) ([]T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return nil, err
	}

	successorsJson, err := d.client.HGet(ctx, d.graphKey(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrVertexNotFound
		}
		return nil, err
	}

	var successorKeys []string
	if err := json.Unmarshal([]byte(successorsJson), &successorKeys); err != nil {
		return nil, err
	}

	successors := make([]T, 0, len(successorKeys))
	for _, k := range successorKeys {
		v, err := d.unmarshal(k)
		if err != nil {
			continue
		}
		successors = append(successors, v)
	}

	return successors, nil
}

func (d *RedisDAG[T]) Predecessors(ctx context.Context, vertex T) ([]T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	key, err := d.marshal(vertex)
	if err != nil {
		return nil, err
	}

	// Check if vertex exists
	exists, err := d.client.HExists(ctx, d.graphKey(), key).Result()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrVertexNotFound
	}

	// Scan all vertices to find predecessors
	allData, err := d.client.HGetAll(ctx, d.graphKey()).Result()
	if err != nil {
		return nil, err
	}

	var predecessors []T
	for vKey, successorsJson := range allData {
		var successorKeys []string
		if err := json.Unmarshal([]byte(successorsJson), &successorKeys); err != nil {
			continue
		}

		for _, s := range successorKeys {
			if s == key {
				v, err := d.unmarshal(vKey)
				if err != nil {
					continue
				}
				predecessors = append(predecessors, v)
				break
			}
		}
	}

	return predecessors, nil
}

func (d *RedisDAG[T]) Pipeline(ctx context.Context) (<-chan T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if d.pipeline != nil {
		return d.pipeline, nil
	}

	d.pipeline = make(chan T, d.opts.BufferSize)

	// Clear processed set for fresh start
	d.client.Del(ctx, d.processedKey())

	// Goroutine to find vertices with in-degree 0 and add to queue
	go d.pollReadyVertices(ctx)

	// Goroutine to process queue and emit to pipeline
	go d.processQueue(ctx)

	return d.pipeline, nil
}

func (d *RedisDAG[T]) pollReadyVertices(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(d.opts.PollInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.closeCh:
			return
		case <-ticker.C:
			// Find vertices with in-degree 0 that haven't been processed
			script := redis.NewScript(`
				local graphKey = KEYS[1]
				local inDegreeKey = KEYS[2]
				local queueKey = KEYS[3]
				local processedKey = KEYS[4]

				local vertices = redis.call('HKEYS', graphKey)
				local added = 0

				for _, v in ipairs(vertices) do
					local inDegree = tonumber(redis.call('HGET', inDegreeKey, v) or 0)
					local inQueue = redis.call('LPOS', queueKey, v)
					local processed = redis.call('SISMEMBER', processedKey, v)

					if inDegree == 0 and not inQueue and processed == 0 then
						redis.call('RPUSH', queueKey, v)
						added = added + 1
					end
				end

				return added
			`)

			script.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey(), d.queueKey(), d.processedKey()})
		}
	}
}

func (d *RedisDAG[T]) processQueue(ctx context.Context) {
	defer close(d.pipeline)

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.closeCh:
			return
		default:
			// Try to pop from queue using BLPOP with timeout
			result, err := d.client.BLPop(ctx, time.Duration(d.opts.PollInterval)*time.Millisecond, d.queueKey()).Result()
			if err != nil {
				if err == redis.Nil {
					continue
				}
				// Check if context cancelled
				select {
				case <-ctx.Done():
					return
				case <-d.closeCh:
					return
				default:
					time.Sleep(time.Duration(d.opts.PollInterval) * time.Millisecond)
					continue
				}
			}

			if len(result) < 2 {
				continue
			}

			vertexKey := result[1]

			// Mark as processed
			d.client.SAdd(ctx, d.processedKey(), vertexKey)

			// Update successors' in-degree and remove vertex
			script := redis.NewScript(`
				local graphKey = KEYS[1]
				local inDegreeKey = KEYS[2]
				local vertexKey = ARGV[1]

				-- Get successors and decrement their in-degree
				local successorsJson = redis.call('HGET', graphKey, vertexKey)
				if successorsJson then
					local successors = cjson.decode(successorsJson)
					for _, s in ipairs(successors) do
						redis.call('HINCRBY', inDegreeKey, s, -1)
					end
				end

				-- Remove vertex from graph
				redis.call('HDEL', graphKey, vertexKey)
				redis.call('HDEL', inDegreeKey, vertexKey)

				return 1
			`)

			script.Run(ctx, d.client, []string{d.graphKey(), d.inDegreeKey()}, vertexKey)

			// Emit vertex
			vertex, err := d.unmarshal(vertexKey)
			if err != nil {
				continue
			}

			select {
			case d.pipeline <- vertex:
			case <-ctx.Done():
				return
			case <-d.closeCh:
				return
			}
		}
	}
}

func (d *RedisDAG[T]) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true
	close(d.closeCh)

	return nil
}

// Cleanup removes all Redis keys associated with this DAG.
func (d *RedisDAG[T]) Cleanup(ctx context.Context) error {
	return d.client.Del(ctx, d.graphKey(), d.inDegreeKey(), d.queueKey(), d.processedKey()).Err()
}

// RedisDAGFactory creates RedisDAG instances.
type RedisDAGFactory[T comparable] struct {
	client redis.Cmdable
	config *RedisDAGConfig[T]
	mu     sync.Mutex
	dags   map[string]*RedisDAG[T]
}

// NewRedisDAGFactory creates a new RedisDAGFactory.
func NewRedisDAGFactory[T comparable](client redis.Cmdable, config *RedisDAGConfig[T]) *RedisDAGFactory[T] {
	return &RedisDAGFactory[T]{
		client: client,
		config: config,
		dags:   make(map[string]*RedisDAG[T]),
	}
}

func (f *RedisDAGFactory[T]) Create(ctx context.Context, name string, opts ...Option) (DAG[T], error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if dag, exists := f.dags[name]; exists {
		return dag, nil
	}

	dag, err := NewRedisDAG(f.client, name, f.config, opts...)
	if err != nil {
		return nil, err
	}

	f.dags[name] = dag

	return dag, nil
}
