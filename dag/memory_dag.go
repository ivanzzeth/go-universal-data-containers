package dag

import (
	"context"
	"sync"
	"time"
)

var _ DAG[string] = (*MemoryDAG[string])(nil)

// MemoryDAG is an in-memory implementation of DAG.
type MemoryDAG[T comparable] struct {
	name string
	opts *Options

	// graph stores adjacency list: vertex -> set of successors
	graph map[T]map[T]struct{}
	// inDegree stores the in-degree of each vertex
	inDegree map[T]int

	// queue stores vertices ready to be processed (in-degree = 0)
	queue []T
	// inQueue tracks which vertices are in the queue
	inQueue map[T]bool

	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	pipeline chan T
}

// NewMemoryDAG creates a new in-memory DAG.
func NewMemoryDAG[T comparable](name string, opts ...Option) *MemoryDAG[T] {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	return &MemoryDAG[T]{
		name:     name,
		opts:     options,
		graph:    make(map[T]map[T]struct{}),
		inDegree: make(map[T]int),
		inQueue:  make(map[T]bool),
		closeCh:  make(chan struct{}),
	}
}

func (d *MemoryDAG[T]) AddVertex(ctx context.Context, vertex T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	if _, exists := d.graph[vertex]; !exists {
		d.graph[vertex] = make(map[T]struct{})
		d.inDegree[vertex] = 0
	}

	return nil
}

func (d *MemoryDAG[T]) AddEdge(ctx context.Context, from, to T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	// Skip self-loops
	if from == to {
		return nil
	}

	// Ensure both vertices exist
	if _, exists := d.graph[from]; !exists {
		d.graph[from] = make(map[T]struct{})
		d.inDegree[from] = 0
	}
	if _, exists := d.graph[to]; !exists {
		d.graph[to] = make(map[T]struct{})
		d.inDegree[to] = 0
	}

	// Check if edge already exists
	if _, exists := d.graph[from][to]; exists {
		return nil
	}

	// Check for cycle: if adding this edge creates a path from `to` to `from`
	if d.hasPath(to, from) {
		return ErrCycleDetected
	}

	// Add edge
	d.graph[from][to] = struct{}{}
	d.inDegree[to]++

	return nil
}

// hasPath checks if there's a path from start to end using BFS.
// Must be called with lock held.
func (d *MemoryDAG[T]) hasPath(start, end T) bool {
	if start == end {
		return true
	}

	visited := make(map[T]bool)
	queue := []T{start}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if visited[current] {
			continue
		}
		visited[current] = true

		for successor := range d.graph[current] {
			if successor == end {
				return true
			}
			if !visited[successor] {
				queue = append(queue, successor)
			}
		}
	}

	return false
}

func (d *MemoryDAG[T]) DelVertex(ctx context.Context, vertex T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	if _, exists := d.graph[vertex]; !exists {
		return ErrVertexNotFound
	}

	// Remove all outgoing edges
	for successor := range d.graph[vertex] {
		d.inDegree[successor]--
	}

	// Remove all incoming edges
	for v := range d.graph {
		if _, exists := d.graph[v][vertex]; exists {
			delete(d.graph[v], vertex)
		}
	}

	// Remove vertex
	delete(d.graph, vertex)
	delete(d.inDegree, vertex)
	delete(d.inQueue, vertex)

	// Remove from queue if present
	for i, v := range d.queue {
		if v == vertex {
			d.queue = append(d.queue[:i], d.queue[i+1:]...)
			break
		}
	}

	return nil
}

func (d *MemoryDAG[T]) DelEdge(ctx context.Context, from, to T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDAGClosed
	}

	if _, exists := d.graph[from]; !exists {
		return ErrEdgeNotFound
	}

	if _, exists := d.graph[from][to]; !exists {
		return ErrEdgeNotFound
	}

	delete(d.graph[from], to)
	d.inDegree[to]--

	return nil
}

func (d *MemoryDAG[T]) HasVertex(ctx context.Context, vertex T) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return false, ErrDAGClosed
	}

	_, exists := d.graph[vertex]
	return exists, nil
}

func (d *MemoryDAG[T]) HasEdge(ctx context.Context, from, to T) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return false, ErrDAGClosed
	}

	if _, exists := d.graph[from]; !exists {
		return false, nil
	}

	_, exists := d.graph[from][to]
	return exists, nil
}

func (d *MemoryDAG[T]) InDegree(ctx context.Context, vertex T) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	if _, exists := d.graph[vertex]; !exists {
		return 0, ErrVertexNotFound
	}

	return d.inDegree[vertex], nil
}

func (d *MemoryDAG[T]) OutDegree(ctx context.Context, vertex T) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	successors, exists := d.graph[vertex]
	if !exists {
		return 0, ErrVertexNotFound
	}

	return len(successors), nil
}

func (d *MemoryDAG[T]) VertexCount(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	return len(d.graph), nil
}

func (d *MemoryDAG[T]) EdgeCount(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, ErrDAGClosed
	}

	count := 0
	for _, successors := range d.graph {
		count += len(successors)
	}

	return count, nil
}

func (d *MemoryDAG[T]) Vertices(ctx context.Context) ([]T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	vertices := make([]T, 0, len(d.graph))
	for v := range d.graph {
		vertices = append(vertices, v)
	}

	return vertices, nil
}

func (d *MemoryDAG[T]) Successors(ctx context.Context, vertex T) ([]T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	successors, exists := d.graph[vertex]
	if !exists {
		return nil, ErrVertexNotFound
	}

	result := make([]T, 0, len(successors))
	for s := range successors {
		result = append(result, s)
	}

	return result, nil
}

func (d *MemoryDAG[T]) Predecessors(ctx context.Context, vertex T) ([]T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if _, exists := d.graph[vertex]; !exists {
		return nil, ErrVertexNotFound
	}

	var result []T
	for v, successors := range d.graph {
		if _, exists := successors[vertex]; exists {
			result = append(result, v)
		}
	}

	return result, nil
}

func (d *MemoryDAG[T]) Pipeline(ctx context.Context) (<-chan T, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDAGClosed
	}

	if d.pipeline != nil {
		return d.pipeline, nil
	}

	d.pipeline = make(chan T, d.opts.BufferSize)

	// Goroutine to find vertices with in-degree 0 and add to queue
	go d.pollReadyVertices(ctx)

	// Goroutine to process queue and emit to pipeline
	go d.processQueue(ctx)

	return d.pipeline, nil
}

func (d *MemoryDAG[T]) pollReadyVertices(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(d.opts.PollInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.closeCh:
			return
		case <-ticker.C:
			d.mu.Lock()
			for vertex, inDeg := range d.inDegree {
				if inDeg == 0 && !d.inQueue[vertex] {
					d.queue = append(d.queue, vertex)
					d.inQueue[vertex] = true
				}
			}
			d.mu.Unlock()
		}
	}
}

func (d *MemoryDAG[T]) processQueue(ctx context.Context) {
	defer close(d.pipeline)

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.closeCh:
			return
		default:
			d.mu.Lock()

			if len(d.queue) == 0 {
				d.mu.Unlock()
				time.Sleep(time.Duration(d.opts.PollInterval) * time.Millisecond)
				continue
			}

			// Dequeue
			vertex := d.queue[0]
			d.queue = d.queue[1:]

			// Update successors' in-degree
			for successor := range d.graph[vertex] {
				d.inDegree[successor]--
				if d.inDegree[successor] == 0 && !d.inQueue[successor] {
					d.queue = append(d.queue, successor)
					d.inQueue[successor] = true
				}
			}

			// Remove vertex from graph
			delete(d.graph, vertex)
			delete(d.inDegree, vertex)
			delete(d.inQueue, vertex)

			d.mu.Unlock()

			// Emit vertex
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

func (d *MemoryDAG[T]) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true
	close(d.closeCh)

	return nil
}

// MemoryDAGFactory creates MemoryDAG instances.
type MemoryDAGFactory[T comparable] struct {
	mu   sync.Mutex
	dags map[string]*MemoryDAG[T]
}

// NewMemoryDAGFactory creates a new MemoryDAGFactory.
func NewMemoryDAGFactory[T comparable]() *MemoryDAGFactory[T] {
	return &MemoryDAGFactory[T]{
		dags: make(map[string]*MemoryDAG[T]),
	}
}

func (f *MemoryDAGFactory[T]) Create(ctx context.Context, name string, opts ...Option) (DAG[T], error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if dag, exists := f.dags[name]; exists {
		return dag, nil
	}

	dag := NewMemoryDAG[T](name, opts...)
	f.dags[name] = dag

	return dag, nil
}
