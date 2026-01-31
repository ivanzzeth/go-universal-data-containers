package dag

import (
	"context"
	"errors"
)

var (
	ErrVertexNotFound = errors.New("vertex not found")
	ErrEdgeNotFound   = errors.New("edge not found")
	ErrCycleDetected  = errors.New("cycle detected: adding this edge would create a cycle")
	ErrDAGClosed      = errors.New("dag is closed")
)

// DAG represents a Directed Acyclic Graph data structure.
// It supports adding vertices (nodes) and edges (dependencies),
// and provides a Pipeline for consuming vertices in topological order.
//
// The generic type T represents the vertex identifier type.
// NOTE: It's thread-safe.
type DAG[T comparable] interface {
	// AddVertex adds a vertex to the graph.
	// If the vertex already exists, this is a no-op.
	AddVertex(ctx context.Context, vertex T) error

	// AddEdge adds a directed edge from `from` to `to`.
	// This means `from` must be processed before `to`.
	// Both vertices are created if they don't exist.
	// Returns ErrCycleDetected if adding this edge would create a cycle.
	AddEdge(ctx context.Context, from, to T) error

	// DelVertex removes a vertex and all its associated edges.
	// Returns ErrVertexNotFound if vertex doesn't exist.
	DelVertex(ctx context.Context, vertex T) error

	// DelEdge removes the edge from `from` to `to`.
	// Returns ErrEdgeNotFound if the edge doesn't exist.
	DelEdge(ctx context.Context, from, to T) error

	// HasVertex checks if a vertex exists in the graph.
	HasVertex(ctx context.Context, vertex T) (bool, error)

	// HasEdge checks if an edge exists from `from` to `to`.
	HasEdge(ctx context.Context, from, to T) (bool, error)

	// InDegree returns the in-degree of a vertex (number of incoming edges).
	// Returns ErrVertexNotFound if vertex doesn't exist.
	InDegree(ctx context.Context, vertex T) (int, error)

	// OutDegree returns the out-degree of a vertex (number of outgoing edges).
	// Returns ErrVertexNotFound if vertex doesn't exist.
	OutDegree(ctx context.Context, vertex T) (int, error)

	// VertexCount returns the total number of vertices in the graph.
	VertexCount(ctx context.Context) (int, error)

	// EdgeCount returns the total number of edges in the graph.
	EdgeCount(ctx context.Context) (int, error)

	// Vertices returns all vertices in the graph.
	Vertices(ctx context.Context) ([]T, error)

	// Successors returns all vertices that have an edge from the given vertex.
	// Returns ErrVertexNotFound if vertex doesn't exist.
	Successors(ctx context.Context, vertex T) ([]T, error)

	// Predecessors returns all vertices that have an edge to the given vertex.
	// Returns ErrVertexNotFound if vertex doesn't exist.
	Predecessors(ctx context.Context, vertex T) ([]T, error)

	// Pipeline returns a channel that emits vertices in topological order.
	// Vertices with in-degree 0 are emitted first.
	// Once a vertex is emitted, it is removed from the graph.
	// The channel is closed when the graph is empty or Close() is called.
	Pipeline(ctx context.Context) (<-chan T, error)

	// Close releases resources and stops the Pipeline.
	Close() error
}

// Factory creates DAG instances.
type Factory[T comparable] interface {
	// Create creates a new DAG with the given name.
	// If a DAG with the same name already exists, it returns the existing one.
	Create(ctx context.Context, name string, opts ...Option) (DAG[T], error)
}

// Option configures DAG behavior.
type Option func(*Options)

// Options holds DAG configuration.
type Options struct {
	// BufferSize is the buffer size for the Pipeline channel.
	// Default is 0 (unbuffered).
	BufferSize int

	// PollInterval is the interval for polling vertices with in-degree 0.
	// Default is 100ms.
	PollInterval int
}

// DefaultOptions returns default options.
func DefaultOptions() *Options {
	return &Options{
		BufferSize:   0,
		PollInterval: 100,
	}
}

// WithBufferSize sets the buffer size for the Pipeline channel.
func WithBufferSize(size int) Option {
	return func(o *Options) {
		o.BufferSize = size
	}
}

// WithPollInterval sets the poll interval in milliseconds.
func WithPollInterval(ms int) Option {
	return func(o *Options) {
		o.PollInterval = ms
	}
}
