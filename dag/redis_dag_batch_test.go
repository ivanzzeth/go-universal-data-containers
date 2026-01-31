package dag

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBatchTestDAG(t *testing.T) (*miniredis.Miniredis, *RedisDAG[int]) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	dag, err := NewRedisDAG[int](client, "batch-test", nil)
	require.NoError(t, err)

	return mr, dag
}

func TestRedisDAG_AddVertices(t *testing.T) {
	t.Run("add vertices successfully", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.AddVertices(ctx, []int{1, 2, 3, 4, 5})
		assert.NoError(t, err)
		assert.Equal(t, 5, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
		assert.Empty(t, result.Errors)

		// Verify vertices exist
		for i := 1; i <= 5; i++ {
			exists, err := dag.HasVertex(ctx, i)
			assert.NoError(t, err)
			assert.True(t, exists)
		}
	})

	t.Run("add empty vertices", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.AddVertices(ctx, []int{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("add duplicate vertices", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Add initial vertices
		result, err := dag.AddVertices(ctx, []int{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, 3, result.Succeeded)

		// Add again with some duplicates
		result, err = dag.AddVertices(ctx, []int{2, 3, 4, 5})
		assert.NoError(t, err)
		// HSetNX doesn't fail on duplicates, just doesn't overwrite
		assert.Equal(t, 4, result.Succeeded)
	})

	t.Run("closed DAG", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()

		dag.Close()

		result, err := dag.AddVertices(context.Background(), []int{1, 2, 3})
		assert.Equal(t, ErrDAGClosed, err)
		assert.Nil(t, result)
	})

	t.Run("without lock", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.AddVertices(ctx, []int{1, 2, 3}, WithoutBatchLock())
		assert.NoError(t, err)
		assert.Equal(t, 3, result.Succeeded)
	})

	t.Run("with custom batch size", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Add 10 vertices with batch size 3
		vertices := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		result, err := dag.AddVertices(ctx, vertices, WithBatchSize(3))
		assert.NoError(t, err)
		assert.Equal(t, 10, result.Succeeded)
	})
}

func TestRedisDAG_AddEdges(t *testing.T) {
	t.Run("add edges successfully", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		edges := []Edge[int]{
			{From: 1, To: 2},
			{From: 2, To: 3},
			{From: 1, To: 3},
		}

		result, err := dag.AddEdges(ctx, edges)
		assert.NoError(t, err)
		assert.Equal(t, 3, result.Succeeded)
		assert.Equal(t, 0, result.Failed)

		// Verify edges exist
		for _, edge := range edges {
			exists, err := dag.HasEdge(ctx, edge.From, edge.To)
			assert.NoError(t, err)
			assert.True(t, exists)
		}
	})

	t.Run("add empty edges", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.AddEdges(ctx, []Edge[int]{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("cycle detection", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Create a chain: 1 -> 2 -> 3
		edges := []Edge[int]{
			{From: 1, To: 2},
			{From: 2, To: 3},
		}
		result, err := dag.AddEdges(ctx, edges)
		assert.NoError(t, err)
		assert.Equal(t, 2, result.Succeeded)

		// Try to add edge that creates cycle: 3 -> 1
		cycleEdges := []Edge[int]{
			{From: 3, To: 1},
		}
		result, err = dag.AddEdges(ctx, cycleEdges)
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 1, result.Failed)
		assert.Equal(t, ErrCycleDetected, result.Errors[0])
	})

	t.Run("skip cycle detection", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Create a chain: 1 -> 2 -> 3
		edges := []Edge[int]{
			{From: 1, To: 2},
			{From: 2, To: 3},
		}
		result, err := dag.AddEdges(ctx, edges)
		assert.NoError(t, err)
		assert.Equal(t, 2, result.Succeeded)

		// Add edge that would create cycle, but skip detection
		cycleEdges := []Edge[int]{
			{From: 3, To: 1},
		}
		result, err = dag.AddEdges(ctx, cycleEdges, WithSkipCycleDetection())
		assert.NoError(t, err)
		assert.Equal(t, 1, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("self-loop skipped", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		edges := []Edge[int]{
			{From: 1, To: 1}, // Self-loop
			{From: 1, To: 2},
		}

		result, err := dag.AddEdges(ctx, edges)
		assert.NoError(t, err)
		assert.Equal(t, 2, result.Succeeded) // Both succeed, self-loop is no-op
	})

	t.Run("duplicate edges", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		edges := []Edge[int]{
			{From: 1, To: 2},
			{From: 1, To: 2}, // Duplicate
		}

		result, err := dag.AddEdges(ctx, edges)
		assert.NoError(t, err)
		assert.Equal(t, 2, result.Succeeded) // Second one is no-op but counts as success
	})

	t.Run("closed DAG", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()

		dag.Close()

		result, err := dag.AddEdges(context.Background(), []Edge[int]{{From: 1, To: 2}})
		assert.Equal(t, ErrDAGClosed, err)
		assert.Nil(t, result)
	})
}

func TestRedisDAG_DelVertices(t *testing.T) {
	t.Run("delete vertices successfully", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Setup: add vertices
		_, err := dag.AddVertices(ctx, []int{1, 2, 3, 4, 5})
		require.NoError(t, err)

		// Delete some vertices
		result, err := dag.DelVertices(ctx, []int{2, 4})
		assert.NoError(t, err)
		assert.Equal(t, 2, result.Succeeded)
		assert.Equal(t, 0, result.Failed)

		// Verify deleted
		exists, _ := dag.HasVertex(ctx, 2)
		assert.False(t, exists)
		exists, _ = dag.HasVertex(ctx, 4)
		assert.False(t, exists)

		// Verify remaining
		exists, _ = dag.HasVertex(ctx, 1)
		assert.True(t, exists)
		exists, _ = dag.HasVertex(ctx, 3)
		assert.True(t, exists)
		exists, _ = dag.HasVertex(ctx, 5)
		assert.True(t, exists)
	})

	t.Run("delete non-existent vertices", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.DelVertices(ctx, []int{99, 100})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 2, result.Failed)
		assert.Equal(t, ErrVertexNotFound, result.Errors[0])
		assert.Equal(t, ErrVertexNotFound, result.Errors[1])
	})

	t.Run("delete empty list", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.DelVertices(ctx, []int{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("delete vertices with edges", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Setup: create edges 1 -> 2 -> 3
		edges := []Edge[int]{
			{From: 1, To: 2},
			{From: 2, To: 3},
		}
		_, err := dag.AddEdges(ctx, edges)
		require.NoError(t, err)

		// Delete vertex 2 (middle)
		result, err := dag.DelVertices(ctx, []int{2})
		assert.NoError(t, err)
		assert.Equal(t, 1, result.Succeeded)

		// Verify vertex 2 is gone
		exists, _ := dag.HasVertex(ctx, 2)
		assert.False(t, exists)

		// Verify edges involving 2 are gone
		hasEdge, _ := dag.HasEdge(ctx, 1, 2)
		assert.False(t, hasEdge)
		hasEdge, _ = dag.HasEdge(ctx, 2, 3)
		assert.False(t, hasEdge)
	})
}

func TestRedisDAG_DelEdges(t *testing.T) {
	t.Run("delete edges successfully", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// Setup: add edges
		edges := []Edge[int]{
			{From: 1, To: 2},
			{From: 2, To: 3},
			{From: 1, To: 3},
		}
		_, err := dag.AddEdges(ctx, edges)
		require.NoError(t, err)

		// Delete some edges
		result, err := dag.DelEdges(ctx, []Edge[int]{
			{From: 1, To: 2},
			{From: 2, To: 3},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, result.Succeeded)
		assert.Equal(t, 0, result.Failed)

		// Verify deleted
		hasEdge, _ := dag.HasEdge(ctx, 1, 2)
		assert.False(t, hasEdge)
		hasEdge, _ = dag.HasEdge(ctx, 2, 3)
		assert.False(t, hasEdge)

		// Verify remaining
		hasEdge, _ = dag.HasEdge(ctx, 1, 3)
		assert.True(t, hasEdge)
	})

	t.Run("delete non-existent edges", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.DelEdges(ctx, []Edge[int]{
			{From: 99, To: 100},
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 1, result.Failed)
		assert.Equal(t, ErrEdgeNotFound, result.Errors[0])
	})

	t.Run("delete empty list", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		result, err := dag.DelEdges(ctx, []Edge[int]{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
	})
}

func TestBatchOptions(t *testing.T) {
	t.Run("WithSkipCycleDetection", func(t *testing.T) {
		opts := defaultBatchOptions()
		assert.False(t, opts.skipCycleDetection)

		WithSkipCycleDetection()(opts)
		assert.True(t, opts.skipCycleDetection)
	})

	t.Run("WithBatchLock", func(t *testing.T) {
		opts := defaultBatchOptions()
		opts.useLock = false // Set to false first
		WithBatchLock()(opts)
		assert.True(t, opts.useLock)
	})

	t.Run("WithoutBatchLock", func(t *testing.T) {
		opts := defaultBatchOptions()
		assert.True(t, opts.useLock) // Default is true

		WithoutBatchLock()(opts)
		assert.False(t, opts.useLock)
	})

	t.Run("WithBatchSize", func(t *testing.T) {
		opts := defaultBatchOptions()
		assert.Equal(t, 100, opts.batchSize)

		WithBatchSize(50)(opts)
		assert.Equal(t, 50, opts.batchSize)

		// Invalid size should be ignored
		WithBatchSize(0)(opts)
		assert.Equal(t, 50, opts.batchSize)

		WithBatchSize(-1)(opts)
		assert.Equal(t, 50, opts.batchSize)
	})
}

func TestBatchConcurrency(t *testing.T) {
	t.Run("concurrent AddVertices with lock", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		var wg sync.WaitGroup
		errs := make(chan error, 5)

		for i := range 5 {
			wg.Add(1)
			go func(batch int) {
				defer wg.Done()
				vertices := make([]int, 10)
				for j := range 10 {
					vertices[j] = batch*10 + j
				}
				_, err := dag.AddVertices(ctx, vertices, WithBatchLock())
				if err != nil {
					errs <- err
				}
			}(i)
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify all vertices were added
		count, err := dag.VertexCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 50, count)
	})

	t.Run("concurrent AddEdges with lock", func(t *testing.T) {
		mr, dag := setupBatchTestDAG(t)
		defer mr.Close()
		ctx := context.Background()

		// First add all vertices
		vertices := make([]int, 100)
		for i := range 100 {
			vertices[i] = i
		}
		_, err := dag.AddVertices(ctx, vertices)
		require.NoError(t, err)

		var wg sync.WaitGroup
		errs := make(chan error, 5)

		// Add non-conflicting edges concurrently
		for i := range 5 {
			wg.Add(1)
			go func(batch int) {
				defer wg.Done()
				edges := make([]Edge[int], 10)
				for j := range 10 {
					// Create edges that don't conflict: batch*20+j -> batch*20+j+10
					edges[j] = Edge[int]{From: batch*20 + j, To: batch*20 + j + 10}
				}
				_, err := dag.AddEdges(ctx, edges, WithBatchLock())
				if err != nil {
					errs <- err
				}
			}(i)
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify edges were added
		count, err := dag.EdgeCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 50, count)
	})
}

func TestEdgeType(t *testing.T) {
	t.Run("Edge creation", func(t *testing.T) {
		edge := Edge[int]{From: 1, To: 2}
		assert.Equal(t, 1, edge.From)
		assert.Equal(t, 2, edge.To)
	})

	t.Run("Edge with string type", func(t *testing.T) {
		edge := Edge[string]{From: "a", To: "b"}
		assert.Equal(t, "a", edge.From)
		assert.Equal(t, "b", edge.To)
	})
}

func TestBatchResult(t *testing.T) {
	t.Run("empty result", func(t *testing.T) {
		result := &BatchResult{
			Succeeded: 0,
			Failed:    0,
			Errors:    make(map[int]error),
		}
		assert.Equal(t, 0, result.Succeeded)
		assert.Equal(t, 0, result.Failed)
		assert.Empty(t, result.Errors)
	})

	t.Run("with errors", func(t *testing.T) {
		result := &BatchResult{
			Succeeded: 5,
			Failed:    2,
			Errors: map[int]error{
				3: errors.New("error 1"),
				7: errors.New("error 2"),
			},
		}
		assert.Equal(t, 5, result.Succeeded)
		assert.Equal(t, 2, result.Failed)
		assert.Len(t, result.Errors, 2)
	})
}
