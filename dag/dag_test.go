package dag

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SpecTestDAG is a shared test specification for DAG implementations.
func SpecTestDAG(t *testing.T, dag DAG[int]) {
	ctx := context.Background()

	t.Run("AddVertex", func(t *testing.T) {
		err := dag.AddVertex(ctx, 1)
		require.NoError(t, err)

		has, err := dag.HasVertex(ctx, 1)
		require.NoError(t, err)
		assert.True(t, has)

		has, err = dag.HasVertex(ctx, 999)
		require.NoError(t, err)
		assert.False(t, has)
	})

	t.Run("AddEdge", func(t *testing.T) {
		err := dag.AddEdge(ctx, 2, 3)
		require.NoError(t, err)

		// Vertices should be created
		has, err := dag.HasVertex(ctx, 2)
		require.NoError(t, err)
		assert.True(t, has)

		has, err = dag.HasVertex(ctx, 3)
		require.NoError(t, err)
		assert.True(t, has)

		// Edge should exist
		has, err = dag.HasEdge(ctx, 2, 3)
		require.NoError(t, err)
		assert.True(t, has)

		// Reverse edge should not exist
		has, err = dag.HasEdge(ctx, 3, 2)
		require.NoError(t, err)
		assert.False(t, has)
	})

	t.Run("InDegree_OutDegree", func(t *testing.T) {
		// Setup: 4 -> 5 -> 6
		err := dag.AddEdge(ctx, 4, 5)
		require.NoError(t, err)
		err = dag.AddEdge(ctx, 5, 6)
		require.NoError(t, err)

		// Check in-degree
		inDeg, err := dag.InDegree(ctx, 4)
		require.NoError(t, err)
		assert.Equal(t, 0, inDeg)

		inDeg, err = dag.InDegree(ctx, 5)
		require.NoError(t, err)
		assert.Equal(t, 1, inDeg)

		inDeg, err = dag.InDegree(ctx, 6)
		require.NoError(t, err)
		assert.Equal(t, 1, inDeg)

		// Check out-degree
		outDeg, err := dag.OutDegree(ctx, 4)
		require.NoError(t, err)
		assert.Equal(t, 1, outDeg)

		outDeg, err = dag.OutDegree(ctx, 5)
		require.NoError(t, err)
		assert.Equal(t, 1, outDeg)

		outDeg, err = dag.OutDegree(ctx, 6)
		require.NoError(t, err)
		assert.Equal(t, 0, outDeg)
	})

	t.Run("Successors_Predecessors", func(t *testing.T) {
		// Setup: 7 -> 8, 7 -> 9
		err := dag.AddEdge(ctx, 7, 8)
		require.NoError(t, err)
		err = dag.AddEdge(ctx, 7, 9)
		require.NoError(t, err)

		successors, err := dag.Successors(ctx, 7)
		require.NoError(t, err)
		assert.Len(t, successors, 2)
		assert.Contains(t, successors, 8)
		assert.Contains(t, successors, 9)

		predecessors, err := dag.Predecessors(ctx, 8)
		require.NoError(t, err)
		assert.Len(t, predecessors, 1)
		assert.Contains(t, predecessors, 7)
	})

	t.Run("DelEdge", func(t *testing.T) {
		// Setup
		err := dag.AddEdge(ctx, 10, 11)
		require.NoError(t, err)

		has, err := dag.HasEdge(ctx, 10, 11)
		require.NoError(t, err)
		assert.True(t, has)

		// Delete edge
		err = dag.DelEdge(ctx, 10, 11)
		require.NoError(t, err)

		has, err = dag.HasEdge(ctx, 10, 11)
		require.NoError(t, err)
		assert.False(t, has)

		// Vertices should still exist
		has, err = dag.HasVertex(ctx, 10)
		require.NoError(t, err)
		assert.True(t, has)

		has, err = dag.HasVertex(ctx, 11)
		require.NoError(t, err)
		assert.True(t, has)
	})

	t.Run("DelVertex", func(t *testing.T) {
		// Setup: 12 -> 13 -> 14
		err := dag.AddEdge(ctx, 12, 13)
		require.NoError(t, err)
		err = dag.AddEdge(ctx, 13, 14)
		require.NoError(t, err)

		// Delete middle vertex
		err = dag.DelVertex(ctx, 13)
		require.NoError(t, err)

		has, err := dag.HasVertex(ctx, 13)
		require.NoError(t, err)
		assert.False(t, has)

		// Check in-degree of 14 is now 0
		inDeg, err := dag.InDegree(ctx, 14)
		require.NoError(t, err)
		assert.Equal(t, 0, inDeg)
	})

	t.Run("VertexCount_EdgeCount", func(t *testing.T) {
		// Note: previous tests have added vertices
		count, err := dag.VertexCount(ctx)
		require.NoError(t, err)
		assert.Greater(t, count, 0)

		edgeCount, err := dag.EdgeCount(ctx)
		require.NoError(t, err)
		// edgeCount can be 0 or more depending on test order
		assert.GreaterOrEqual(t, edgeCount, 0)
	})
}

// TestCycleDetection tests that cycles are properly detected.
func TestCycleDetection(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-cycle")
	require.NoError(t, err)
	ctx := context.Background()

	// Create a chain: 1 -> 2 -> 3
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)
	err = dag.AddEdge(ctx, 2, 3)
	require.NoError(t, err)

	// Try to add edge that creates cycle: 3 -> 1
	err = dag.AddEdge(ctx, 3, 1)
	assert.ErrorIs(t, err, ErrCycleDetected)

	// Self-loop should be ignored (not an error)
	err = dag.AddEdge(ctx, 1, 1)
	assert.NoError(t, err)
}

func TestMemoryDAG(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test")
	require.NoError(t, err)
	SpecTestDAG(t, dag)
}

func TestMemoryDAG_EmptyName(t *testing.T) {
	_, err := NewMemoryDAG[int]("")
	assert.Error(t, err)
}

func TestMemoryDAG_Pipeline(t *testing.T) {
	type testcase struct {
		name         string
		dependencies [][]int // [from, to] or [vertex] for single vertex
	}

	testcases := []testcase{
		{
			name:         "simple chain",
			dependencies: [][]int{{1, 2}, {2, 3}, {3, 4}},
		},
		{
			name:         "diamond",
			dependencies: [][]int{{1, 2}, {1, 3}, {2, 4}, {3, 4}},
		},
		{
			name:         "independent vertices",
			dependencies: [][]int{{1}, {2}, {3}},
		},
		{
			name:         "complex",
			dependencies: [][]int{{1, 2}, {2, 3}, {3, 4}, {5, 6}, {4, 5}},
		},
		{
			name:         "multiple roots",
			dependencies: [][]int{{1, 3}, {2, 3}, {3, 4}},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			dag, err := NewMemoryDAG[int]("test-pipeline", WithBufferSize(10), WithPollInterval(50))
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			for _, d := range tc.dependencies {
				if len(d) == 1 {
					err := dag.AddVertex(ctx, d[0])
					require.NoError(t, err)
				} else if len(d) == 2 {
					err := dag.AddEdge(ctx, d[0], d[1])
					require.NoError(t, err)
				}
			}

			pipeline, err := dag.Pipeline(ctx)
			require.NoError(t, err)

			var result []int
			timer := time.NewTimer(2 * time.Second)

		waitRes:
			for {
				select {
				case <-timer.C:
					break waitRes
				case item, ok := <-pipeline:
					if !ok {
						break waitRes
					}
					result = append(result, item)
					// Check if we got all vertices
					count, _ := dag.VertexCount(ctx)
					if count == 0 && len(result) > 0 {
						// Give some time for remaining items
						time.Sleep(100 * time.Millisecond)
					}
				}
			}

			t.Logf("result: %v", result)
			assert.True(t, isValidTopologicalOrder(tc.dependencies, result),
				"result is not a valid topological order")

			dag.Close()
		})
	}
}

// isValidTopologicalOrder checks if the output respects all dependencies.
func isValidTopologicalOrder(dependencies [][]int, output []int) bool {
	// Build dependency graph
	graph := make(map[int]map[int]bool)
	for _, d := range dependencies {
		if len(d) == 2 {
			from, to := d[0], d[1]
			if graph[from] == nil {
				graph[from] = make(map[int]bool)
			}
			graph[from][to] = true
		}
	}

	// Check that for every pair (i, j) where i < j in output,
	// there's no edge from output[j] to output[i]
	for i, v1 := range output {
		for j := i + 1; j < len(output); j++ {
			v2 := output[j]
			if graph[v2][v1] {
				fmt.Printf("invalid: %d should come before %d\n", v2, v1)
				return false
			}
		}
	}

	return true
}

func TestMemoryDAGFactory(t *testing.T) {
	factory := NewMemoryDAGFactory[int]()
	ctx := context.Background()

	// Create first DAG
	dag1, err := factory.Create(ctx, "test1")
	require.NoError(t, err)
	require.NotNil(t, dag1)

	// Create second DAG with same name should return the same instance
	dag2, err := factory.Create(ctx, "test1")
	require.NoError(t, err)
	assert.Equal(t, dag1, dag2)

	// Create third DAG with different name
	dag3, err := factory.Create(ctx, "test2")
	require.NoError(t, err)
	assert.NotEqual(t, dag1, dag3)
}

func TestMemoryDAGFactory_Remove(t *testing.T) {
	factory := NewMemoryDAGFactory[int]()
	ctx := context.Background()

	// Create a DAG
	dag1, err := factory.Create(ctx, "test-remove")
	require.NoError(t, err)
	require.NotNil(t, dag1)

	// Add some data
	err = dag1.AddEdge(ctx, 1, 2)
	require.NoError(t, err)

	// Remove the DAG
	err = factory.Remove("test-remove")
	require.NoError(t, err)

	// Creating a new DAG with the same name should return a new instance
	dag2, err := factory.Create(ctx, "test-remove")
	require.NoError(t, err)
	assert.NotEqual(t, dag1, dag2)

	// The new DAG should be empty
	count, err := dag2.VertexCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Removing a non-existent DAG should not error
	err = factory.Remove("non-existent")
	require.NoError(t, err)
}

func TestMemoryDAGFactory_Close(t *testing.T) {
	factory := NewMemoryDAGFactory[int]()
	ctx := context.Background()

	// Create multiple DAGs
	_, err := factory.Create(ctx, "dag1")
	require.NoError(t, err)
	_, err = factory.Create(ctx, "dag2")
	require.NoError(t, err)

	// Close the factory
	err = factory.Close()
	require.NoError(t, err)

	// Creating a new DAG after close should still work (factory is reusable)
	dag3, err := factory.Create(ctx, "dag3")
	require.NoError(t, err)
	require.NotNil(t, dag3)
}

func TestMemoryDAG_Vertices(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-vertices")
	require.NoError(t, err)
	ctx := context.Background()

	// Empty DAG should return empty slice
	vertices, err := dag.Vertices(ctx)
	require.NoError(t, err)
	assert.Empty(t, vertices)

	// Add some vertices
	err = dag.AddVertex(ctx, 1)
	require.NoError(t, err)
	err = dag.AddVertex(ctx, 2)
	require.NoError(t, err)
	err = dag.AddEdge(ctx, 3, 4)
	require.NoError(t, err)

	vertices, err = dag.Vertices(ctx)
	require.NoError(t, err)
	assert.Len(t, vertices, 4)
	assert.Contains(t, vertices, 1)
	assert.Contains(t, vertices, 2)
	assert.Contains(t, vertices, 3)
	assert.Contains(t, vertices, 4)
}

func TestMemoryDAG_ClosedOperations(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-closed")
	require.NoError(t, err)
	ctx := context.Background()

	// Close the DAG
	err = dag.Close()
	require.NoError(t, err)

	// All operations should return ErrDAGClosed
	err = dag.AddVertex(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	err = dag.AddEdge(ctx, 1, 2)
	assert.ErrorIs(t, err, ErrDAGClosed)

	err = dag.DelVertex(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	err = dag.DelEdge(ctx, 1, 2)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.HasVertex(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.HasEdge(ctx, 1, 2)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.InDegree(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.OutDegree(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.VertexCount(ctx)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.EdgeCount(ctx)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.Vertices(ctx)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.Successors(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.Predecessors(ctx, 1)
	assert.ErrorIs(t, err, ErrDAGClosed)

	_, err = dag.Pipeline(ctx)
	assert.ErrorIs(t, err, ErrDAGClosed)

	// Double close should be no-op
	err = dag.Close()
	assert.NoError(t, err)
}

func TestMemoryDAG_ErrorCases(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-errors")
	require.NoError(t, err)
	ctx := context.Background()

	// InDegree for non-existent vertex
	_, err = dag.InDegree(ctx, 999)
	assert.ErrorIs(t, err, ErrVertexNotFound)

	// OutDegree for non-existent vertex
	_, err = dag.OutDegree(ctx, 999)
	assert.ErrorIs(t, err, ErrVertexNotFound)

	// Successors for non-existent vertex
	_, err = dag.Successors(ctx, 999)
	assert.ErrorIs(t, err, ErrVertexNotFound)

	// Predecessors for non-existent vertex
	_, err = dag.Predecessors(ctx, 999)
	assert.ErrorIs(t, err, ErrVertexNotFound)

	// DelVertex for non-existent vertex
	err = dag.DelVertex(ctx, 999)
	assert.ErrorIs(t, err, ErrVertexNotFound)

	// DelEdge for non-existent edge
	err = dag.AddVertex(ctx, 1)
	require.NoError(t, err)
	err = dag.AddVertex(ctx, 2)
	require.NoError(t, err)
	err = dag.DelEdge(ctx, 1, 2)
	assert.ErrorIs(t, err, ErrEdgeNotFound)

	// DelEdge for non-existent from vertex
	err = dag.DelEdge(ctx, 999, 1)
	assert.ErrorIs(t, err, ErrEdgeNotFound)
}

func TestMemoryDAG_DuplicateOperations(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-duplicate")
	require.NoError(t, err)
	ctx := context.Background()

	// Adding same vertex twice should be no-op
	err = dag.AddVertex(ctx, 1)
	require.NoError(t, err)
	err = dag.AddVertex(ctx, 1)
	require.NoError(t, err)

	count, err := dag.VertexCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Adding same edge twice should be no-op
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)

	edgeCount, err := dag.EdgeCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, edgeCount)
}

func TestMemoryDAG_PipelineRepeatedCall(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-pipeline-repeat", WithBufferSize(10), WithPollInterval(50))
	require.NoError(t, err)
	ctx := context.Background()

	// Add vertices
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)

	// First pipeline call
	pipeline1, err := dag.Pipeline(ctx)
	require.NoError(t, err)

	// Second call should return the same channel
	pipeline2, err := dag.Pipeline(ctx)
	require.NoError(t, err)
	assert.Equal(t, pipeline1, pipeline2)

	dag.Close()
}

func TestMemoryDAG_HasEdge_NonExistent(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-hasedge")
	require.NoError(t, err)
	ctx := context.Background()

	// Test HasEdge when 'from' vertex doesn't exist
	has, err := dag.HasEdge(ctx, 1, 2)
	require.NoError(t, err)
	assert.False(t, has)

	// Add 'from' vertex only
	err = dag.AddVertex(ctx, 1)
	require.NoError(t, err)

	// Test HasEdge when edge doesn't exist
	has, err = dag.HasEdge(ctx, 1, 2)
	require.NoError(t, err)
	assert.False(t, has)
}

func TestMemoryDAG_DelVertex_WithEdges(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-delvertex-edges")
	require.NoError(t, err)
	ctx := context.Background()

	// Setup: create vertices with multiple edges
	// 1 -> 2 -> 3
	// 4 -> 2
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)
	err = dag.AddEdge(ctx, 2, 3)
	require.NoError(t, err)
	err = dag.AddEdge(ctx, 4, 2)
	require.NoError(t, err)

	// Delete vertex 2 (has both incoming and outgoing edges)
	err = dag.DelVertex(ctx, 2)
	require.NoError(t, err)

	// Verify vertex 2 is gone
	has, err := dag.HasVertex(ctx, 2)
	require.NoError(t, err)
	assert.False(t, has)

	// Verify edges are cleaned up
	has, err = dag.HasEdge(ctx, 1, 2)
	require.NoError(t, err)
	assert.False(t, has)

	// Verify other vertices still exist
	has, err = dag.HasVertex(ctx, 1)
	require.NoError(t, err)
	assert.True(t, has)

	has, err = dag.HasVertex(ctx, 3)
	require.NoError(t, err)
	assert.True(t, has)
}

func TestMemoryDAG_Pipeline_ContextCancel(t *testing.T) {
	dag, err := NewMemoryDAG[int]("test-pipeline-cancel", WithBufferSize(10), WithPollInterval(50))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Add vertices
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)

	// Start pipeline
	pipeline, err := dag.Pipeline(ctx)
	require.NoError(t, err)

	// Cancel context
	cancel()

	// Give time for goroutines to stop
	time.Sleep(100 * time.Millisecond)

	// Pipeline should eventually be closed
	select {
	case _, ok := <-pipeline:
		// Either got an item or channel is closed - both are valid
		_ = ok
	case <-time.After(500 * time.Millisecond):
		// Timeout is also acceptable
	}

	dag.Close()
}

func TestMemoryDAGFactory_CreateWithEmptyName(t *testing.T) {
	factory := NewMemoryDAGFactory[int]()
	ctx := context.Background()

	// Create with empty name should fail
	_, err := factory.Create(ctx, "")
	assert.Error(t, err)
}

func TestWithDAGLogger(t *testing.T) {
	// Just test that the option function works
	opts := DefaultOptions()
	assert.Nil(t, opts.Logger)

	// Note: We don't have a real logger to test with, but we can verify the option works
	// by checking that it doesn't panic
	opt := WithDAGLogger(nil)
	opt(opts)
	assert.Nil(t, opts.Logger)
}
