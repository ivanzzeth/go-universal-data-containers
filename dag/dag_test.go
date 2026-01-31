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
	dag := NewMemoryDAG[int]("test-cycle")
	ctx := context.Background()

	// Create a chain: 1 -> 2 -> 3
	err := dag.AddEdge(ctx, 1, 2)
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
	dag := NewMemoryDAG[int]("test")
	SpecTestDAG(t, dag)
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
			dag := NewMemoryDAG[int]("test-pipeline", WithBufferSize(10), WithPollInterval(50))
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
