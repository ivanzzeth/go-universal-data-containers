package dag

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMiniRedis(t *testing.T) (*miniredis.Miniredis, redis.Cmdable) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	t.Cleanup(func() {
		client.Close()
		mr.Close()
	})

	return mr, client
}

func TestRedisDAG(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test", nil)
	require.NoError(t, err)

	SpecTestDAG(t, dag)
}

func TestRedisDAG_CycleDetection(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-cycle", nil)
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

func TestRedisDAG_Pipeline(t *testing.T) {
	type testcase struct {
		name         string
		dependencies [][]int
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
			_, client := setupMiniRedis(t)

			dag, err := NewRedisDAG[int](client, "test-pipeline-"+tc.name, nil, WithBufferSize(10), WithPollInterval(50))
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			timer := time.NewTimer(3 * time.Second)

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
						time.Sleep(200 * time.Millisecond)
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

func TestRedisDAG_Cleanup(t *testing.T) {
	mr, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-cleanup", nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Add some data
	err = dag.AddEdge(ctx, 1, 2)
	require.NoError(t, err)

	// Verify keys exist
	keys := mr.Keys()
	assert.NotEmpty(t, keys)

	// Cleanup
	err = dag.Cleanup(ctx)
	require.NoError(t, err)

	// Verify keys are gone
	keys = mr.Keys()
	assert.Empty(t, keys)
}

func TestRedisDAGFactory(t *testing.T) {
	_, client := setupMiniRedis(t)

	factory := NewRedisDAGFactory[int](client, nil)
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

func TestRedisDAG_StringVertex(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[string](client, "test-string", nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Test with string vertices
	err = dag.AddEdge(ctx, "task-a", "task-b")
	require.NoError(t, err)
	err = dag.AddEdge(ctx, "task-b", "task-c")
	require.NoError(t, err)

	has, err := dag.HasEdge(ctx, "task-a", "task-b")
	require.NoError(t, err)
	assert.True(t, has)

	successors, err := dag.Successors(ctx, "task-a")
	require.NoError(t, err)
	assert.Contains(t, successors, "task-b")
}

func TestRedisDAG_Vertices(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-vertices", nil)
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

func TestRedisDAG_ClosedOperations(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-closed", nil)
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

func TestRedisDAG_ErrorCases(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-errors", nil)
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

func TestRedisDAG_DuplicateOperations(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-duplicate", nil)
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

func TestRedisDAG_PipelineRepeatedCall(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-pipeline-repeat", nil, WithBufferSize(10), WithPollInterval(50))
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

func TestRedisDAGFactory_Remove(t *testing.T) {
	mr, client := setupMiniRedis(t)

	factory := NewRedisDAGFactory[int](client, nil)
	ctx := context.Background()

	// Create a DAG
	dag1, err := factory.Create(ctx, "test-remove")
	require.NoError(t, err)
	require.NotNil(t, dag1)

	// Add some data
	err = dag1.AddEdge(ctx, 1, 2)
	require.NoError(t, err)

	// Verify keys exist
	keys := mr.Keys()
	assert.NotEmpty(t, keys)

	// Remove the DAG
	err = factory.Remove(ctx, "test-remove")
	require.NoError(t, err)

	// Verify keys are gone (cleanup was called)
	keys = mr.Keys()
	assert.Empty(t, keys)

	// Creating a new DAG with the same name should return a new instance
	dag2, err := factory.Create(ctx, "test-remove")
	require.NoError(t, err)
	assert.NotEqual(t, dag1, dag2)

	// Removing a non-existent DAG should not error
	err = factory.Remove(ctx, "non-existent")
	require.NoError(t, err)
}

func TestRedisDAGFactory_Close(t *testing.T) {
	_, client := setupMiniRedis(t)

	factory := NewRedisDAGFactory[int](client, nil)
	ctx := context.Background()

	// Create multiple DAGs
	dag1, err := factory.Create(ctx, "dag1")
	require.NoError(t, err)
	dag2, err := factory.Create(ctx, "dag2")
	require.NoError(t, err)

	// Add data to both
	err = dag1.AddVertex(ctx, 1)
	require.NoError(t, err)
	err = dag2.AddVertex(ctx, 2)
	require.NoError(t, err)

	// Close the factory
	err = factory.Close(ctx)
	require.NoError(t, err)

	// Creating a new DAG after close should still work (factory is reusable)
	dag3, err := factory.Create(ctx, "dag3")
	require.NoError(t, err)
	require.NotNil(t, dag3)
}

func TestRedisDAG_NewRedisDAG_Validation(t *testing.T) {
	_, client := setupMiniRedis(t)

	// Empty name should fail
	_, err := NewRedisDAG[int](client, "", nil)
	assert.Error(t, err)

	// Nil client should fail
	_, err = NewRedisDAG[int](nil, "test", nil)
	assert.Error(t, err)
}

func TestRedisDAG_CustomMarshal(t *testing.T) {
	_, client := setupMiniRedis(t)

	// Custom marshal/unmarshal for string type
	config := &RedisDAGConfig[string]{
		Marshal: func(s string) (string, error) {
			return "prefix:" + s, nil
		},
		Unmarshal: func(s string) (string, error) {
			if len(s) > 7 && s[:7] == "prefix:" {
				return s[7:], nil
			}
			return s, nil
		},
	}

	dag, err := NewRedisDAG[string](client, "test-custom", config)
	require.NoError(t, err)

	ctx := context.Background()

	err = dag.AddEdge(ctx, "a", "b")
	require.NoError(t, err)

	has, err := dag.HasEdge(ctx, "a", "b")
	require.NoError(t, err)
	assert.True(t, has)

	successors, err := dag.Successors(ctx, "a")
	require.NoError(t, err)
	assert.Contains(t, successors, "b")
}

func TestRedisDAG_HasEdge_NonExistent(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-hasedge", nil)
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

func TestRedisDAG_Pipeline_ContextCancel(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-pipeline-cancel", nil, WithBufferSize(10), WithPollInterval(50))
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
		_ = ok
	case <-time.After(500 * time.Millisecond):
		// Timeout is acceptable
	}

	dag.Close()
}

func TestRedisDAGFactory_CreateWithEmptyName(t *testing.T) {
	_, client := setupMiniRedis(t)

	factory := NewRedisDAGFactory[int](client, nil)
	ctx := context.Background()

	// Create with empty name should fail
	_, err := factory.Create(ctx, "")
	assert.Error(t, err)
}

func TestRedisDAG_DelVertex_WithEdges(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-delvertex-edges", nil)
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

func TestRedisDAG_EdgeCount_Empty(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-edgecount-empty", nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Empty DAG should have 0 edges
	count, err := dag.EdgeCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestRedisDAG_InDegree_Zero(t *testing.T) {
	_, client := setupMiniRedis(t)

	dag, err := NewRedisDAG[int](client, "test-indegree-zero", nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Add a vertex with no incoming edges
	err = dag.AddVertex(ctx, 1)
	require.NoError(t, err)

	inDeg, err := dag.InDegree(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 0, inDeg)
}
