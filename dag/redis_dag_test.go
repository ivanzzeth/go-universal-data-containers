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
