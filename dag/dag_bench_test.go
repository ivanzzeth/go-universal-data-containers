package dag

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func BenchmarkMemoryDAG_AddVertex(b *testing.B) {
	dag, _ := NewMemoryDAG[int]("bench")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.AddVertex(ctx, i)
	}
}

func BenchmarkMemoryDAG_AddEdge(b *testing.B) {
	dag, _ := NewMemoryDAG[int]("bench")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.AddEdge(ctx, i, i+1)
	}
}

func BenchmarkMemoryDAG_HasVertex(b *testing.B) {
	dag, _ := NewMemoryDAG[int]("bench")
	ctx := context.Background()

	// Setup: add vertices
	for i := 0; i < 1000; i++ {
		dag.AddVertex(ctx, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.HasVertex(ctx, i%1000)
	}
}

func BenchmarkMemoryDAG_HasEdge(b *testing.B) {
	dag, _ := NewMemoryDAG[int]("bench")
	ctx := context.Background()

	// Setup: add edges
	for i := 0; i < 1000; i++ {
		dag.AddEdge(ctx, i, i+1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.HasEdge(ctx, i%1000, (i%1000)+1)
	}
}

func BenchmarkMemoryDAG_Successors(b *testing.B) {
	dag, _ := NewMemoryDAG[int]("bench")
	ctx := context.Background()

	// Setup: add edges with multiple successors
	for i := 0; i < 100; i++ {
		for j := 0; j < 10; j++ {
			dag.AddEdge(ctx, i, 100+i*10+j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.Successors(ctx, i%100)
	}
}

func BenchmarkMemoryDAG_InDegree(b *testing.B) {
	dag, _ := NewMemoryDAG[int]("bench")
	ctx := context.Background()

	// Setup: add edges
	for i := 0; i < 1000; i++ {
		dag.AddEdge(ctx, i, i+1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.InDegree(ctx, i%1000+1)
	}
}

func BenchmarkMemoryDAG_CycleDetection(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("chain_%d", size), func(b *testing.B) {
			dag, _ := NewMemoryDAG[int]("bench")
			ctx := context.Background()

			// Setup: create a chain
			for i := 0; i < size; i++ {
				dag.AddEdge(ctx, i, i+1)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Try to add an edge that would create a cycle
				dag.AddEdge(ctx, size, 0)
			}
		})
	}
}

func BenchmarkRedisDAG_AddVertex(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	dag, _ := NewRedisDAG[int](client, "bench", nil)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.AddVertex(ctx, i)
	}
}

func BenchmarkRedisDAG_AddEdge(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	dag, _ := NewRedisDAG[int](client, "bench", nil)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.AddEdge(ctx, i, i+1)
	}
}

func BenchmarkRedisDAG_HasVertex(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	dag, _ := NewRedisDAG[int](client, "bench", nil)
	ctx := context.Background()

	// Setup: add vertices
	for i := 0; i < 1000; i++ {
		dag.AddVertex(ctx, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.HasVertex(ctx, i%1000)
	}
}

func BenchmarkRedisDAG_HasEdge(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	dag, _ := NewRedisDAG[int](client, "bench", nil)
	ctx := context.Background()

	// Setup: add edges
	for i := 0; i < 1000; i++ {
		dag.AddEdge(ctx, i, i+1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.HasEdge(ctx, i%1000, (i%1000)+1)
	}
}

func BenchmarkRedisDAG_Successors(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	dag, _ := NewRedisDAG[int](client, "bench", nil)
	ctx := context.Background()

	// Setup: add edges with multiple successors
	for i := 0; i < 100; i++ {
		for j := 0; j < 10; j++ {
			dag.AddEdge(ctx, i, 100+i*10+j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag.Successors(ctx, i%100)
	}
}

func BenchmarkRedisDAG_CycleDetection(b *testing.B) {
	for _, size := range []int{10, 100} {
		b.Run(fmt.Sprintf("chain_%d", size), func(b *testing.B) {
			mr, _ := miniredis.Run()
			defer mr.Close()
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()

			dag, _ := NewRedisDAG[int](client, "bench", nil)
			ctx := context.Background()

			// Setup: create a chain
			for i := 0; i < size; i++ {
				dag.AddEdge(ctx, i, i+1)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Try to add an edge that would create a cycle
				dag.AddEdge(ctx, size, 0)
			}
		})
	}
}

// Comparison benchmarks
func BenchmarkComparison_AddVertex(b *testing.B) {
	b.Run("Memory", func(b *testing.B) {
		dag, _ := NewMemoryDAG[int]("bench")
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.AddVertex(ctx, i)
		}
	})

	b.Run("Redis", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.AddVertex(ctx, i)
		}
	})
}

func BenchmarkComparison_AddEdge(b *testing.B) {
	b.Run("Memory", func(b *testing.B) {
		dag, _ := NewMemoryDAG[int]("bench")
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.AddEdge(ctx, i, i+1)
		}
	})

	b.Run("Redis", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.AddEdge(ctx, i, i+1)
		}
	})
}

// Batch operation benchmarks - compare single vs batch operations
func BenchmarkRedisDAG_AddVertices_Batch(b *testing.B) {
	for _, batchSize := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			mr, _ := miniredis.Run()
			defer mr.Close()
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()

			dag, _ := NewRedisDAG[int](client, "bench", nil)
			ctx := context.Background()

			// Prepare batch of vertices
			vertices := make([]int, batchSize)
			for i := range vertices {
				vertices[i] = i
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reset DAG for each iteration
				dag.Cleanup(ctx)
				dag.AddVertices(ctx, vertices, WithoutBatchLock())
			}
		})
	}
}

func BenchmarkRedisDAG_AddEdges_Batch(b *testing.B) {
	for _, batchSize := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			mr, _ := miniredis.Run()
			defer mr.Close()
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()

			dag, _ := NewRedisDAG[int](client, "bench", nil)
			ctx := context.Background()

			// Prepare batch of edges (chain)
			edges := make([]Edge[int], batchSize)
			for i := range edges {
				edges[i] = Edge[int]{From: i, To: i + 1}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reset DAG for each iteration
				dag.Cleanup(ctx)
				dag.AddEdges(ctx, edges, WithoutBatchLock())
			}
		})
	}
}

func BenchmarkRedisDAG_AddEdges_BatchSkipCycleDetection(b *testing.B) {
	for _, batchSize := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			mr, _ := miniredis.Run()
			defer mr.Close()
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()

			dag, _ := NewRedisDAG[int](client, "bench", nil)
			ctx := context.Background()

			// Prepare batch of edges (chain)
			edges := make([]Edge[int], batchSize)
			for i := range edges {
				edges[i] = Edge[int]{From: i, To: i + 1}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reset DAG for each iteration
				dag.Cleanup(ctx)
				dag.AddEdges(ctx, edges, WithoutBatchLock(), WithSkipCycleDetection())
			}
		})
	}
}

// Compare single operations vs batch operations
func BenchmarkComparison_AddVertices_SingleVsBatch(b *testing.B) {
	const numVertices = 100

	b.Run("Single", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.Cleanup(ctx)
			for j := 0; j < numVertices; j++ {
				dag.AddVertex(ctx, j)
			}
		}
	})

	b.Run("Batch", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		vertices := make([]int, numVertices)
		for i := range vertices {
			vertices[i] = i
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.Cleanup(ctx)
			dag.AddVertices(ctx, vertices, WithoutBatchLock())
		}
	})
}

func BenchmarkComparison_AddEdges_SingleVsBatch(b *testing.B) {
	const numEdges = 50

	b.Run("Single", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.Cleanup(ctx)
			for j := 0; j < numEdges; j++ {
				dag.AddEdge(ctx, j, j+1)
			}
		}
	})

	b.Run("Batch", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		edges := make([]Edge[int], numEdges)
		for i := range edges {
			edges[i] = Edge[int]{From: i, To: i + 1}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.Cleanup(ctx)
			dag.AddEdges(ctx, edges, WithoutBatchLock())
		}
	})

	b.Run("BatchSkipCycleDetection", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		edges := make([]Edge[int], numEdges)
		for i := range edges {
			edges[i] = Edge[int]{From: i, To: i + 1}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.Cleanup(ctx)
			dag.AddEdges(ctx, edges, WithoutBatchLock(), WithSkipCycleDetection())
		}
	})
}

// Benchmark distributed lock overhead
func BenchmarkRedisDAG_LockOverhead(b *testing.B) {
	b.Run("WithLock", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		vertices := make([]int, 10)
		for i := range vertices {
			vertices[i] = i
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.AddVertices(ctx, vertices, WithBatchLock())
		}
	})

	b.Run("WithoutLock", func(b *testing.B) {
		mr, _ := miniredis.Run()
		defer mr.Close()
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		dag, _ := NewRedisDAG[int](client, "bench", nil)
		ctx := context.Background()

		vertices := make([]int, 10)
		for i := range vertices {
			vertices[i] = i
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dag.AddVertices(ctx, vertices, WithoutBatchLock())
		}
	})
}
