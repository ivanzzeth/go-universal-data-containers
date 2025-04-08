package state

import (
	"testing"
	"time"
)

func TestMemoryStorage(t *testing.T) {
	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, nil)
	snapshot := NewBaseStorageSnapshot(storageFactory)

	storage := NewMemoryStateStorage(registry, snapshot)

	SpecTestStorage(t, registry, storage)
}

// func BenchmarkMemoryStorage(b *testing.B) {
// 	registry := NewSimpleRegistry()
// 	storageFactory := NewMemoryStorageFactory(registry, nil)
// 	snapshot := NewBaseStorageSnapshot(storageFactory)

// 	storage := NewMemoryStateStorage(registry, snapshot)
// 	snapshot.SetStorage(storage)
// 	SpecBenchmarkStorage(b, registry, storage)
// }

func BenchmarkMemoryStorageWith2msLatency(b *testing.B) {
	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, nil)
	snapshot := NewBaseStorageSnapshot(storageFactory)

	storage := NewMemoryStateStorage(registry, snapshot)
	storage.setDelay(2 * time.Millisecond)
	snapshot.SetStorage(storage)
	SpecBenchmarkStorage(b, registry, storage)
}
