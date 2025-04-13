package state

import (
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

func TestMemoryStorage(t *testing.T) {
	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, locker.NewMemoryLockerGenerator(), nil)
	snapshot := NewSimpleStorageSnapshot(storageFactory)

	storage := NewMemoryStorage(&sync.Mutex{}, registry, snapshot)

	SpecTestStorage(t, registry, storage)
}

// func BenchmarkMemoryStorage(b *testing.B) {
// 	registry := NewSimpleRegistry()
// 	storageFactory := NewMemoryStorageFactory(registry, nil)
// 	snapshot := NewSimpleStorageSnapshot(storageFactory)

// 	storage := NewMemoryStateStorage(registry, snapshot)
// 	snapshot.SetStorage(storage)
// 	SpecBenchmarkStorage(b, registry, storage)
// }

func BenchmarkMemoryStorageWith2msLatency(b *testing.B) {
	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, locker.NewMemoryLockerGenerator(), nil)
	snapshot := NewSimpleStorageSnapshot(storageFactory)

	storage := NewMemoryStorage(&sync.Mutex{}, registry, snapshot)
	storage.setDelay(2 * time.Millisecond)
	snapshot.SetStorage(storage)
	SpecBenchmarkStorage(b, registry, storage)
}
