package state

import (
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

func TestMemoryStorage(t *testing.T) {
	registry := NewSimpleRegistry()
	lockerGenerator := locker.NewMemoryLockerGenerator()
	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	snapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")

	storage, err := NewMemoryStorage(lockerGenerator, registry, snapshot, "")
	if err != nil {
		t.Fatal(err)
	}
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
	lockerGenerator := locker.NewMemoryLockerGenerator()
	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	snapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")

	storage, err := NewMemoryStorage(lockerGenerator, registry, snapshot, "")
	if err != nil {
		b.Fatal(err)
	}
	storage.setDelay(2 * time.Millisecond)
	snapshot.SetStorageForSnapshot(storage)
	SpecBenchmarkStorage(b, registry, storage)
}
