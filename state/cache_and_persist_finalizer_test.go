package state

import (
	"sync"
	"testing"
)

func TestCacheAndPersistFinalizer(t *testing.T) {
	registry := NewSimpleRegistry()
	err := registry.RegisterState(NewTestUserModel(&sync.Mutex{}, "", ""))
	if err != nil {
		t.Fatal(err)
	}

	storageFactory := NewMemoryStorageFactory(registry, nil)
	cacheSnapshot := NewBaseStorageSnapshot(storageFactory)
	cache := NewMemoryStateStorage(registry, cacheSnapshot)
	cacheSnapshot.SetStorage(cache)

	persistSnapshot := NewBaseStorageSnapshot(storageFactory)
	persist := NewMemoryStateStorage(registry, persistSnapshot)
	persistSnapshot.SetStorage(persist)

	f := NewCacheAndPersistFinalizer(registry, cache, persist)
	SpecTestFinalizer(t, f)
}
