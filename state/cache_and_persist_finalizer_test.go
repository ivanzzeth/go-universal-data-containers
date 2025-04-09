package state

import (
	"sync"
	"testing"
	"time"
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

	ticker := time.NewTicker(2 * time.Second).C
	f := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer f.Close()
	SpecTestFinalizer(t, f)
}
