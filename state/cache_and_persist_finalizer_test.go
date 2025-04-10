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
	cache := NewMemoryStorage(registry, cacheSnapshot)

	persistSnapshot := NewBaseStorageSnapshot(storageFactory)
	persist := NewMemoryStorage(registry, persistSnapshot)

	ticker := time.NewTicker(2 * time.Second).C
	f := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer f.Close()
	SpecTestFinalizer(t, f)
}
