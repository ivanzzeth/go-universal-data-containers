package state

import (
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

func TestCacheAndPersistFinalizer(t *testing.T) {
	registry := NewSimpleRegistry()
	err := registry.RegisterState(MustNewTestUserModel(&sync.Mutex{}, "", ""))
	if err != nil {
		t.Fatal(err)
	}

	storageFactory := NewMemoryStorageFactory(registry, locker.NewMemoryLockerGenerator(), nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory)
	cache := NewMemoryStorage(&sync.Mutex{}, registry, cacheSnapshot, "")

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory)
	persist := NewMemoryStorage(&sync.Mutex{}, registry, persistSnapshot, "")

	ticker := time.NewTicker(2 * time.Second).C
	f := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer f.Close()
	SpecTestFinalizer(t, f)
}
