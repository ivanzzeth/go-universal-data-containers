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

	lockerGenerator := locker.NewMemoryLockerGenerator()
	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory)
	cache, _ := NewMemoryStorage(lockerGenerator, registry, cacheSnapshot, "")

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory)
	persist, _ := NewMemoryStorage(lockerGenerator, registry, persistSnapshot, "")

	ticker := time.NewTicker(2 * time.Second).C
	f := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer f.Close()
	SpecTestFinalizer(t, f)
}
