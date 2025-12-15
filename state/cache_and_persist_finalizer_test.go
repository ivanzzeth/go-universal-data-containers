package state

import (
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

func TestCacheAndPersistFinalizer(t *testing.T) {
	registry := NewSimpleRegistry()
	err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", "", ""))
	if err != nil {
		t.Fatal(err)
	}

	lockerGenerator := locker.NewMemoryLockerGenerator()
	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")
	cache, _ := NewMemoryStorage(lockerGenerator, registry, cacheSnapshot, "cache")

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")
	persist, _ := NewMemoryStorage(lockerGenerator, registry, persistSnapshot, "persist")

	f := NewCacheAndPersistFinalizer(100*time.Millisecond, registry, lockerGenerator, cache, persist, "")
	defer f.Close()

	SpecTestFinalizer(t, lockerGenerator, f, func() Finalizer {
		return NewCacheAndPersistFinalizer(100*time.Millisecond, registry, lockerGenerator, cache, persist, "")
	})
}
