package state

import (
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

func TestCacheAndPersistFinalizer(t *testing.T) {
	registry := NewSimpleRegistry()
	err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
	if err != nil {
		t.Fatal(err)
	}

	lockerGenerator := locker.NewMemoryLockerGenerator()
	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator)
	cache, _ := NewMemoryStorage(lockerGenerator, registry, cacheSnapshot, "")

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator)
	persist, _ := NewMemoryStorage(lockerGenerator, registry, persistSnapshot, "")

	ticker := time.NewTicker(2 * time.Second).C
	f := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer f.Close()
	SpecTestFinalizer(t, f)
}
