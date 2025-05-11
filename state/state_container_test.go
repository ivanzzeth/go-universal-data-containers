package state

import (
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func TestFinalizerStateContainer(t *testing.T) {
	registry := NewSimpleRegistry()
	err := registry.RegisterState(MustNewTestUserModel(&sync.Mutex{}, "", ""))
	if err != nil {
		t.Fatal(err)
	}

	storageFactory := NewMemoryStorageFactory(registry, locker.NewMemoryLockerGenerator(), nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory)
	cache := NewMemoryStorage(&sync.Mutex{}, registry, cacheSnapshot, "")
	cacheSnapshot.SetStorageForSnapshot(cache)

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory)
	persist := NewMemoryStorage(&sync.Mutex{}, registry, persistSnapshot, "")
	persistSnapshot.SetStorageForSnapshot(persist)

	ticker := time.NewTicker(2 * time.Second).C
	finalizer := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer finalizer.Close()

	user1Container := NewStateContainer(finalizer, MustNewTestUserModel(&sync.Mutex{}, "user1", "server"))
	user1, err := user1Container.Get()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "user1", user1.Name)
	assert.Equal(t, "server", user1.Server)
	assert.Equal(t, 0, user1.Age)
	assert.Equal(t, 0, user1.Height)

	user1StateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	// Change the value in memory.
	// Use sync.Locker to make sure concurrent safety accross
	// 1. go-routines if the implementation of sync.Locker is like sync.Mutex or
	// 2. micro-services if the implementation of sync.Locker is distributed locker.
	user1.Lock()
	user1.Age = 18
	user1.Height = 180

	// Then save changes in memory into cache
	err = user1Container.Save()
	// err = user1Container.Wrap(user1).Save()
	if err != nil {
		t.Fatal(err)
	}
	user1.Unlock()

	// Load user1 from cache
	newUser1, err := NewStateContainer(finalizer, MustNewTestUserModel(&sync.Mutex{}, "user1", "server")).Get()
	if err != nil {
		t.Fatal(err)
	}

	// Make sure the changes are in cache
	assert.Equal(t, "user1", newUser1.Name)
	assert.Equal(t, "server", newUser1.Server)
	assert.Equal(t, 18, newUser1.Age)
	assert.Equal(t, 180, newUser1.Height)

	// Load user1 from persist
	_, err = persist.LoadState(user1.StateName(), user1StateID)
	assert.Equal(t, err, ErrStateNotFound)

	// Finalize states into persist database
	err = finalizer.FinalizeAllCachedStates()
	if err != nil {
		t.Fatal(err)
	}

	// Make sure that the changes are in persist after finalization
	_, err = persist.LoadState(user1.StateName(), user1StateID)
	assert.NotEqual(t, err, ErrStateNotFound)
}
