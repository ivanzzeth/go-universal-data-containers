package state

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFinalizerStateContainer(t *testing.T) {
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

	finalizer := NewCacheAndPersistFinalizer(registry, cache, persist)

	user1Container := NewStateContainer(finalizer, NewTestUserModel(&sync.Mutex{}, "user1", "server"))
	user1, err := user1Container.Get()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "user1", user1.Name)
	assert.Equal(t, "server", user1.Server)
	assert.Equal(t, 0, user1.Age)
	assert.Equal(t, 0, user1.Height)

	user1StateID, err := user1.GetIDComposer().ComposeStateID(user1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	// Change the value in memory
	user1.Age = 18
	user1.Height = 180

	// Then save changes in memory into cache
	err = user1Container.Wrap(user1).Save()
	if err != nil {
		t.Fatal(err)
	}

	// Load user1 from cache
	newUser1, err := NewStateContainer(finalizer, NewTestUserModel(&sync.Mutex{}, "user1", "server")).Get()
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
