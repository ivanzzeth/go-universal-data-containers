package state

import "testing"

func TestCacheAndPersistFinalizer(t *testing.T) {
	registry := NewSimpleRegistry()
	err := registry.RegisterState(NewTestUserModel())
	if err != nil {
		t.Fatal(err)
	}

	cache := NewMemoryStateStorage(registry)
	persist := NewMemoryStateStorage(registry)
	f := NewCacheAndPersistStateFinalizer(registry, cache, persist)
	SpecTestFinalizer(t, f)
}
