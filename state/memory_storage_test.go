package state

import "testing"

func TestMemoryStorage(t *testing.T) {
	registry := NewSimpleRegistry()
	SpecTestStorage(t, registry, NewMemoryStateStorage(registry))
}
