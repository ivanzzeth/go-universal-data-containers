package state

import "testing"

func TestSimpleRegistry(t *testing.T) {
	SpecTestRegistry(t, NewSimpleRegistry())
}
