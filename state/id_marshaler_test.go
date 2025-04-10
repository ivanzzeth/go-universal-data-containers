package state

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonIDMarshaler(t *testing.T) {
	m := NewJsonIDMarshaler("-")
	SpecTestIDMarshaler(t, m)
}

func SpecTestIDMarshaler(t *testing.T, m IDMarshaler) {
	user1 := MustNewTestUserModel(&sync.Mutex{}, "user1", "server")

	stateID, err := m.MarshalStateID(user1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	newUser1 := MustNewTestUserModel(&sync.Mutex{}, "", "")
	err = m.UnmarshalStateID(stateID, newUser1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, user1.Name, newUser1.Name)
	assert.Equal(t, user1.Server, newUser1.Server)
}
