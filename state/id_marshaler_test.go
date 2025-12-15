package state

import (
	"testing"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func TestJsonIDMarshaler(t *testing.T) {
	m := NewJsonIDMarshaler("-")
	SpecTestIDMarshaler(t, m)
}

func TestBase64IDMarshaler(t *testing.T) {
	m := NewBase64IDMarshaler("-")
	SpecTestIDMarshaler(t, m)
}

func SpecTestIDMarshaler(t *testing.T, m IDMarshaler) {
	user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "partition1", "user1", "server")

	stateID, err := m.MarshalStateID(user1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	newUser1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "partition1", "", "")
	err = m.UnmarshalStateID(stateID, newUser1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, user1.Name, newUser1.Name)
	assert.Equal(t, user1.Server, newUser1.Server)
}
