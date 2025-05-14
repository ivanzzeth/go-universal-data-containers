package state

import (
	"testing"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func SpecTestFinalizer(t *testing.T, finalizer Finalizer) {
	err := finalizer.ClearAllCachedStates()
	if err != nil {
		t.Fatal(err)
	}

	err = finalizer.ClearSnapshots()
	if err != nil {
		t.Fatal(err)
	}

	snapshot1, err := finalizer.SnapshotStates()
	if err != nil {
		t.Fatal(err)
	}

	// Pattern1: LoadState then assert state as *TestUserModel
	user1Name := "user1"
	user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), user1Name, "server")
	user1StateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}

	_, err = finalizer.LoadState(user1.StateName(), user1StateID)
	assert.Equal(t, err, ErrStateNotFound)

	user1.Name = user1Name
	user1.Server = "server"
	user1.Age = 1
	user1.Height = 1

	err = finalizer.SaveState(user1)
	if err != nil {
		t.Fatal(err)
	}

	snapshot2, err := finalizer.SnapshotStates()
	if err != nil {
		t.Fatal(err)
	}

	// Pattern2:
	newUser1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), user1Name, "server")
	err = newUser1.WithStateFinalizer(finalizer).Get()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, user1Name, newUser1.Name)
	assert.Equal(t, "server", newUser1.Server)
	assert.Equal(t, 1, newUser1.Age)
	assert.Equal(t, 1, newUser1.Height)

	// Revert to snapshot1
	err = finalizer.RevertStatesToSnapshot(snapshot1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = finalizer.LoadState(user1.StateName(), user1StateID)
	assert.Equal(t, ErrStateNotFound, err)

	// Revert to snapshot2
	err = finalizer.RevertStatesToSnapshot(snapshot2)
	if err != nil {
		t.Fatal(err)
	}

	user1.Name = user1Name
	user1.Server = "server"
	user1StateID, err = user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
	if err != nil {
		t.Fatal(err)
	}
	newUserState1, err := finalizer.LoadState(user1.StateName(), user1StateID)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, user1.StateName(), newUserState1.StateName())

	newUser1, ok := newUserState1.(*TestUserModel)
	if !ok {
		t.Fatal("newUserState1 is not *TestUserModel")
	}

	assert.Equal(t, user1Name, newUser1.Name)
	assert.Equal(t, "server", newUser1.Server)
	assert.Equal(t, 1, newUser1.Age)
	assert.Equal(t, 1, newUser1.Height)
}
