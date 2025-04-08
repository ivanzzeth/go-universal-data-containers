package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func SpecTestFinalizer(t *testing.T, finalizer Finalizer) {
	err := finalizer.ClearAllCachedStates()
	if err != nil {
		t.Fatal(err)
	}

	userState1, err := finalizer.LoadState("user", "1")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "user", userState1.StateName())
	assert.Equal(t, "1", userState1.StateID())

	user1, ok := userState1.(*TestUserModel)
	if !ok {
		t.Fatal("userState1 is not *TestUserModel")
	}

	// check default values for first loading
	assert.Equal(t, "", user1.Name)
	assert.Equal(t, 0, user1.Age)
	assert.Equal(t, 0, user1.Height)

	user1.Name = "user1"
	user1.Age = 1
	user1.Height = 1

	err = finalizer.SaveState(user1)
	if err != nil {
		t.Fatal(err)
	}

	newUserState1, err := finalizer.LoadState("user", "1")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "user", newUserState1.StateName())
	assert.Equal(t, "1", newUserState1.StateID())

	newUser1, ok := newUserState1.(*TestUserModel)
	if !ok {
		t.Fatal("newUserState1 is not *TestUserModel")
	}

	assert.Equal(t, "user1", newUser1.Name)
	assert.Equal(t, 1, newUser1.Age)
	assert.Equal(t, 1, newUser1.Height)
}
