package state

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func SpecTestStorage(t *testing.T, registry Registry, storage Storage) {
	t.Run("LoadState if not registered", func(t *testing.T) {
		_, err := storage.LoadState("TestUserModel", "test")
		assert.NotNil(t, err)
	})

	t.Run("LoadState if registered", func(t *testing.T) {
		err := registry.RegisterState(NewTestUserModel())
		if err != nil {
			t.Error(err)
		}

		_, err = storage.LoadState("user", "1")
		assert.True(t, errors.Is(err, ErrStateNotFound))
	})

	t.Run("SaveState", func(t *testing.T) {
		err := registry.RegisterState(NewTestUserModel())
		if err != nil {
			t.Error(err)
		}

		u1 := NewTestUserModel()
		u1.Name = "user1"
		u1.Age = 1
		u1.Height = 1
		u1.SetStateID("1")

		err = storage.SaveStates(u1)
		if err != nil {
			t.Fatal(err)
		}

		newU1, err := storage.LoadState("user", "1")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "user1", newU1.(*TestUserModel).Name)
		assert.Equal(t, 1, newU1.(*TestUserModel).Age)
		assert.Equal(t, 1, newU1.(*TestUserModel).Height)
	})
}
