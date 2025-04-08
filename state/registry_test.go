package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func SpecTestRegistry(t *testing.T, r Registry) {
	t.Run("RegisterState", func(t *testing.T) {
		err := r.RegisterState(NewTestUserModel())
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("LoadState", func(t *testing.T) {
		userTemplate := NewTestUserModel()
		err := r.RegisterState(userTemplate)
		if err != nil {
			t.Error(err)
		}

		userState, err := r.NewState(userTemplate.StateName())
		if err != nil {
			t.Error(err)
		}

		user, ok := userState.(*TestUserModel)
		if !ok {
			t.Error("userState is not *TestUserModel")
		}

		assert.Equal(t, userTemplate.StateName(), user.StateName())
		assert.Equal(t, userTemplate.StateID(), user.StateID())

		assert.Equal(t, "", user.Name)
		assert.Equal(t, 0, user.Age)
		assert.Equal(t, 0, user.Height)
	})
}
