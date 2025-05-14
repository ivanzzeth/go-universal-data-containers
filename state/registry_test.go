package state

import (
	"testing"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func SpecTestRegistry(t *testing.T, r Registry) {
	t.Run("RegisterState", func(t *testing.T) {
		err := r.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("New state after registering", func(t *testing.T) {
		userTemplate := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", "")
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

		assert.Equal(t, "", user.Name)
		assert.Equal(t, 0, user.Age)
		assert.Equal(t, 0, user.Height)

		// call NewState twice
		_, err = r.NewState(userTemplate.StateName())
		if err != nil {
			t.Error(err)
		}
	})
}
