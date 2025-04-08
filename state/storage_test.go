package state

import (
	"errors"
	"fmt"
	"sync"
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

	t.Run("SnapshotStates", func(t *testing.T) {
		err := registry.RegisterState(NewTestUserModel())
		if err != nil {
			t.Error(err)
		}

		snapshot1, err := storage.SnapshotStates()
		if err != nil {
			t.Fatal(err)
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

		snapshot2, err := storage.SnapshotStates()
		if err != nil {
			t.Fatal(err)
		}

		err = storage.RevertStatesToSnapshot(snapshot1)
		if err != nil {
			t.Fatal(err)
		}

		_, err = storage.LoadState("user", "1")
		assert.True(t, errors.Is(err, ErrStateNotFound))

		err = storage.RevertStatesToSnapshot(snapshot2)
		if err != nil {
			t.Fatal(err)
		}

		newU1, err = storage.LoadState("user", "1")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "user1", newU1.(*TestUserModel).Name)
		assert.Equal(t, 1, newU1.(*TestUserModel).Age)
		assert.Equal(t, 1, newU1.(*TestUserModel).Height)
	})
}

func SpecBenchmarkStorage(b *testing.B, registry Registry, storage Storage) {
	b.ReportAllocs()

	err := registry.RegisterState(NewTestUserModel())
	if err != nil {
		b.Fatal(err)
	}

	snapshot1, err := storage.SnapshotStates()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Test simple sync.Map", func(b *testing.B) {
		b.ResetTimer()
		var table sync.Map
		for i := 0; i < b.N; i++ {
			u1 := NewTestUserModel()
			u1.Name = "user1"
			u1.Age = 1
			u1.Height = 1
			u1.SetStateID("1")
			table.Store(u1.StateID(), u1)
		}
	})

	b.Run("Test single save", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			u1 := NewTestUserModel()
			u1.Name = "user1"
			u1.Age = 1
			u1.Height = 1
			u1.SetStateID("1")

			err = storage.SaveStates(u1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = storage.RevertStatesToSnapshot(snapshot1)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Test single load", func(b *testing.B) {
		u1 := NewTestUserModel()
		u1.Name = "user1"
		u1.Age = 1
		u1.Height = 1
		u1.SetStateID("1")

		err = storage.SaveStates(u1)
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := storage.LoadState("user", "1")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = storage.RevertStatesToSnapshot(snapshot1)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Test single revert for 10k states", func(b *testing.B) {
		users := []State{}
		for i := 0; i < 10_000; i++ {
			user := NewTestUserModel()
			user.Name = fmt.Sprintf("user-%v", i)
			user.Age = i
			user.Height = i
			user.SetStateID(fmt.Sprint(i))

			users = append(users, user)
		}

		err = storage.SaveStates(users...)
		if err != nil {
			b.Fatal(err)
		}

		snapshot2, err := storage.SnapshotStates()
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = storage.RevertStatesToSnapshot(snapshot1)
			if err != nil {
				b.Fatal(err)
			}

			err = storage.RevertStatesToSnapshot(snapshot2)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = storage.RevertStatesToSnapshot(snapshot1)
	if err != nil {
		b.Fatal(err)
	}
}
