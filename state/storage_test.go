package state

import (
	"context"
	"sync"
	"testing"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func SpecTestStorage(t *testing.T, registry Registry, storage Storage) {
	t.Run("LoadState if not registered", func(t *testing.T) {
		_, err := storage.LoadState(context.Background(), "TestUserModel", "test")
		assert.Equal(t, ErrStateNotRegistered, err)
	})

	t.Run("LoadState if registered", func(t *testing.T) {
		err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Fatal(err)
		}

		user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		stateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}

		_, err = storage.LoadState(context.Background(), user1.StateName(), stateID)
		assert.Equal(t, ErrStateNotFound, err)
	})

	t.Run("Write once and clear", func(t *testing.T) {
		err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Fatal(err)
		}

		user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		stateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}

		user1.Age = 1
		err = storage.SaveStates(context.Background(), user1)
		if err != nil {
			t.Fatal(err)
		}

		// Check all getter before clearing
		stateNames, err := storage.GetStateNames(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(stateNames))
		if len(stateNames) > 0 {
			assert.Equal(t, user1.StateName(), stateNames[0])
		}

		stateIDs, err := storage.GetStateIDs(context.Background(), user1.StateName())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(stateIDs))

		if len(stateIDs) > 0 {
			assert.Equal(t, stateID, stateIDs[0])
		}

		allStates, err := storage.LoadAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(allStates))

		// Clear

		err = storage.ClearAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		stateNames, err = storage.GetStateNames(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 0, len(stateNames), "GetStateNames must be empty after clear")

		stateIDs, err = storage.GetStateIDs(context.Background(), user1.StateName())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 0, len(stateIDs), "GetStateIDs must be empty after clear")

		allStates, err = storage.LoadAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 0, len(allStates), "LoadAllStates must be empty after clear")
	})

	t.Run("Write, clear, and write", func(t *testing.T) {
		err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Fatal(err)
		}

		user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		stateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}

		user1.Age = 1
		user1.Height = 1
		err = storage.SaveStates(context.Background(), user1)
		if err != nil {
			t.Fatal(err)
		}

		err = storage.ClearAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		allStates, err := storage.LoadAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 0, len(allStates), "LoadAllStates must be empty after clear")

		// Load state using same key
		newU1, err := storage.LoadState(context.Background(), user1.StateName(), stateID)
		assert.Equal(t, ErrStateNotFound, err)
		assert.Nil(t, newU1)

		// Write twice
		user1.Age = 2
		err = storage.SaveStates(context.Background(), user1)
		if err != nil {
			t.Fatal(err)
		}

		newU1, err = storage.LoadState(context.Background(), user1.StateName(), stateID)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "user1", newU1.(*TestUserModel).Name)
		assert.Equal(t, "server", newU1.(*TestUserModel).Server)
		assert.Equal(t, 2, newU1.(*TestUserModel).Age)
		assert.Equal(t, 1, newU1.(*TestUserModel).Height)

		err = storage.ClearAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		allStates, err = storage.LoadAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 0, len(allStates), "LoadAllStates must be empty after clear")
	})

	snapshot1, err := storage.SnapshotStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot1: %v", snapshot1)
	snapshotIds, err := storage.GetSnapshotIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(snapshotIds))
	assert.Equal(t, snapshot1, snapshotIds[0])

	var snapshot2 string

	t.Run("SnapshotStates", func(t *testing.T) {
		err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Fatal(err)
		}

		u1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		u1.Age = 1
		u1.Height = 1

		err = storage.SaveStates(context.Background(), u1)
		if err != nil {
			t.Fatal(err)
		}

		u1ID, err := u1.GetIDMarshaler().MarshalStateID("user1", "server")
		if err != nil {
			t.Fatal(err)
		}

		newU1, err := storage.LoadState(context.Background(), u1.StateName(), u1ID)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "user1", newU1.(*TestUserModel).Name)
		assert.Equal(t, "server", newU1.(*TestUserModel).Server)
		assert.Equal(t, 1, newU1.(*TestUserModel).Age)
		assert.Equal(t, 1, newU1.(*TestUserModel).Height)

		allStates, err := storage.LoadAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 1, len(allStates), "expected length of all states is 1")

		snapshot2, err = storage.SnapshotStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("snapshot2: %v", snapshot2)
		snapshotIds, err := storage.GetSnapshotIDs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(snapshotIds))
		assert.Contains(t, snapshotIds, snapshot1)
		assert.Contains(t, snapshotIds, snapshot2)

		t.Logf("RevertStatesToSnapshot revert to snapshot1")
		err = storage.RevertStatesToSnapshot(context.Background(), snapshot1)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("RevertStatesToSnapshot revert to snapshot1 ended")

		snapshotIds, err = storage.GetSnapshotIDs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(snapshotIds), "Remain snapshots after reverting")
		assert.Contains(t, snapshotIds, snapshot1)
		assert.Contains(t, snapshotIds, snapshot2)

		_, err = storage.LoadState(context.Background(), u1.StateName(), u1ID)
		assert.Equal(t, ErrStateNotFound, err)

		t.Logf("RevertStatesToSnapshot revert to snapshot2")

		err = storage.RevertStatesToSnapshot(context.Background(), snapshot2)
		if err != nil {
			t.Fatal(err)
		}

		snapshotIds, err = storage.GetSnapshotIDs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(snapshotIds), "Remain snapshots after reverting")
		assert.Contains(t, snapshotIds, snapshot1)
		assert.Contains(t, snapshotIds, snapshot2)

		newU1, err = storage.LoadState(context.Background(), u1.StateName(), u1ID)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "user1", newU1.(*TestUserModel).Name)
		assert.Equal(t, "server", newU1.(*TestUserModel).Server)
		assert.Equal(t, 1, newU1.(*TestUserModel).Age)
		assert.Equal(t, 1, newU1.(*TestUserModel).Height)
	})

	t.Run("Multiple states", func(t *testing.T) {
		user2 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user2", "server")
		user3 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user3", "server2")

		user2.Height = 120

		user3.Age = 13

		err := storage.SaveStates(context.Background(), user2, user3)
		if err != nil {
			t.Fatal(err)
		}

		allStates, err := storage.LoadAllStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 3, len(allStates))

		user2StateID, err := GetStateID(user2)
		if err != nil {
			t.Fatal(err)
		}

		user3StateID, err := GetStateID(user3)
		if err != nil {
			t.Fatal(err)
		}

		user2State, err := storage.LoadState(context.Background(), user2.StateName(), user2StateID)
		if err != nil {
			t.Fatal(err)
		}

		user3State, err := storage.LoadState(context.Background(), user3.StateName(), user3StateID)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 120, user2State.(*TestUserModel).Height)
		assert.Equal(t, 13, user3State.(*TestUserModel).Age)
	})

	t.Run("Update multiple times", func(t *testing.T) {
		// Clear changes before
		err = storage.RevertStatesToSnapshot(context.Background(), snapshot1)
		if err != nil {
			t.Fatal(err)
		}

		err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Fatal(err)
		}

		{
			allStates, loadErr := storage.LoadAllStates(context.Background())
			if loadErr == nil {
				t.Logf("LoadAllStates on start: err=%v states=%+v", err, allStates)
			}
		}

		user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		stateID, err := GetStateID(user1)
		if err != nil {
			t.Fatal(err)
		}

		count := 2
		for i := 0; i < count; i++ {
			{
				allStates, loadErr := storage.LoadAllStates(context.Background())
				if loadErr == nil {
					for _, state := range allStates {
						t.Logf("LoadAllStates on loop %d: err=%v state=%+v", i, err, state)
					}
				}
			}

			user1.Age++
			err := storage.SaveStates(context.Background(), user1)
			if err != nil {
				t.Fatal(err)
			}
			// time.Sleep(1 * time.Second)
		}

		newUser1State, err := storage.LoadState(context.Background(), user1.StateName(), stateID)
		if err != nil {
			t.Fatal(err)
		}

		newUser1, ok := newUser1State.(*TestUserModel)
		if !ok {
			t.Fatal("type of newUser1State is not *TestUserModel")
		}

		assert.Equal(t, count, newUser1.Age)
	})

	t.Run("Concurrent", func(t *testing.T) {
		err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
		if err != nil {
			t.Fatal(err)
		}

		user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		stateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}

		count := 100
		errChan := make(chan error, count)
		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				user1.Lock(context.Background())
				defer user1.Unlock(context.Background())
				user1.Age++
				err := storage.SaveStates(context.Background(), user1)
				if err != nil {
					errChan <- err

					{
						allStates, loadErr := storage.LoadAllStates(context.Background())
						if loadErr == nil {
							for _, state := range allStates {
								t.Logf("LoadAllStates on failed: err=%v state=%+v", err, state)
							}
						}
					}
				}
			}()
		}

		wg.Wait()
		close(errChan)
		for err := range errChan {
			if err != nil {
				t.Fatal(err)
			}
		}

		newUser1State, err := storage.LoadState(context.Background(), user1.StateName(), stateID)
		if err != nil {
			t.Fatal(err)
		}

		newUser1, ok := newUser1State.(*TestUserModel)
		if !ok {
			t.Fatal("type of newUser1State is not *TestUserModel")
		}

		assert.Equal(t, count, newUser1.Age)
	})

	t.Run("ClearSnapshot", func(t *testing.T) {
		snapshotIds, err := storage.GetSnapshotIDs(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(snapshotIds))

		t.Logf("snapshots: %v", snapshotIds)
		t.Logf("snapshot1: %v", snapshot1)
		t.Logf("snapshot2: %v", snapshot2)

		err = storage.ClearSnapshots(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		_, err = storage.GetSnapshot(context.Background(), snapshot1)
		assert.Equal(t, ErrSnapshotNotFound, err)
	})
}

func SpecBenchmarkStorage(b *testing.B, registry Registry, storage Storage) {
	b.ReportAllocs()

	err := registry.RegisterState(MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", ""))
	if err != nil {
		b.Fatal(err)
	}

	snapshot1, err := storage.SnapshotStates(context.Background())
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Test simple sync.Map", func(b *testing.B) {
		b.ResetTimer()
		var table sync.Map
		for i := 0; i < b.N; i++ {
			u1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
			u1.Age = 1
			u1.Height = 1
			stateID, err := u1.GetIDMarshaler().MarshalStateID(u1.StateIDComponents()...)
			if err != nil {
				b.Fatal(err)
			}

			table.Store(stateID, u1)
		}
	})

	b.Run("Test single save", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			u1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
			u1.Age = 1
			u1.Height = 1

			err = storage.SaveStates(context.Background(), u1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = storage.RevertStatesToSnapshot(context.Background(), snapshot1)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Test single load", func(b *testing.B) {
		u1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
		u1.Age = 1
		u1.Height = 1

		err = storage.SaveStates(context.Background(), u1)
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			stateID, err := u1.GetIDMarshaler().MarshalStateID(u1.StateIDComponents()...)
			if err != nil {
				b.Fatal(err)
			}
			_, err = storage.LoadState(context.Background(), u1.StateName(), stateID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = storage.RevertStatesToSnapshot(context.Background(), snapshot1)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Test single revert for 10k states", func(b *testing.B) {
		users := []State{}
		// for i := 0; i < 10; i++ {
		for i := 0; i < 10_000; i++ {
			u1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "user1", "server")
			u1.Age = 1
			u1.Height = 1

			users = append(users, u1)
		}

		err = storage.SaveStates(context.Background(), users...)
		if err != nil {
			b.Fatal(err)
		}

		// fmt.Printf("storage.SnapshotStates() >>>>>>>\n")
		snapshot2, err := storage.SnapshotStates(context.Background())
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = storage.RevertStatesToSnapshot(context.Background(), snapshot1)
			if err != nil {
				b.Fatal(err)
			}

			err = storage.RevertStatesToSnapshot(context.Background(), snapshot2)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	err = storage.RevertStatesToSnapshot(context.Background(), snapshot1)
	if err != nil {
		b.Fatal(err)
	}
}
