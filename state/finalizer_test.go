package state

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func SpecTestFinalizer(t *testing.T, lockerGenerator locker.SyncLockerGenerator, finalizer Finalizer, finalizerFactory func() Finalizer) {
	t.Run("Test basic cases", func(t *testing.T) {
		err := finalizer.ClearAllCachedStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = finalizer.ClearSnapshots(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		snapshot1, err := finalizer.SnapshotStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Pattern1: LoadState then assert state as *TestUserModel
		user1Name := "user1"
		user1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", user1Name, "server")
		user1StateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}

		_, err = finalizer.LoadState(context.Background(), user1.StateName(), user1StateID)
		assert.Equal(t, err, ErrStateNotFound)

		user1.Name = user1Name
		user1.Server = "server"
		user1.Age = 1
		user1.Height = 1

		err = finalizer.SaveState(context.Background(), user1)
		if err != nil {
			t.Fatal(err)
		}

		snapshot2, err := finalizer.SnapshotStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Pattern2:
		newUser1 := MustNewTestUserModel(locker.NewMemoryLockerGenerator(), "", user1Name, "server")
		err = newUser1.WithStateFinalizer(finalizer).Get()
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, user1Name, newUser1.Name)
		assert.Equal(t, "server", newUser1.Server)
		assert.Equal(t, 1, newUser1.Age)
		assert.Equal(t, 1, newUser1.Height)

		// Revert to snapshot1
		err = finalizer.RevertStatesToSnapshot(context.Background(), snapshot1)
		if err != nil {
			t.Fatal(err)
		}

		_, err = finalizer.LoadState(context.Background(), user1.StateName(), user1StateID)
		assert.Equal(t, ErrStateNotFound, err)

		// Revert to snapshot2
		err = finalizer.RevertStatesToSnapshot(context.Background(), snapshot2)
		if err != nil {
			t.Fatal(err)
		}

		user1.Name = user1Name
		user1.Server = "server"
		user1StateID, err = user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}
		newUserState1, err := finalizer.LoadState(context.Background(), user1.StateName(), user1StateID)
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
	})

	t.Run("Test auto finalize", func(t *testing.T) {
		// Simulate multiple finalizers which runs at different instances at the same time
		finalizers := []Finalizer{
			finalizer,
		}

		for i := 0; i < 10; i++ {
			finalizers = append(finalizers, finalizerFactory())
		}

		for i := 0; i < len(finalizers); i++ {
			err := finalizers[i].ClearAllCachedStates(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			err = finalizers[i].ClearSnapshots(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			// t.Logf("Finalizer %p EnableAutoFinalizeAllCachedStates", finalizers[i])
			finalizers[i].EnableAutoFinalizeAllCachedStates(true)
		}

		user1Container := NewStateContainerWithFinalizer(finalizer, MustNewTestUserModel(lockerGenerator, "", "user1", "server"))
		user1ContainerForPersist := NewStateContainerWithFinalizer(finalizer, MustNewTestUserModel(lockerGenerator, "", "user1", "server"))

		for i := 0; i < 10; i++ {
			user1, err := user1Container.GetAndLock(context.Background())
			if err != nil {
				fmt.Printf("GetAndLock failed: %v\n", err)
				continue
			}

			user1.Age++

			err = user1Container.Save(context.Background())
			if err != nil {
				user1.Unlock(context.Background())

				fmt.Printf("Save failed: %v\n", err)
				continue
			}
			user1.Unlock(context.Background())

			// user1Finalized, err := user1ContainerForPersist.GetFromPersist()
			// if err != nil {
			// 	t.Fatal(err)
			// }

			// t.Logf("user1Finalized-%d: %v", i, user1Finalized)

			time.Sleep(finalizer.GetAutoFinalizeInterval())
		}

		time.Sleep(1 * time.Second)

		user1Cached, err := user1Container.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 10, user1Cached.Age)

		user1Finalized, err := user1ContainerForPersist.GetFromPersist(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 10, user1Finalized.Age)
	})
}
