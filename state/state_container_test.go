package state

import (
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func TestFinalizerStateContainer(t *testing.T) {
	registry := NewSimpleRegistry()
	lockerGenerator := locker.NewMemoryLockerGenerator()
	// fmt.Printf("TestFinalizerStateContainer: lockerGenerator:%p\n", lockerGenerator)

	err := registry.RegisterState(MustNewTestUserModel(lockerGenerator, "", ""))
	if err != nil {
		t.Fatal(err)
	}

	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator)
	cache, _ := NewMemoryStorage(lockerGenerator, registry, cacheSnapshot, "")
	cacheSnapshot.SetStorageForSnapshot(cache)

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator)
	persist, _ := NewMemoryStorage(lockerGenerator, registry, persistSnapshot, "")
	persistSnapshot.SetStorageForSnapshot(persist)

	ticker := time.NewTicker(2 * time.Second).C
	finalizer := NewCacheAndPersistFinalizer(ticker, registry, cache, persist)
	defer finalizer.Close()

	t.Run("Single instance access states", func(t *testing.T) {
		user1Container := NewStateContainer(finalizer, MustNewTestUserModel(lockerGenerator, "user1", "server"))
		// Use sync.Locker to make sure concurrent safety accross
		// 1. go-routines if the implementation of sync.Locker is like sync.Mutex or
		// 2. micro-services if the implementation of sync.Locker is distributed locker.
		// t.Logf("user1: %+v", user1Container.Unwrap())
		user1, err := user1Container.GetAndLock()
		// user1, err := user1Container.Get()
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Printf("User1 lock, locker: %p, generator: %p, user1: %+v\n", user1.GetLocker(), user1.GetLockerGenerator(), user1)

		assert.Equal(t, "user1", user1.Name)
		assert.Equal(t, "server", user1.Server)
		assert.Equal(t, 0, user1.Age)
		assert.Equal(t, 0, user1.Height)

		user1StateID, err := user1.GetIDMarshaler().MarshalStateID(user1.StateIDComponents()...)
		if err != nil {
			t.Fatal(err)
		}

		// Change the value in memory.
		// Locked before
		// user1.Lock()
		user1.Age = 18
		user1.Height = 180

		// Then save changes in memory into cache
		err = user1Container.Save()
		// err = user1Container.Wrap(user1).Save()
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Printf("User1 unlock, locker: %p, generator: %p\n", user1.GetLocker(), user1.GetLockerGenerator())

		user1.Unlock()

		// Load user1 from cache
		newUser1, err := NewStateContainer(finalizer, MustNewTestUserModel(lockerGenerator, "user1", "server")).Get()
		if err != nil {
			t.Fatal(err)
		}

		// Make sure the changes are in cache
		assert.Equal(t, "user1", newUser1.Name)
		assert.Equal(t, "server", newUser1.Server)
		assert.Equal(t, 18, newUser1.Age)
		assert.Equal(t, 180, newUser1.Height)

		// Load user1 from persist
		_, err = persist.LoadState(user1.StateName(), user1StateID)
		assert.Equal(t, err, ErrStateNotFound)

		// Finalize states into persist database
		err = finalizer.FinalizeAllCachedStates()
		if err != nil {
			t.Fatal(err)
		}

		// Make sure that the changes are in persist after finalization
		_, err = persist.LoadState(user1.StateName(), user1StateID)
		assert.NotEqual(t, err, ErrStateNotFound)
	})

	// t.Run("Multiple instances access states", func(t *testing.T) {
	// 	lockGen := locker.NewMemoryLockerGenerator()

	// 	user2Container := NewStateContainer(finalizer, MustNewTestUserModel(lockGen, "user2", "serve2"))
	// 	user3Container := NewStateContainer(finalizer, MustNewTestUserModel(lockGen, "user3", "serve2"))

	// 	// Simulate multiple instances
	// 	var wg sync.WaitGroup
	// 	errChan := make(chan error, 100)
	// 	for i := 0; i < 10; i++ {
	// 		wg.Add(1)
	// 		go func() {
	// 			defer wg.Done()

	// 			log.Printf("goroutine: %d, user2 GetAndLock\n", i)
	// 			user2, err := user2Container.GetAndLock()
	// 			if err != nil {
	// 				errChan <- err
	// 			}
	// 			defer user2.Unlock()

	// 			log.Printf("goroutine: %d, user3 GetAndLock\n", i)

	// 			user3, err := user3Container.GetAndLock()
	// 			if err != nil {
	// 				errChan <- err
	// 			}
	// 			defer user3.Unlock()

	// 			log.Printf("goroutine: %d, Update\n", i)

	// 			user2.Height++
	// 			user3.Age++

	// 			err = user2Container.Save()
	// 			if err != nil {
	// 				errChan <- err
	// 			}

	// 			err = user3Container.Save()
	// 			if err != nil {
	// 				errChan <- err
	// 			}
	// 		}()
	// 	}

	// 	wg.Wait()

	// 	close(errChan)
	// 	for err := range errChan {
	// 		if err != nil {
	// 			t.Fatal(err)
	// 		}
	// 	}
	// })
}
