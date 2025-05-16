package state

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
)

func TestMemoryFinalizerStateContainer(t *testing.T) {
	registry := NewSimpleRegistry()
	lockerGenerator := locker.NewMemoryLockerGenerator()
	// fmt.Printf("TestFinalizerStateContainer: lockerGenerator:%p\n", lockerGenerator)

	err := registry.RegisterState(MustNewTestUserModel(lockerGenerator, "", "", ""))
	if err != nil {
		t.Fatal(err)
	}

	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")
	cache, _ := NewMemoryStorage(lockerGenerator, registry, cacheSnapshot, "")
	cacheSnapshot.SetStorageForSnapshot(cache)

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")
	persist, _ := NewMemoryStorage(lockerGenerator, registry, persistSnapshot, "")
	persistSnapshot.SetStorageForSnapshot(persist)

	finalizer := NewCacheAndPersistFinalizer(2*time.Second, registry, lockerGenerator, cache, persist, "")
	defer finalizer.Close()

	SpecTestFinalizerStateContainer(t, cache, persist, finalizer, lockerGenerator)
}

func TestRedisAndGormFinalizerStateContainer(t *testing.T) {
	db, err := setupTestGormDB()
	if err != nil {
		t.Fatal(err)
	}

	err = db.AutoMigrate(&TestUserModel{}, &FinalizeState{}, &SnapshotState{})
	if err != nil {
		t.Fatal(err)
	}

	rdb := setupRdb(t)
	redisPool := goredis.NewPool(rdb)
	registry := NewSimpleRegistry()
	lockerGenerator := locker.NewRedisLockerGenerator(redisPool)
	// fmt.Printf("TestFinalizerStateContainer: lockerGenerator:%p\n", lockerGenerator)

	err = registry.RegisterState(MustNewTestUserModel(lockerGenerator, "", "", ""))
	if err != nil {
		t.Fatal(err)
	}

	storageFactory := NewRedisStorageFactory(rdb, registry, lockerGenerator, nil)
	cacheSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")
	cache, _ := NewRedisStorage(lockerGenerator, rdb, registry, cacheSnapshot, "")
	cacheSnapshot.SetStorageForSnapshot(cache)

	persistSnapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")
	persist, _ := NewGORMStorage(lockerGenerator, db, registry, persistSnapshot, "")
	persistSnapshot.SetStorageForSnapshot(persist)

	finalizer := NewCacheAndPersistFinalizer(2*time.Second, registry, lockerGenerator, cache, persist, "")
	defer finalizer.Close()

	SpecTestFinalizerStateContainer(t, cache, persist, finalizer, lockerGenerator)
}

func SpecTestFinalizerStateContainer(t *testing.T, cache, persist Storage, finalizer Finalizer, lockerGenerator locker.SyncLockerGenerator) {
	t.Run("Single instance access states", func(t *testing.T) {
		user1Container := NewStateContainer(finalizer, MustNewTestUserModel(lockerGenerator, "partition1", "user1", "server"))
		// Use SyncLocker to make sure concurrent safety accross
		// 1. go-routines if the implementation of SyncLocker is like sync.Mutex or
		// 2. micro-services if the implementation of SyncLocker is distributed locker.
		// t.Logf("user1: %+v", user1Container.Unwrap())
		user1, err := user1Container.GetAndLock(context.Background())
		// user1, err := user1Container.Get()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("User1 lock, locker: %p, generator: %p, user1: %+v\n", user1.GetLocker(), user1.GetLockerGenerator(), user1)

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
		err = user1Container.Save(context.Background())
		// err = user1Container.Wrap(user1).Save()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("User1 unlock, locker: %p, generator: %p\n", user1.GetLocker(), user1.GetLockerGenerator())

		user1.Unlock(context.Background())

		fmt.Printf("User1 unlocked, locker: %p, generator: %p\n", user1.GetLocker(), user1.GetLockerGenerator())

		// Load user1 from cache
		newUser1, err := NewStateContainer(finalizer, MustNewTestUserModel(lockerGenerator, "partition1", "user1", "server")).Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Make sure the changes are in cache
		assert.Equal(t, "user1", newUser1.Name)
		assert.Equal(t, "server", newUser1.Server)
		assert.Equal(t, 18, newUser1.Age)
		assert.Equal(t, 180, newUser1.Height)

		// Load user1 from persist
		_, err = persist.LoadState(context.Background(), user1.StateName(), user1StateID)
		assert.Equal(t, ErrStateNotFound, err)

		// Finalize states into persist database
		err = finalizer.FinalizeAllCachedStates(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Make sure that the changes are in persist after finalization
		_, err = persist.LoadState(context.Background(), user1.StateName(), user1StateID)
		assert.NotEqual(t, err, ErrStateNotFound)
	})

	t.Run("Multiple instances access states", func(t *testing.T) {
		user2Container := NewStateContainer(finalizer, MustNewTestUserModel(lockerGenerator, "partition1", "user2", "serve2"))
		user3Container := NewStateContainer(finalizer, MustNewTestUserModel(lockerGenerator, "partition1", "user3", "serve2"))
		user2Id, _ := user2Container.StateID()
		user3Id, _ := user3Container.StateID()

		// Simulate multiple instances
		var wg sync.WaitGroup
		errChan := make(chan error, 100)

		count := 10
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				defer func() {
					fmt.Printf("goroutine: %d, released locker\n", i)
				}()

				fmt.Printf("goroutine: %d, user2 %v GetAndLock\n", i, user2Id)
				user2, err := user2Container.GetAndLock(context.Background())
				if err != nil {
					errChan <- err
					return
				}
				defer func() {
					err := user2.Unlock(context.Background())
					fmt.Printf("goroutine: %d, released %v locker, err: %v\n", i, "user2 "+user2Id, err)
				}()

				fmt.Printf("goroutine: %d, user3 %v GetAndLock\n", i, user3Id)

				user3, err := user3Container.GetAndLock(context.Background())
				if err != nil {
					errChan <- err
					return
				}
				defer func() {
					err := user3.Unlock(context.Background())
					fmt.Printf("goroutine: %d, released %v locker, err: %v\n", i, "user3 "+user2Id, err)
				}()

				fmt.Printf("goroutine: %d, Update states\n", i)

				user2.Height += 2
				user3.Age++

				err = user2Container.Save(context.Background())
				if err != nil {
					errChan <- err
					return
				}

				err = user3Container.Save(context.Background())
				if err != nil {
					errChan <- err
					return
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

		newUser2, err := user2Container.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		newUser3, err := user3Container.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 2*count, newUser2.Height)
		assert.Equal(t, count, newUser3.Age)
	})
}
