package state

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	_ Finalizer = (*CacheAndPersistFinalizer)(nil)
)

type CacheAndPersistFinalizer struct {
	ticker   <-chan time.Time
	interval time.Duration

	registry        Registry
	lockerGenerator locker.SyncLockerGenerator
	name            string

	StorageSnapshot
	cache               Storage
	persist             Storage
	autoFinalizeEnabled atomic.Bool
	exitChannel         chan struct{}
}

func NewCacheAndPersistFinalizer(interval time.Duration, registry Registry, lockerGenerator locker.SyncLockerGenerator, cache Storage, persist Storage, name string) *CacheAndPersistFinalizer {
	ticker := time.NewTicker(interval).C

	if name == "" {
		name = "default"
	}

	err := registry.RegisterState(MustNewFinalizeState(lockerGenerator, name))
	if err != nil {
		panic(fmt.Errorf("failed to register finalize state: %v", err))
	}

	f := &CacheAndPersistFinalizer{
		ticker:   ticker,
		interval: interval,

		registry: registry,
		name:     name,

		lockerGenerator: lockerGenerator,
		StorageSnapshot: cache,
		cache:           cache,
		persist:         persist,

		exitChannel: make(chan struct{}),
	}

	go f.run()

	return f
}

type FinalizeState struct {
	BaseState
	Name             string
	LastFinalizeTime time.Time
}

func MustNewFinalizeState(lockerGenerator locker.SyncLockerGenerator, name string) *FinalizeState {
	f := &FinalizeState{Name: name}

	state, err := NewBaseState(lockerGenerator, "finalize_states", NewBase64IDMarshaler("_"), f.StateIDComponents())
	if err != nil {
		panic(fmt.Errorf("failed to create base state: %v", err))
	}

	f.BaseState = *state

	return f
}

func (u *FinalizeState) StateIDComponents() StateIDComponents {
	return []any{&u.Name}
}

func (s *CacheAndPersistFinalizer) Close() {
	close(s.exitChannel)
}

func (s *CacheAndPersistFinalizer) LoadState(name string, id string) (State, error) {
	state, err := s.cache.LoadState(name, id)
	if err != nil {
		if !errors.Is(err, ErrStateNotFound) {
			return nil, err
		}

		state, err = s.persist.LoadState(name, id)
		if err != nil {
			if !errors.Is(err, ErrStateNotFound) {
				return nil, err
			}

			state, err = s.registry.NewState(name)
			if err != nil {
				return nil, err
			}

			return state, ErrStateNotFound
		}
	}

	return state, nil
}

func (s *CacheAndPersistFinalizer) SaveState(state State) error {
	return s.cache.SaveStates(state)
}

func (s *CacheAndPersistFinalizer) SaveStates(states ...State) error {
	return s.cache.SaveStates(states...)
}

func (s *CacheAndPersistFinalizer) ClearCacheStates(states ...State) error {
	return s.cache.ClearStates(states...)
}

func (s *CacheAndPersistFinalizer) ClearPersistStates(states ...State) error {
	return s.persist.ClearStates(states...)
}

func (s *CacheAndPersistFinalizer) ClearStates(states ...State) error {
	err := s.ClearCacheStates(states...)
	if err != nil {
		return err
	}

	err = s.ClearPersistStates(states...)
	if err != nil {
		return err
	}

	return nil
}

func (s *CacheAndPersistFinalizer) FinalizeSnapshot(snapshotID string) error {
	snapshot, err := s.StorageSnapshot.GetSnapshot(snapshotID)
	if err != nil {
		return err
	}

	allStates, err := snapshot.LoadAllStates()
	if err != nil {
		return err
	}

	err = s.persist.SaveStates(allStates...)
	if err != nil {
		return err
	}

	return nil
}

func (s *CacheAndPersistFinalizer) FinalizeAllCachedStates() error {
	states, err := s.cache.LoadAllStates()
	if err != nil {
		return err
	}

	err = s.persist.SaveStates(states...)
	if err != nil {
		return err
	}

	// err = s.ClearAllCachedStates()
	// if err != nil {
	// 	return err
	// }

	// MUST clear snapshots manully
	// err = s.cache.ClearSnapshots()
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (s *CacheAndPersistFinalizer) ClearAllCachedStates() error {
	return s.cache.ClearAllStates()
}

func (s *CacheAndPersistFinalizer) EnableAutoFinalizeAllCachedStates(enable bool) {
	s.autoFinalizeEnabled.Store(enable)
}

func (s *CacheAndPersistFinalizer) GetAutoFinalizeInterval() time.Duration {
	return s.interval
}

func (s *CacheAndPersistFinalizer) GetCacheStorage() Storage {
	return s.cache
}

func (s *CacheAndPersistFinalizer) GetPersistStorage() Storage {
	return s.persist
}

func (s *CacheAndPersistFinalizer) run() {
	for {
		select {
		case <-s.exitChannel:
			return
		case <-s.ticker:
			if s.autoFinalizeEnabled.Load() {
				func() {
					// log.Printf("FinalizeAllCachedStates started..., finalizer: %p\n", s)

					stateContainer := NewStateContainer(s, MustNewFinalizeState(s.lockerGenerator, s.name))

					// log.Printf("FinalizeAllCachedStates getAndLock..., finalizer: %p\n", s)

					finalizeState, err := stateContainer.GetAndLock()
					// log.Printf("FinalizeAllCachedStates getAndLock2..., finalizer: %p\n", s)

					if err != nil {
						// TODO: logging
						// log.Printf("FinalizeAllCachedStates getAndLock3..., finalizer: %p, err: %v\n", s, err)
						return
					}
					// var finalized bool

					// defer func() {
					// 	if finalized {
					// 		log.Printf("FinalizeAllCachedStates finalized..., finalizer: %p\n", s)
					// 	}
					// }()

					defer finalizeState.Unlock()

					// Double check lastTime
					// log.Printf("FinalizeAllCachedStates check lastTime..., finalizer: %p\n", s)

					if time.Since(finalizeState.LastFinalizeTime) <= s.interval {
						return
					}

					// log.Printf("FinalizeAllCachedStates finalizing..., finalizer: %p\n", s)

					finalizeState.LastFinalizeTime = time.Now()

					err = s.FinalizeAllCachedStates()
					if err != nil {
						// TODO: logging
						// fmt.Printf("FinalizeAllCachedStates failed: %v\n", err)
						return
					}

					err = stateContainer.Save()
					if err != nil {
						// TODO: logging
						// fmt.Printf("Save failed: %v\n", err)
						return
					}

					// finalized = true
				}()
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}
