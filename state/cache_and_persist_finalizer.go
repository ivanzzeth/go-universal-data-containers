package state

import (
	"context"
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

	err := registry.RegisterState(MustNewFinalizeState(lockerGenerator, name, name))
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
	GormModel
	BaseState
	Name             string
	LastFinalizeTime time.Time
}

func MustNewFinalizeState(lockerGenerator locker.SyncLockerGenerator, partition, name string) *FinalizeState {
	f := &FinalizeState{GormModel: GormModel{Partition: partition}, Name: name}

	state, err := NewBaseState(lockerGenerator, "finalize_states", NewBase64IDMarshaler("_"), f.StateIDComponents())
	if err != nil {
		panic(fmt.Errorf("failed to create base state: %v", err))
	}

	f.BaseState = *state

	err = f.FillID(f)
	if err != nil {
		panic(fmt.Errorf("invalid stateID: %v", err))
	}

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

func (s *CacheAndPersistFinalizer) FinalizeSnapshot(snapshotID string) (err error) {
	stateContainer := NewStateContainer(s, MustNewFinalizeState(s.lockerGenerator, s.name, s.name))

	finalizeState, err := stateContainer.GetAndLock()

	if err != nil {
		return
	}
	defer finalizeState.Unlock(context.TODO())

	// Double check lastTime
	if time.Since(finalizeState.LastFinalizeTime) <= s.interval {
		return
	}

	finalizeState.LastFinalizeTime = time.Now()

	err = s.finalizeSnapshot(snapshotID)
	if err != nil {
		return err
	}

	err = stateContainer.Save()
	if err != nil {
		return
	}

	return nil
}

func (s *CacheAndPersistFinalizer) finalizeSnapshot(snapshotID string) error {
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

func (s *CacheAndPersistFinalizer) FinalizeAllCachedStates() (err error) {
	stateContainer := NewStateContainer(s, MustNewFinalizeState(s.lockerGenerator, s.name, s.name))

	finalizeState, err := stateContainer.GetAndLock()

	if err != nil {
		return
	}
	defer finalizeState.Unlock(context.TODO())

	// Double check lastTime
	if time.Since(finalizeState.LastFinalizeTime) <= s.interval {
		return
	}

	finalizeState.LastFinalizeTime = time.Now()

	err = s.finalizeAllCachedStates()
	if err != nil {
		return
	}

	err = stateContainer.Save()
	if err != nil {
		return
	}

	return
}

func (s *CacheAndPersistFinalizer) finalizeAllCachedStates() error {
	states, err := s.cache.LoadAllStates()
	if err != nil {
		return err
	}

	err = s.persist.SaveStates(states...)
	if err != nil {
		return err
	}

	err = s.ClearAllCachedStates()
	if err != nil {
		return err
	}

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
				err := s.FinalizeAllCachedStates()
				if err != nil {
					// TODO: logging
					// fmt.Printf("FinalizeAllCachedStates failed: %v\n", err)
					return
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}
