package state

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	_ Finalizer = (*CacheAndPersistFinalizer)(nil)
)

type CacheAndPersistFinalizer struct {
	ticker <-chan time.Time

	registry Registry
	StorageSnapshot
	cache               Storage
	persist             Storage
	autoFinalizeEnabled atomic.Bool
	exitChannel         chan struct{}
}

func NewCacheAndPersistFinalizer(ticker <-chan time.Time, registry Registry, cache Storage, persist Storage) *CacheAndPersistFinalizer {
	f := &CacheAndPersistFinalizer{
		ticker:          ticker,
		registry:        registry,
		StorageSnapshot: cache,
		cache:           cache,
		persist:         persist,

		exitChannel: make(chan struct{}),
	}

	go f.run()

	return f
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
					continue
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}
