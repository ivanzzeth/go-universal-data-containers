package state

import "errors"

var (
	_ Finalizer = (*CacheAndPersistFinalizer)(nil)
)

type CacheAndPersistFinalizer struct {
	registry Registry
	StorageSnapshot
	cache   Storage
	persist Storage
}

func NewCacheAndPersistFinalizer(registry Registry, cache Storage, persist Storage) *CacheAndPersistFinalizer {
	return &CacheAndPersistFinalizer{
		registry:        registry,
		StorageSnapshot: cache,
		cache:           cache,
		persist:         persist,
	}
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
		}
	}

	return state, nil
}

func (s *CacheAndPersistFinalizer) SaveState(state State) error {
	return s.cache.SaveStates(state)
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

	err = s.ClearAllCachedStates()
	if err != nil {
		return err
	}

	err = s.cache.ClearSnapshots()
	if err != nil {
		return err
	}

	return nil
}

func (s *CacheAndPersistFinalizer) ClearAllCachedStates() error {
	return s.cache.ClearAllStates()
}
