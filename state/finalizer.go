package state

import "time"

// Finalize uses two types of storage: cache and persist
// read/write from/to cache first and then
// write all cached states into persist storage.
type Finalizer interface {
	StorageSnapshot

	GetCacheStorage() Storage
	GetPersistStorage() Storage

	Close()

	// LoadState loads state from cache first, if not found, load from persist
	// if not found, return ErrStateNotFound
	LoadState(name string, id string) (State, error)

	// SaveState/SaveStates saves state(s) to cache
	SaveState(state State) error
	SaveStates(state ...State) error

	ClearCacheStates(states ...State) error
	ClearPersistStates(states ...State) error
	ClearStates(states ...State) error
	ClearAllCachedStates() error

	// Fianlize* functions are used to finalize snapshot/cached states into persist,
	// and they are only called once at the same time until all states are finalized.
	// It's safe for distributed system if you're using distributed locker like
	// `RedSyncMutexWrapper` implemented in package `locker`
	FinalizeSnapshot(snapshotID string) error
	FinalizeAllCachedStates() error

	// EnableAutoFinalizeAllCachedStates allows you to enable/disable auto finalize.
	// false by default.
	// If you want to finalize all cached states manually, use `FinalizeAllCachedStates`
	// and disable auto finalize feature.
	EnableAutoFinalizeAllCachedStates(enable bool)
	GetAutoFinalizeInterval() time.Duration
}
