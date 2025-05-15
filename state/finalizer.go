package state

import (
	"context"
	"time"
)

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
	LoadState(ctx context.Context, name string, id string) (State, error)

	// SaveState/SaveStates saves state(s) to cache
	SaveState(ctx context.Context, state State) error
	SaveStates(ctx context.Context, state ...State) error

	ClearCacheStates(ctx context.Context, states ...State) error
	ClearPersistStates(ctx context.Context, states ...State) error
	ClearStates(ctx context.Context, states ...State) error
	ClearAllCachedStates(ctx context.Context) error

	// Fianlize* functions are used to finalize snapshot/cached states into persist,
	// and they are only called once at the same time until all states are finalized.
	// It's safe for distributed system if you're using distributed locker like
	// `RedSyncMutexWrapper` implemented in package `locker`
	FinalizeSnapshot(ctx context.Context, snapshotID string) error
	FinalizeAllCachedStates(ctx context.Context) error

	// EnableAutoFinalizeAllCachedStates allows you to enable/disable auto finalize.
	// false by default.
	// If you want to finalize all cached states manually, use `FinalizeAllCachedStates`
	// and disable auto finalize feature.
	EnableAutoFinalizeAllCachedStates(enable bool)
	GetAutoFinalizeInterval() time.Duration
}
