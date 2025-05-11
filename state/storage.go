package state

import (
	"errors"
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	ErrSnapshotNotFound = errors.New("snapshot not found")
	ErrStorageNotFound  = errors.New("storage not found")
)

type StorageFactory interface {
	locker.SyncLockerGenerator

	GetOrCreateStorage(name string) (Storage, error)
}

type Storage interface {
	// Please use locker to protect the storage in concurrent
	sync.Locker

	StorageSnapshot

	// Used for distinguishing different storages implementations.
	StorageType() string

	// Used for distinguishing different storages.
	StorageName() string

	LoadState(name string, id string) (State, error)
	LoadAllStates() ([]State, error)
	SaveStates(states ...State) error
	ClearStates(states ...State) error
	ClearAllStates() error

	GetStateIDs(name string) ([]string, error)
	GetStateNames() ([]string, error)
}

type StorageSnapshot interface {
	SetStorageForSnapshot(storage Storage)
	GetStorageForSnapshot() (storage Storage)

	SnapshotStates() (snapshotID string, err error)
	RevertStatesToSnapshot(snapshotID string) (err error)
	GetSnapshot(snapshotID string) (storage Storage, err error)
	GetSnapshotIDs() (snapshotIDs []string, err error)
	DeleteSnapshot(snapshotID string) (err error)
	ClearSnapshots() (err error)
}
