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

	LoadState(name string, id string) (State, error)
	LoadAllStates() ([]State, error)
	SaveStates(states ...State) error
	ClearAllStates() error

	GetStateIDs(name string) ([]string, error)
	GetStateNames() ([]string, error)
}

type StorageSnapshot interface {
	SetStorage(storage Storage)
	SnapshotStates() (snapshotID int, err error)
	RevertStatesToSnapshot(snapshotID int) (err error)
	GetSnapshot(snapshotID int) (storage Storage, err error)
	DeleteSnapshot(snapshotID int) (err error)
	ClearSnapshots() (err error)
}
