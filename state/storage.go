package state

import "errors"

var (
	ErrSnapshotNotFound = errors.New("snapshot not found")
)

type StorageFactory interface {
	GetOrCreateStorage(name string) (Storage, error)
}

type Storage interface {
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
