package state

import (
	"context"
	"errors"
	"fmt"

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
	locker.SyncLocker

	StorageSnapshot

	// Used for distinguishing different storages implementations.
	StorageType() string

	// Used for distinguishing different storages.
	StorageName() string

	LoadState(ctx context.Context, name string, id string) (State, error)
	LoadAllStates(ctx context.Context) ([]State, error)
	SaveStates(ctx context.Context, states ...State) error
	ClearStates(ctx context.Context, states ...State) error
	ClearAllStates(ctx context.Context) error

	GetStateIDs(ctx context.Context, name string) ([]string, error)
	GetStateNames(ctx context.Context) ([]string, error)
}

type StorageSnapshot interface {
	SetStorageForSnapshot(storage Storage)
	GetStorageForSnapshot() (storage Storage)

	SnapshotStates(ctx context.Context) (snapshotID string, err error)
	RevertStatesToSnapshot(ctx context.Context, snapshotID string) (err error)
	GetSnapshot(ctx context.Context, snapshotID string) (storage Storage, err error)
	GetSnapshotIDs(ctx context.Context) (snapshotIDs []string, err error)
	DeleteSnapshot(ctx context.Context, snapshotID string) (err error)
	ClearSnapshots(ctx context.Context) (err error)
}

func GetStorageLockerByName(lockerGenerator locker.SyncLockerGenerator, storageName string) (locker.SyncLocker, error) {
	locker, err := lockerGenerator.CreateSyncLocker(fmt.Sprintf("storage-locker-%v", storageName))
	return locker, err
}
