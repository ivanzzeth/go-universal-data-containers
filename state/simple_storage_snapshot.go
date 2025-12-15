package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	_ StorageSnapshot = (*SimpleStorageSnapshot)(nil)
)

type SnapshotState struct {
	GormModel
	BaseState

	SnapshotID string `gorm:"not null;index"`
}

func MustNewSnapshotState(lockerGenerator locker.SyncLockerGenerator, name, snapshotId string) *SnapshotState {
	// Make sure that it's compatible for all storages you want to use
	// For GORMStorage and MemoryStorage, it is ok.
	m := &SnapshotState{GormModel: GormModel{Partition: name}, SnapshotID: snapshotId}

	state, err := NewBaseState(lockerGenerator, "snapshot_states", NewBase64IDMarshaler("_"), m.StateIDComponents())
	if err != nil {
		panic(fmt.Errorf("failed to create base state: %v", err))
	}

	m.BaseState = *state

	err = m.FillID(m)
	if err != nil {
		panic(fmt.Errorf("invalid stateID: %v", err))
	}

	return m
}

func (u *SnapshotState) StateIDComponents() StateIDComponents {
	return []any{&u.Partition, &u.SnapshotID}
}

type SimpleStorageSnapshot struct {
	name            string
	storage         Storage
	setStorageOnce  sync.Once
	storageFactory  StorageFactory
	lockerGenerator locker.SyncLockerGenerator
	registry        Registry
}

func NewSimpleStorageSnapshot(registry Registry, storageFactory StorageFactory, lockerGenerator locker.SyncLockerGenerator, name string) *SimpleStorageSnapshot {
	if name == "" {
		name = "default"
	}

	return &SimpleStorageSnapshot{
		storageFactory:  storageFactory,
		registry:        registry,
		lockerGenerator: lockerGenerator,
		name:            name,
	}
}

func (s *SimpleStorageSnapshot) SetStorageForSnapshot(storage Storage) {
	s.setStorageOnce.Do(func() {
		s.storage = storage
		s.registry.RegisterState(MustNewSnapshotState(s.lockerGenerator, s.name, ""))
	})
}

func (s *SimpleStorageSnapshot) GetStorageForSnapshot() (storage Storage) {
	return s.storage
}

func (s *SimpleStorageSnapshot) SnapshotStates(ctx context.Context) (snapshotID string, err error) {
	// fmt.Printf("SnapshotStates\n")
	err = s.storage.Lock(ctx)
	if err != nil {
		return "", err
	}

	defer s.storage.Unlock(ctx)

	// fmt.Printf("SnapshotStates LoadAllStates\n")
	states, err := s.storage.LoadAllStates(ctx)
	if err != nil {
		return "", err
	}

	// Generate random snapshotID
	snapshotID = uuid.New().String()
	// fmt.Printf("SnapshotStates GetOrCreateStorage\n")
	storage, err := s.createSnapshot(ctx, snapshotID)
	if err != nil {
		return "", err
	}

	// fmt.Printf("SnapshotStates SaveStates\n")
	err = storage.SaveStates(ctx, states...)
	if err != nil {
		return "", err
	}

	return
}

func (s *SimpleStorageSnapshot) RevertStatesToSnapshot(ctx context.Context, snapshotID string) (err error) {
	// fmt.Printf("RevertStatesToSnapshot\n")
	err = s.storage.Lock(ctx)
	if err != nil {
		return err
	}

	defer s.storage.Unlock(ctx)

	// fmt.Printf("RevertStatesToSnapshot GetSnapshot\n")

	snapshot, err := s.getSnapshot(ctx, snapshotID)
	if err != nil {
		return err
	}
	// fmt.Printf("RevertStatesToSnapshot LoadAllStates\n")

	states, err := snapshot.LoadAllStates(ctx)
	if err != nil {
		return err
	}

	// fmt.Printf("RevertStatesToSnapshot ClearAllStates\n")

	err = s.storage.ClearAllStates(ctx)
	if err != nil {
		return err
	}

	// fmt.Printf("RevertStatesToSnapshot SaveStates\n")

	err = s.storage.SaveStates(ctx, states...)
	if err != nil {
		return err
	}

	return nil
}

func (s *SimpleStorageSnapshot) GetSnapshot(ctx context.Context, snapshotID string) (storage Storage, err error) {
	err = s.storage.Lock(ctx)
	if err != nil {
		return
	}
	defer s.storage.Unlock(ctx)

	return s.getSnapshot(ctx, snapshotID)
}

func (s *SimpleStorageSnapshot) GetSnapshotIDs(ctx context.Context) (snapshotIDs []string, err error) {
	err = s.storage.Lock(ctx)
	if err != nil {
		return
	}
	defer s.storage.Unlock(ctx)

	return s.getSnapshotIDs(ctx)
}

func (s *SimpleStorageSnapshot) getSnapshotIDs(ctx context.Context) (snapshotIDs []string, err error) {
	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshotState := MustNewSnapshotState(s.lockerGenerator, s.name, "")
	snapshotStateIds, err := sm.GetStateIDs(ctx, snapshotState.StateName())
	if err != nil {
		return
	}

	for _, snapshotStateId := range snapshotStateIds {
		snapshot := MustNewSnapshotState(s.lockerGenerator, s.name, "")
		err = NewBase64IDMarshaler("_").UnmarshalStateID(snapshotStateId, snapshot.StateIDComponents()...)
		if err != nil {
			return
		}

		snapshotIDs = append(snapshotIDs, snapshot.SnapshotID)
	}

	return
}

func (s *SimpleStorageSnapshot) getSnapshot(ctx context.Context, snapshotID string) (storage Storage, err error) {
	snapshot := MustNewSnapshotState(s.lockerGenerator, s.name, snapshotID)
	stateID, err := GetStateID(snapshot)
	if err != nil {
		return nil, err
	}

	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshotState, err := sm.LoadState(ctx, snapshot.StateName(), stateID)
	if err != nil {
		if errors.Is(err, ErrStateNotFound) {
			return nil, ErrSnapshotNotFound
		}

		return nil, err
	}

	snapshot = snapshotState.(*SnapshotState)
	_ = snapshot

	storage, err = s.storageFactory.GetOrCreateStorage(fmt.Sprintf("snapshot-%v", snapshotID))

	return
}

func (s *SimpleStorageSnapshot) createSnapshot(ctx context.Context, snapshotID string) (storage Storage, err error) {
	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshot := MustNewSnapshotState(s.lockerGenerator, s.name, snapshotID)

	err = snapshot.Lock(ctx)
	if err != nil {
		return
	}
	defer snapshot.Unlock(ctx)

	snapshot.CreatedAt = time.Now()
	snapshot.UpdatedAt = time.Now()

	// fmt.Printf("SnapshotStates createSnapshot: %+v\n", snapshot)
	err = sm.SaveStates(ctx, snapshot)
	if err != nil {
		return
	}

	storage, err = s.storageFactory.GetOrCreateStorage(fmt.Sprintf("snapshot-%v", snapshotID))

	return
}

func (s *SimpleStorageSnapshot) getSnapshotManagementStorage() (storage Storage, err error) {
	storage, err = s.storageFactory.GetOrCreateStorage("snapshot-management")

	return
}

func (s *SimpleStorageSnapshot) DeleteSnapshot(ctx context.Context, snapshotID string) (err error) {
	err = s.storage.Lock(ctx)
	if err != nil {
		return
	}
	defer s.storage.Unlock(ctx)

	return s.deleteSnapshot(ctx, snapshotID)
}

func (s *SimpleStorageSnapshot) deleteSnapshot(ctx context.Context, snapshotID string) (err error) {
	storage, err := s.getSnapshot(ctx, snapshotID)
	if err != nil {
		return err
	}

	err = storage.ClearAllStates(ctx)
	if err != nil {
		return err
	}

	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshot := MustNewSnapshotState(s.lockerGenerator, s.name, snapshotID)
	err = sm.ClearStates(ctx, snapshot)
	if err != nil {
		return
	}

	return
}

func (s *SimpleStorageSnapshot) ClearSnapshots(ctx context.Context) (err error) {
	err = s.storage.Lock(ctx)
	if err != nil {
		return
	}
	defer s.storage.Unlock(ctx)

	snapshotIds, err := s.getSnapshotIDs(ctx)
	if err != nil {
		return
	}

	for _, snapshotId := range snapshotIds {
		err = s.deleteSnapshot(ctx, snapshotId)
		if err != nil {
			return err
		}
	}

	return nil
}
