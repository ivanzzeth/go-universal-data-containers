package state

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	_ StorageSnapshot = (*SimpleStorageSnapshot)(nil)
)

type SnapshotState struct {
	GormModel
	BaseState

	SnapshotID string `gorm:"not null;uniqueIndex"`
}

func MustNewSnapshotState(locker sync.Locker, snapshotId string) *SnapshotState {
	state := NewBaseState(locker)
	// Make sure that it's compatible for all storages you want to use
	// For GORMStorage and MemoryStorage, it is ok.
	state.SetStateName("snapshot_states")
	state.SetIDMarshaler(NewBase64IDMarshaler("_"))

	m := &SnapshotState{BaseState: *state, GormModel: GormModel{}, SnapshotID: snapshotId}

	err := m.FillID(m)
	if err != nil {
		panic(fmt.Errorf("invalid stateID: %v", err))
	}

	return m
}

func (u *SnapshotState) StateIDComponents() []any {
	return []any{&u.SnapshotID}
}

type SimpleStorageSnapshot struct {
	storage        Storage
	setStorageOnce sync.Once
	storageFactory StorageFactory
	registry       Registry
}

func NewSimpleStorageSnapshot(registry Registry, storageFactory StorageFactory) *SimpleStorageSnapshot {
	return &SimpleStorageSnapshot{
		storageFactory: storageFactory,
		registry:       registry,
	}
}

func (s *SimpleStorageSnapshot) SetStorageForSnapshot(storage Storage) {
	s.setStorageOnce.Do(func() {
		s.storage = storage
		s.registry.RegisterState(MustNewSnapshotState(&sync.Mutex{}, ""))
	})
}

func (s *SimpleStorageSnapshot) GetStorageForSnapshot() (storage Storage) {
	return s.storage
}

func (s *SimpleStorageSnapshot) SnapshotStates() (snapshotID string, err error) {
	// fmt.Printf("SnapshotStates\n")
	s.storage.Lock()
	defer s.storage.Unlock()

	// fmt.Printf("SnapshotStates LoadAllStates\n")
	states, err := s.storage.LoadAllStates()
	if err != nil {
		return "", err
	}

	// Generate random snapshotID
	snapshotID = uuid.New().String()
	// fmt.Printf("SnapshotStates GetOrCreateStorage\n")
	storage, err := s.createSnapshot(snapshotID)
	if err != nil {
		return "", err
	}

	// fmt.Printf("SnapshotStates SaveStates\n")
	err = storage.SaveStates(states...)
	if err != nil {
		return "", err
	}

	return
}

func (s *SimpleStorageSnapshot) RevertStatesToSnapshot(snapshotID string) (err error) {
	// fmt.Printf("RevertStatesToSnapshot\n")
	s.storage.Lock()
	defer s.storage.Unlock()

	// fmt.Printf("RevertStatesToSnapshot GetSnapshot\n")

	snapshot, err := s.getSnapshot(snapshotID)
	if err != nil {
		return err
	}
	// fmt.Printf("RevertStatesToSnapshot LoadAllStates\n")

	states, err := snapshot.LoadAllStates()
	if err != nil {
		return err
	}

	// fmt.Printf("RevertStatesToSnapshot ClearAllStates\n")

	err = s.storage.ClearAllStates()
	if err != nil {
		return err
	}

	// fmt.Printf("RevertStatesToSnapshot SaveStates\n")

	err = s.storage.SaveStates(states...)
	if err != nil {
		return err
	}

	return nil
}

func (s *SimpleStorageSnapshot) GetSnapshot(snapshotID string) (storage Storage, err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	return s.getSnapshot(snapshotID)
}

func (s *SimpleStorageSnapshot) GetSnapshotIDs() (snapshotIDs []string, err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	return s.getSnapshotIDs()
}

func (s *SimpleStorageSnapshot) getSnapshotIDs() (snapshotIDs []string, err error) {
	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshotState := MustNewSnapshotState(&sync.Mutex{}, "")
	snapshotStateIds, err := sm.GetStateIDs(snapshotState.StateName())
	if err != nil {
		return
	}

	for _, snapshotStateId := range snapshotStateIds {
		snapshot := MustNewSnapshotState(&sync.Mutex{}, "")
		err = NewBase64IDMarshaler("_").UnmarshalStateID(snapshotStateId, &snapshot.ID)
		if err != nil {
			return
		}

		snapshotIDs = append(snapshotIDs, snapshot.ID)
	}

	return
}

func (s *SimpleStorageSnapshot) getSnapshot(snapshotID string) (storage Storage, err error) {
	snapshot := MustNewSnapshotState(&sync.Mutex{}, snapshotID)
	stateID, err := snapshot.GetIDMarshaler().MarshalStateID(snapshot.StateIDComponents()...)
	if err != nil {
		return nil, err
	}

	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshotState, err := sm.LoadState(snapshot.StateName(), stateID)
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

func (s *SimpleStorageSnapshot) createSnapshot(snapshotID string) (storage Storage, err error) {
	snapshot := MustNewSnapshotState(&sync.Mutex{}, snapshotID)
	snapshot.CreatedAt = time.Now()
	snapshot.UpdatedAt = time.Now()

	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	err = sm.SaveStates(snapshot)
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

func (s *SimpleStorageSnapshot) DeleteSnapshot(snapshotID string) (err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	return s.deleteSnapshot(snapshotID)
}

func (s *SimpleStorageSnapshot) deleteSnapshot(snapshotID string) (err error) {
	storage, err := s.getSnapshot(snapshotID)
	if err != nil {
		return err
	}

	err = storage.ClearAllStates()
	if err != nil {
		return err
	}

	sm, err := s.getSnapshotManagementStorage()
	if err != nil {
		return
	}

	snapshot := MustNewSnapshotState(&sync.Mutex{}, snapshotID)
	err = sm.ClearStates(snapshot)
	if err != nil {
		return
	}

	return
}

func (s *SimpleStorageSnapshot) ClearSnapshots() (err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	snapshotIds, err := s.getSnapshotIDs()
	if err != nil {
		return
	}

	for _, snapshotId := range snapshotIds {
		err = s.deleteSnapshot(snapshotId)
		if err != nil {
			return err
		}
	}

	return nil
}
