package state

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

var (
	_ StorageSnapshot = (*SimpleStorageSnapshot)(nil)
)

type SnapshotState struct {
	GormModel
	BaseState
}

func MustNewSnapshotState(locker sync.Locker, id string) *SnapshotState {
	state := NewBaseState(locker)
	// Make sure that it's compatible for all storages you want to use
	// For GORMStorage and MemoryStorage, it is ok.
	state.SetStateName("snapshot_states")
	state.SetIDMarshaler(NewJsonIDMarshaler("-"))

	m := &SnapshotState{BaseState: *state, GormModel: GormModel{ID: id}}

	err := m.FillID(m)
	if err != nil {
		panic(fmt.Errorf("invalid stateID: %v", err))
	}

	return m
}

func (u *SnapshotState) StateIDComponents() []any {
	return []any{&u.GormModel.ID}
}

type SimpleStorageSnapshot struct {
	storage        Storage
	setStorageOnce sync.Once
	storageFactory StorageFactory
}

func NewSimpleStorageSnapshot(storageFactory StorageFactory) *SimpleStorageSnapshot {
	return &SimpleStorageSnapshot{
		storageFactory: storageFactory,
	}
}

func (s *SimpleStorageSnapshot) SetStorage(storage Storage) {
	s.setStorageOnce.Do(func() {
		s.storage = storage
	})
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
	storage, err := s.storageFactory.GetOrCreateStorage(fmt.Sprintf("snapshot-%s", snapshotID))
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

func (s *SimpleStorageSnapshot) getSnapshot(snapshotID string) (storage Storage, err error) {
	storage, err = s.storageFactory.GetOrCreateStorage(fmt.Sprintf("snapshot-%v", snapshotID))

	return
}

func (s *SimpleStorageSnapshot) DeleteSnapshot(snapshotID string) (err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	storage, err := s.getSnapshot(snapshotID)
	if err != nil {
		return err
	}

	err = storage.ClearAllStates()
	if err != nil {
		return err
	}

	return nil
}

func (s *SimpleStorageSnapshot) ClearSnapshots() (err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	snapshotState := MustNewSnapshotState(&sync.Mutex{}, "")
	snapshotIds, err := s.storage.GetStateIDs(snapshotState.StateName())
	if err != nil {
		return err
	}

	for _, snapshotId := range snapshotIds {
		err = s.DeleteSnapshot(snapshotId)
		if err != nil {
			return err
		}
	}
	return nil
}
