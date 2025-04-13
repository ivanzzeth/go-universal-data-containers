package state

import (
	"fmt"
	"sync"
)

var (
	_ StorageSnapshot = (*SimpleStorageSnapshot)(nil)
)

type SimpleStorageSnapshot struct {
	storage        Storage
	setStorageOnce sync.Once
	storageFactory StorageFactory
	snapshotID     int
	snapshots      map[int]Storage // TODO: DO NOT store it in memory
}

func NewSimpleStorageSnapshot(storageFactory StorageFactory) *SimpleStorageSnapshot {
	return &SimpleStorageSnapshot{
		snapshots:      make(map[int]Storage),
		storageFactory: storageFactory,
	}
}

func (s *SimpleStorageSnapshot) SetStorage(storage Storage) {
	s.setStorageOnce.Do(func() {
		s.storage = storage
	})
}

func (s *SimpleStorageSnapshot) SnapshotStates() (snapshotID int, err error) {
	// fmt.Printf("SnapshotStates\n")
	s.storage.Lock()
	defer s.storage.Unlock()

	// fmt.Printf("SnapshotStates LoadAllStates\n")
	states, err := s.storage.LoadAllStates()
	if err != nil {
		return 0, err
	}

	s.snapshotID++
	snapshotID = s.snapshotID

	// fmt.Printf("SnapshotStates GetOrCreateStorage\n")
	s.snapshots[snapshotID], err = s.storageFactory.GetOrCreateStorage(fmt.Sprintf("snapshot-%d", snapshotID))
	if err != nil {
		return 0, err
	}

	// fmt.Printf("SnapshotStates SaveStates\n")
	err = s.snapshots[snapshotID].SaveStates(states...)
	if err != nil {
		return 0, err
	}

	return
}

func (s *SimpleStorageSnapshot) RevertStatesToSnapshot(snapshotID int) (err error) {
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

func (s *SimpleStorageSnapshot) GetSnapshot(snapshotID int) (storage Storage, err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	return s.getSnapshot(snapshotID)
}

func (s *SimpleStorageSnapshot) getSnapshot(snapshotID int) (storage Storage, err error) {
	storage, ok := s.snapshots[snapshotID]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	return
}

func (s *SimpleStorageSnapshot) DeleteSnapshot(snapshotID int) (err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	delete(s.snapshots, snapshotID)
	return nil
}

func (s *SimpleStorageSnapshot) ClearSnapshots() (err error) {
	s.storage.Lock()
	defer s.storage.Unlock()

	s.snapshots = make(map[int]Storage)
	return nil
}
