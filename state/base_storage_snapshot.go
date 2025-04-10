package state

import (
	"fmt"
	"sync"
)

var (
	_ StorageSnapshot = (*BaseStorageSnapshot)(nil)
)

type BaseStorageSnapshot struct {
	m              sync.RWMutex
	storage        Storage
	storageFactory StorageFactory
	snapshotID     int
	snapshots      map[int]Storage // TODO: DO NOT store it in memory
}

func NewBaseStorageSnapshot(storageFactory StorageFactory) *BaseStorageSnapshot {
	return &BaseStorageSnapshot{
		snapshots:      make(map[int]Storage),
		storageFactory: storageFactory,
	}
}

func (s *BaseStorageSnapshot) SetStorage(storage Storage) {
	s.storage = storage
}

func (s *BaseStorageSnapshot) SnapshotStates() (snapshotID int, err error) {
	// fmt.Printf("SnapshotStates\n")
	s.m.Lock()
	defer s.m.Unlock()

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

func (s *BaseStorageSnapshot) RevertStatesToSnapshot(snapshotID int) (err error) {
	// fmt.Printf("RevertStatesToSnapshot\n")
	s.m.Lock()
	defer s.m.Unlock()

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

func (s *BaseStorageSnapshot) GetSnapshot(snapshotID int) (storage Storage, err error) {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.getSnapshot(snapshotID)
}

func (s *BaseStorageSnapshot) getSnapshot(snapshotID int) (storage Storage, err error) {
	storage, ok := s.snapshots[snapshotID]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	return
}

func (s *BaseStorageSnapshot) DeleteSnapshot(snapshotID int) (err error) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.snapshots, snapshotID)
	return nil
}

func (s *BaseStorageSnapshot) ClearSnapshots() (err error) {
	s.m.Lock()
	defer s.m.Unlock()

	s.snapshots = make(map[int]Storage)
	return nil
}
