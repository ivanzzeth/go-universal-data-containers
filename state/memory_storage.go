package state

import (
	"fmt"
	"sync"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	_ Storage = (*MemoryStorage)(nil)
)

type MemoryStorageFactory struct {
	registry Registry
	locker.SyncLockerGenerator
	newSnapshot func(storageFactory StorageFactory) StorageSnapshot
	table       sync.Map
}

func NewMemoryStorageFactory(registry Registry, lockerGenerator locker.SyncLockerGenerator, newSnapshot func(storageFactory StorageFactory) StorageSnapshot) *MemoryStorageFactory {
	return &MemoryStorageFactory{registry: registry, SyncLockerGenerator: lockerGenerator, newSnapshot: newSnapshot}
}

func (f *MemoryStorageFactory) GetOrCreateStorage(name string) (Storage, error) {
	// fmt.Printf("GetOrCreateStorage: %v\n", name)

	onceVal, _ := f.table.LoadOrStore(fmt.Sprintf("%v-once", name), &sync.Once{})

	var err error
	onceVal.(*sync.Once).Do(func() {
		if f.newSnapshot == nil {
			f.newSnapshot = func(storageFactory StorageFactory) StorageSnapshot {
				return NewSimpleStorageSnapshot(f.registry, f, f.SyncLockerGenerator)
			}
		}
		snapshot := f.newSnapshot(f)
		var storage Storage
		storage, err = NewMemoryStorage(f.SyncLockerGenerator, f.registry, snapshot, name)
		storage = NewStorageWithMetrics(storage)
		f.table.LoadOrStore(name, storage)
	})
	if err != nil {
		f.table.Delete(fmt.Sprintf("%v-once", name))
		return nil, err
	}

	storeVal, loaded := f.table.Load(name)
	if !loaded {
		return nil, ErrStorageNotFound
	}

	return storeVal.(Storage), nil
}

type MemoryStorage struct {
	registry Registry

	locker sync.Locker
	StorageSnapshot

	name string

	// Only used for simulating network latency
	delay time.Duration

	stateLocker sync.Locker

	States map[string]map[string]State
}

func NewMemoryStorage(lockerGenerator locker.SyncLockerGenerator, registry Registry, snapshot StorageSnapshot, name string) (*MemoryStorage, error) {
	if name == "" {
		name = "default"
	}

	locker, err := GetStateLockerByName(lockerGenerator, name)
	if err != nil {
		return nil, err
	}

	s := &MemoryStorage{
		locker:      locker,
		stateLocker: &sync.Mutex{},
		registry:    registry,
		name:        name,
		States:      make(map[string]map[string]State),
	}

	snapshot.SetStorageForSnapshot(s)
	s.StorageSnapshot = snapshot
	return s, nil
}

func (s *MemoryStorage) setDelay(delay time.Duration) {
	s.delay = delay
}

func (s *MemoryStorage) StorageType() string {
	return "memory"
}

func (s *MemoryStorage) StorageName() string {
	return s.name
}

func (s *MemoryStorage) Lock() {
	s.locker.Lock()
}

func (s *MemoryStorage) Unlock() {
	s.locker.Unlock()
}

func (s *MemoryStorage) GetStateIDs(name string) ([]string, error) {
	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	table, ok := s.States[name]
	if !ok {
		table = make(map[string]State)
		s.States[name] = table
	}

	ids := make([]string, 0, len(table))
	for id := range table {
		ids = append(ids, id)
	}

	return ids, nil
}

func (s *MemoryStorage) GetStateNames() ([]string, error) {
	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	names := make([]string, 0, len(s.States))
	for name := range s.States {
		names = append(names, name)
	}

	return names, nil
}

func (s *MemoryStorage) LoadAllStates() ([]State, error) {
	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	states := make([]State, 0, len(s.States))
	for _, table := range s.States {
		for _, state := range table {
			states = append(states, state)
		}
	}

	return states, nil
}

func (s *MemoryStorage) LoadState(name string, id string) (State, error) {
	_, err := s.registry.NewState(name)
	if err != nil {
		return nil, err
	}

	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	table, ok := s.States[name]
	if !ok {
		table = make(map[string]State)
		s.States[name] = table
	}

	state, ok := table[id]
	if !ok {
		return nil, ErrStateNotFound
	}

	if state == nil {
		return nil, ErrStateNotFound
	}

	return state, nil
}

func (s *MemoryStorage) SaveStates(states ...State) error {
	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	for _, state := range states {
		if state == nil {
			continue
		}

		table, ok := s.States[state.StateName()]
		if !ok {
			table = make(map[string]State)
			s.States[state.StateName()] = table
		}

		stateId, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}

		table[stateId] = state
	}

	return nil
}

func (s *MemoryStorage) ClearStates(states ...State) error {
	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	for _, state := range states {
		if state == nil {
			continue
		}

		table, ok := s.States[state.StateName()]
		if ok {
			stateId, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
			if err != nil {
				return err
			}

			delete(table, stateId)
			// fmt.Printf("Delete state: %s -> %s\n", state.StateName(), stateId)
		}
	}

	return nil
}

func (s *MemoryStorage) ClearAllStates() error {
	time.Sleep(s.delay)

	s.stateLocker.Lock()
	defer s.stateLocker.Unlock()

	s.States = make(map[string]map[string]State)

	return nil
}
