package state

import (
	"sync"
	"time"
)

var (
	_ Storage = (*MemoryStorage)(nil)
)

type MemoryStorageFactory struct {
	registry    Registry
	newSnapshot func(storageFactory StorageFactory) StorageSnapshot
	table       sync.Map
}

func NewMemoryStorageFactory(registry Registry, newSnapshot func(storageFactory StorageFactory) StorageSnapshot) *MemoryStorageFactory {
	return &MemoryStorageFactory{registry: registry, newSnapshot: newSnapshot}
}

func (f *MemoryStorageFactory) GetOrCreateStorage(name string) (Storage, error) {
	// fmt.Printf("GetOrCreateStorage: %v\n", name)

	storeVal, _ := f.table.LoadOrStore(name, func() interface{} {
		if f.newSnapshot == nil {
			f.newSnapshot = func(storageFactory StorageFactory) StorageSnapshot {
				return NewBaseStorageSnapshot(f)
			}
		}
		snapshot := f.newSnapshot(f)
		storage := NewMemoryStorage(f.registry, snapshot)
		return storage
	}())

	// fmt.Printf("GetOrCreateStorage storage: %v\n", reflect.TypeOf(storeVal))
	return storeVal.(Storage), nil
}

type MemoryStorage struct {
	registry Registry

	m sync.RWMutex
	StorageSnapshot

	// Only used for simulating network latency
	delay  time.Duration
	States map[string]map[string]State
}

func NewMemoryStorage(registry Registry, snapshot StorageSnapshot) *MemoryStorage {
	s := &MemoryStorage{
		registry: registry,
		States:   make(map[string]map[string]State),
	}

	snapshot.SetStorage(s)
	s.StorageSnapshot = snapshot
	return s
}

func (s *MemoryStorage) setDelay(delay time.Duration) {
	s.delay = delay
}

func (s *MemoryStorage) GetStateIDs(name string) ([]string, error) {
	time.Sleep(s.delay)

	s.m.RLock()
	defer s.m.RUnlock()

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

	s.m.RLock()
	defer s.m.RUnlock()

	names := make([]string, 0, len(s.States))
	for name := range s.States {
		names = append(names, name)
	}

	return names, nil
}

func (s *MemoryStorage) LoadAllStates() ([]State, error) {
	time.Sleep(s.delay)

	s.m.RLock()
	defer s.m.RUnlock()

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

	s.m.RLock()
	defer s.m.RUnlock()

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

	s.m.Lock()
	defer s.m.Unlock()

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

func (s *MemoryStorage) ClearAllStates() error {
	time.Sleep(s.delay)

	s.m.Lock()
	defer s.m.Unlock()

	s.States = make(map[string]map[string]State)

	return nil
}
