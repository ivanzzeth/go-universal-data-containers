package state

import "sync"

var (
	_ Storage = (*MemoryStateStorage)(nil)
)

type MemoryStateStorage struct {
	Registry

	m      sync.RWMutex
	States map[string]map[string]State
}

func NewMemoryStateStorage(registry Registry) *MemoryStateStorage {
	return &MemoryStateStorage{
		Registry: registry,
		States:   make(map[string]map[string]State),
	}
}

func (s *MemoryStateStorage) GetStateIDs(name string) ([]string, error) {
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

func (s *MemoryStateStorage) GetStateNames() ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	names := make([]string, 0, len(s.States))
	for name := range s.States {
		names = append(names, name)
	}

	return names, nil
}

func (s *MemoryStateStorage) LoadAllStates() ([]State, error) {
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

func (s *MemoryStateStorage) LoadState(name string, id string) (State, error) {
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

func (s *MemoryStateStorage) SaveStates(states ...State) error {
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

		table[state.StateID()] = state
	}

	return nil
}

func (s *MemoryStateStorage) ClearAllStates() error {
	s.m.Lock()
	defer s.m.Unlock()

	s.States = make(map[string]map[string]State)

	return nil
}
