package state

import (
	"reflect"
	"sync"
)

var (
	_                   Registry = (*SimpleRegistry)(nil)
	simpleStateRegistry          = NewSimpleRegistry()
)

func GetSimpleStateRegistry() *SimpleRegistry {
	return simpleStateRegistry
}

type SimpleRegistry struct {
	states sync.Map
}

func NewSimpleRegistry() *SimpleRegistry {
	return &SimpleRegistry{}
}

func (s *SimpleRegistry) RegisterState(state State) error {
	if reflect.TypeOf(state).Kind() != reflect.Pointer {
		return ErrStateNotPointer
	}
	s.states.Store(state.StateName(), state)

	return nil
}

func (s *SimpleRegistry) NewState(name string) (State, error) {
	registered, ok := s.states.Load(name)
	if !ok {
		return nil, ErrStateNotRegistered
	}

	registeredElemType := reflect.TypeOf(registered).Elem()

	stateInterface := reflect.New(registeredElemType).Interface()
	// fmt.Printf("registered: %v, elem: %v, state: %v, nil state: %v\n", reflect.TypeOf(registered), registeredElemType, reflect.TypeOf(stateInterface), stateInterface == nil)

	state := stateInterface.(State)

	err := state.Initialize(registered.(State).GetLockerGenerator(), name, registered.(State).GetIDMarshaler(), registered.(State).StateIDComponents())
	if err != nil {
		return nil, err
	}

	return state, nil
}
