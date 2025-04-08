package state

import (
	"reflect"
	"sync"
)

var (
	simpleStateRegistry = NewSimpleRegistry()
)

func GetSimpleStateRegistry() *SimpleStateRegistry {
	return simpleStateRegistry
}

type SimpleStateRegistry struct {
	States sync.Map
}

func NewSimpleRegistry() *SimpleStateRegistry {
	return &SimpleStateRegistry{}
}

func (s *SimpleStateRegistry) RegisterState(state State) error {
	if reflect.TypeOf(state).Kind() != reflect.Pointer {
		return ErrStateNotPointer
	}
	s.States.Store(state.StateName(), state)
	return nil
}

func (s *SimpleStateRegistry) NewState(name string) (State, error) {
	registered, ok := s.States.Load(name)
	if !ok {
		return nil, ErrStateNotRegistered
	}

	registeredElemType := reflect.TypeOf(registered).Elem()

	stateInterface := reflect.New(registeredElemType).Interface()
	// fmt.Printf("registered: %v, elem: %v, state: %v, nil state: %v\n", reflect.TypeOf(registered), registeredElemType, reflect.TypeOf(stateInterface), stateInterface == nil)

	state := stateInterface.(State)

	state.SetStateName(name)
	state.SetLocker(registered.(State).GetLocker())
	return state, nil
}
