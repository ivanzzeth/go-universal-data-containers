package state

import (
	"errors"
	"reflect"
	"sync"
)

type StateContainer[T State] struct {
	finalizer Finalizer
	state     T
}

func NewStateContainer[T State](finalizer Finalizer, state T) *StateContainer[T] {
	return &StateContainer[T]{
		finalizer: finalizer,
		state:     state,
	}
}

func (s *StateContainer[T]) Wrap(state T) *StateContainer[T] {
	s.state = state
	return s
}

func (s *StateContainer[T]) GetLocker() (sync.Locker, error) {
	return GetStateLockerByName(s.state.GetLockerGenerator(), s.state.StateName())
}

func (s *StateContainer[T]) GetAndLock() (T, error) {
	locker, err := s.GetLocker()
	if err != nil {
		return s.nilState(), err
	}

	// fmt.Printf("GetAndLock, name: %v locker: %p, generator: %p\n",
	// 	s.state.StateName(), locker, s.state.GetLockerGenerator())

	// Lock first
	locker.Lock()

	// then get the value
	return s.Get()
}

func (s *StateContainer[T]) Get() (T, error) {
	if len(s.state.StateIDComponents()) == 0 {
		return s.nilState(), ErrStateIDComponents
	}

	stateID, err := GetStateID(s.state)
	if err != nil {
		return s.state, err
	}

	state, err := s.finalizer.LoadState(s.state.StateName(), stateID)
	if err != nil {
		if !errors.Is(err, ErrStateNotFound) {
			return s.nilState(), err
		}

		// Not found, then using initial state
		return s.state, nil
	}

	s.state = state.(T)

	return s.state, nil
}

func (s *StateContainer[T]) Unwrap() T {
	return s.state
}

func (s *StateContainer[T]) Save() error {
	return s.finalizer.SaveState(s.state)
}

func (s *StateContainer[T]) Delete() error {
	return s.finalizer.ClearStates(s.state)
}

func (s *StateContainer[T]) DeleteCache() error {
	return s.finalizer.ClearCacheStates(s.state)
}

func (s *StateContainer[T]) nilState() T {
	return reflect.New(reflect.TypeOf(s.state)).Elem().Interface().(T)
}
