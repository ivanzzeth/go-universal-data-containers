package state

import (
	"errors"
	"reflect"
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

func (s *StateContainer[T]) Get() (T, error) {
	if len(s.state.StateIDComponents()) == 0 {
		return reflect.New(reflect.TypeOf(s.state)).Elem().Interface().(T), ErrStateIDComponents
	}

	stateID, err := s.state.GetIDMarshaler().MarshalStateID(s.state.StateIDComponents()...)
	if err != nil {
		return s.state, err
	}

	state, err := s.finalizer.LoadState(s.state.StateName(), stateID)
	if err != nil {
		if !errors.Is(err, ErrStateNotFound) {
			return reflect.New(reflect.TypeOf(s.state)).Elem().Interface().(T), err
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
