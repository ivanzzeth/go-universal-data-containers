package state

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

// StateContainer is helpful to work with state.
// It's a wrapper for state, that provides some useful methods to
// simplify work with state no matter what storage you are using.
// It uses `Finalizer` to finalize state into persist storage to
// speed up your application even distributed system.
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

func (s *StateContainer[T]) StateID() (stateID string, err error) {
	return GetStateID(s.state)
}

func (s *StateContainer[T]) GetLocker() (locker.SyncLocker, error) {
	// fmt.Printf("GetLocker started\n")
	stateID, err := s.StateID()
	if err != nil {
		return nil, err
	}

	fmt.Printf("GetLocker GetStateLockerByName, lockerGenerator: %T, state: %+v\n", s.state.GetLockerGenerator(), s.state)

	locker, err := GetStateLockerByName(s.state.GetLockerGenerator(), s.state.StateName(), stateID)
	if err != nil {
		return nil, err
	}

	// fmt.Printf("GetLocker, name: %v stateID: %v, locker: %p, generator: %p\n",
	// 	s.state.StateName(), stateID, locker, s.state.GetLockerGenerator())

	return locker, nil
}

func (s *StateContainer[T]) GetAndLock(ctx context.Context) (T, error) {
	// fmt.Printf("GetAndLock started\n")
	locker, err := s.GetLocker()
	if err != nil {
		return s.nilState(), err
	}

	// Lock first
	locker.Lock(ctx)

	// then get the value
	return s.Get(ctx)
}

func (s *StateContainer[T]) Get(ctx context.Context) (T, error) {
	if len(s.state.StateIDComponents()) == 0 {
		return s.nilState(), ErrStateIDComponents
	}

	stateID, err := GetStateID(s.state)
	if err != nil {
		return s.state, err
	}

	state, err := s.finalizer.LoadState(ctx, s.state.StateName(), stateID)
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

func (s *StateContainer[T]) GetFromPersist(ctx context.Context) (T, error) {
	if len(s.state.StateIDComponents()) == 0 {
		return s.nilState(), ErrStateIDComponents
	}

	stateID, err := GetStateID(s.state)
	if err != nil {
		return s.state, err
	}

	state, err := s.finalizer.GetPersistStorage().LoadState(ctx, s.state.StateName(), stateID)
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

func (s *StateContainer[T]) Save(ctx context.Context) error {
	return s.finalizer.SaveState(ctx, s.state)
}

func (s *StateContainer[T]) Delete(ctx context.Context) error {
	return s.finalizer.ClearStates(ctx, s.state)
}

func (s *StateContainer[T]) DeleteCache(ctx context.Context) error {
	return s.finalizer.ClearCacheStates(ctx, s.state)
}

func (s *StateContainer[T]) nilState() T {
	return reflect.New(reflect.TypeOf(s.state)).Elem().Interface().(T)
}
