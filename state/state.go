package state

import (
	"errors"
	"sync"
)

var (
	ErrStateNotFound      = errors.New("state not found")
	ErrStateNotRegistered = errors.New("state not registered")
	ErrStateNotPointer    = errors.New("state must be a pointer")
)

type Storage interface {
	LoadState(name string, id string) (State, error)
	LoadAllStates() ([]State, error)
	SaveStates(states ...State) error
	ClearAllStates() error

	GetStateIDs(name string) ([]string, error)
	GetStateNames() ([]string, error)
}

type State interface {
	StateName() string
	StateID() string

	SetStateName(name string)
	SetStateID(id string)

	GetLocker() sync.Locker
	SetLocker(locker sync.Locker)
	sync.Locker
}
