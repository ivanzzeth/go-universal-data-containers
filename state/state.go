package state

import (
	"errors"
	"sync"
)

var (
	ErrStateNotFound      = errors.New("state not found")
	ErrStateNotRegistered = errors.New("state not registered")
	ErrStateNotPointer    = errors.New("state must be a pointer")
	ErrStateIDComponents  = errors.New("state id components must not be empty")
)

type State interface {
	StateName() string
	SetStateName(name string)

	GetIDMarshaler() IDMarshaler
	SetIDMarshaler(IDMarshaler)

	StateIDComponents() []any

	GetLocker() sync.Locker
	SetLocker(locker sync.Locker)
	sync.Locker
}
