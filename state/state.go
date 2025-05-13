package state

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
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

func GetStateID(state State) (stateID string, err error) {
	stateID, err = state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
	if err != nil {
		return
	}

	return
}

func GetStateLockerByName(lockerGenerator locker.SyncLockerGenerator, stateName string) (sync.Locker, error) {
	locker, err := lockerGenerator.CreateSyncLocker(fmt.Sprintf("state-locker-%v", stateName))
	return locker, err
}
