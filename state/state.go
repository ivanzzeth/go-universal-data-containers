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
	sync.Locker
	GetLocker() sync.Locker
	GetLockerGenerator() locker.SyncLockerGenerator

	StateName() string

	GetIDMarshaler() IDMarshaler
	StateIDComponents() StateIDComponents

	Initialize(generator locker.SyncLockerGenerator, stateName string, idMarshaler IDMarshaler, idComponents StateIDComponents) error
}

type StateIDComponents []any

func GetStateID(state State) (stateID string, err error) {
	stateID, err = GetStateIDByComponents(state.GetIDMarshaler(), state.StateIDComponents())
	if err != nil {
		return
	}

	return
}

func GetStateIDByComponents(idMarshaler IDMarshaler, stateIDComponents StateIDComponents) (stateID string, err error) {
	stateID, err = idMarshaler.MarshalStateID(stateIDComponents...)
	if err != nil {
		return
	}

	return
}

func GetStateLockerByName(lockerGenerator locker.SyncLockerGenerator, stateName, stateID string) (sync.Locker, error) {
	locker, err := lockerGenerator.CreateSyncLocker(fmt.Sprintf("state-locker-%v-id-%v", stateName, stateID))
	return locker, err
}
