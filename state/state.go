package state

import (
	"errors"
	"fmt"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	ErrStateNotFound      = errors.New("state not found")
	ErrStateNotRegistered = errors.New("state not registered")
	ErrStateNotPointer    = errors.New("state must be a pointer")
	ErrStateIDComponents  = errors.New("state id components must not be empty")
)

// State is a state interface.
// It aims to provide a simple way to work with state.
// What does it mean?
// If a record has a unique identifier, and it can be generated through
// mulitple other fields. e.g, a user id can be generated through user name, location, etc.,
// then we can use it as a state id.
// It's very simular to multiple primary keys in SQL databases.
type State interface {
	locker.SyncLocker
	GetLocker() locker.SyncLocker
	GetLockerGenerator() locker.SyncLockerGenerator

	StateName() string

	GetIDMarshaler() IDMarshaler
	StateIDComponents() StateIDComponents

	// To avoid use it outside of initialization.
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

func GetStateLockerByName(lockerGenerator locker.SyncLockerGenerator, stateName, stateID string) (locker.SyncLocker, error) {
	locker, err := lockerGenerator.CreateSyncLocker(fmt.Sprintf("state-locker-%v-id-%v", stateName, stateID))
	// fmt.Printf("GetStateLockerByName, key: %v, generator: %p, generator type: %T, locker: %p\n", fmt.Sprintf("state-locker-%v-id-%v", stateName, stateID),
	// 	lockerGenerator, lockerGenerator, locker)
	return locker, err
}
