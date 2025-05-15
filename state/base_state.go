package state

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	_ State = (*BaseState)(nil)
)

type BaseState struct {
	stateName       string
	idMarshaler     IDMarshaler
	locker          sync.Locker
	lockerGenerator locker.SyncLockerGenerator
}

func NewBaseState(lockerGenerator locker.SyncLockerGenerator, stateName string, idMarshaler IDMarshaler, idComponents StateIDComponents) (*BaseState, error) {
	s := &BaseState{}

	fmt.Printf("NewBaseState Initialize: stateName: %v, idMarshaler: %T, idComponents: %+v\n", stateName, idMarshaler, idComponents)
	err := s.Initialize(lockerGenerator, stateName, idMarshaler, idComponents)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *BaseState) StateName() string {
	return s.stateName
}

func (s *BaseState) GetIDMarshaler() IDMarshaler {
	if s.idMarshaler == nil {
		panic(common.ErrNotImplemented)
	}

	return s.idMarshaler
}

func (s *BaseState) StateIDComponents() StateIDComponents {
	panic(common.ErrNotImplemented)
}

func (s *BaseState) GetLocker() sync.Locker {
	return s.locker
}

func (s *BaseState) Initialize(generator locker.SyncLockerGenerator, stateName string, idMarshaler IDMarshaler, idComponents StateIDComponents) (err error) {
	s.idMarshaler = idMarshaler
	s.stateName = stateName
	s.lockerGenerator = generator

	defer func() {
		if err != nil {
			s.locker = nil
			s.idMarshaler = nil
			s.stateName = ""
			s.lockerGenerator = nil
		}
	}()

	stateID, err := GetStateIDByComponents(idMarshaler, idComponents)
	if err != nil {
		return err
	}

	idComponentsStr := ""

	for _, component := range idComponents {
		idComponentsStr += fmt.Sprintf(" %+v", reflect.ValueOf(component).Elem().Interface())
	}

	fmt.Printf("Initialize GetStateLockerByName, stateName: %v, stateID: %v, idComponents: %v\n", stateName, stateID, idComponentsStr)
	s.locker, err = GetStateLockerByName(generator, s.stateName, stateID)
	if err != nil {
		return err
	}

	return nil
}

func (s *BaseState) GetLockerGenerator() locker.SyncLockerGenerator {
	return s.lockerGenerator
}

func (s *BaseState) Lock() {
	s.locker.Lock()
}

func (s *BaseState) Unlock() {
	s.locker.Unlock()
}
