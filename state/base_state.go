package state

import (
	"context"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	_ State = (*BaseState)(nil)
)

type BaseState struct {
	stateName    string
	idMarshaler  IDMarshaler
	idComponents StateIDComponents
	locker       locker.SyncLocker

	lockerGenerator locker.SyncLockerGenerator
}

func NewBaseState(lockerGenerator locker.SyncLockerGenerator, stateName string, idMarshaler IDMarshaler, idComponents StateIDComponents) (*BaseState, error) {
	s := &BaseState{}

	// fmt.Printf("NewBaseState Initialize: stateName: %v, idMarshaler: %T, idComponents: %+v\n", stateName, idMarshaler, idComponents)
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

func (s *BaseState) GetLocker() locker.SyncLocker {
	return s.locker
}

func (s *BaseState) Initialize(generator locker.SyncLockerGenerator, stateName string, idMarshaler IDMarshaler, idComponents StateIDComponents) (err error) {
	s.idMarshaler = idMarshaler
	s.idComponents = idComponents
	s.stateName = stateName
	s.lockerGenerator = generator

	return nil
}

func (s *BaseState) GetLockerGenerator() locker.SyncLockerGenerator {
	return s.lockerGenerator
}

func (s *BaseState) Lock(ctx context.Context) error {
	if s.locker == nil {
		err := s.initLocker()
		if err != nil {
			return err
		}
	}

	return s.locker.Lock(ctx)
}

func (s *BaseState) Unlock(ctx context.Context) error {
	if s.locker == nil {
		err := s.initLocker()
		if err != nil {
			return err
		}
	}

	return s.locker.Unlock(ctx)
}

func (s *BaseState) initLocker() error {
	stateID, err := GetStateIDByComponents(s.idMarshaler, s.idComponents)
	if err != nil {
		return err
	}

	// idComponentsStr := ""

	// for _, component := range idComponents {
	// 	idComponentsStr += fmt.Sprintf(" %+v", reflect.ValueOf(component).Elem().Interface())
	// }

	// fmt.Printf("Initialize GetStateLockerByName, stateName: %v, stateID: %v, idComponents: %v\n", stateName, stateID, idComponentsStr)
	s.locker, err = GetStateLockerByName(s.lockerGenerator, s.stateName, stateID)
	if err != nil {
		return err
	}

	return nil
}
