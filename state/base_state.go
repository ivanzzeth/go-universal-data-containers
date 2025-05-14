package state

import (
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

func NewBaseState(lockerGenerator locker.SyncLockerGenerator) *BaseState {
	s := &BaseState{}
	if lockerGenerator != nil {
		s.SetLockerGenerator(lockerGenerator)
	}

	return s
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

func (s *BaseState) SetIDMarshaler(idMarshaler IDMarshaler) {
	s.idMarshaler = idMarshaler
}

func (s *BaseState) StateIDComponents() []any {
	panic(common.ErrNotImplemented)
}

func (s *BaseState) SetStateName(name string) {
	s.stateName = name
}

func (s *BaseState) GetLocker() sync.Locker {
	return s.locker
}

func (s *BaseState) SetLockerGenerator(generator locker.SyncLockerGenerator) (err error) {
	s.lockerGenerator = generator
	s.locker, err = generator.CreateSyncLocker(s.stateName)
	return
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
