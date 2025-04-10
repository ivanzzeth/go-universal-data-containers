package state

import (
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/common"
)

var (
	_ State = (*BaseState)(nil)
)

type BaseState struct {
	stateName   string
	idMarshaler IDMarshaler
	locker      sync.Locker
}

func NewBaseState(locker sync.Locker) *BaseState {
	return &BaseState{
		locker: locker,
	}
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

func (s *BaseState) SetLocker(locker sync.Locker) {
	s.locker = locker
}

func (s *BaseState) Lock() {
	s.locker.Lock()
}
func (s *BaseState) Unlock() {
	s.locker.Unlock()
}
