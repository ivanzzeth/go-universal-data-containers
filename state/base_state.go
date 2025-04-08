package state

import (
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/common"
)

var (
	_ State = (*BaseState)(nil)
)

type BaseState struct {
	stateName string
	sync.Locker
}

func NewBaseState(locker sync.Locker) *BaseState {
	return &BaseState{
		Locker: locker,
	}
}

func (s *BaseState) StateName() string {
	return s.stateName
}

func (s *BaseState) GetIDComposer() IDComposer {
	panic(common.ErrNotImplemented)
}

func (s *BaseState) StateIDComponents() []any {
	panic(common.ErrNotImplemented)
}

func (s *BaseState) SetStateName(name string) {
	s.stateName = name
}

func (s *BaseState) GetLocker() sync.Locker {
	return s.Locker
}

func (s *BaseState) SetLocker(locker sync.Locker) {
	s.Locker = locker
}
