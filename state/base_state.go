package state

import "sync"

var (
	_ State = (*BaseState)(nil)
)

type BaseState struct {
	stateName string
	stateID   string
	sync.Locker
}

func NewBaseState(locker sync.Locker) *BaseState {
	return &BaseState{
		Locker: locker,
	}
}

func (s *BaseState) StateID() string {
	return s.stateID
}

func (s *BaseState) StateName() string {
	return s.stateName
}

func (s *BaseState) SetStateID(id string) {
	s.stateID = id
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
