package state

import (
	"sync"
)

// Make sure (Name, Server) or (Name + Server) is unique, so that they can be composed as StateID
type TestUserModel struct {
	BaseState
	finalizer Finalizer
	Name      string `gorm:"not null;unique"`
	Server    string `gorm:"not null;unique"`
	Age       int
	Height    int
}

func NewTestUserModel(locker sync.Locker, name, server string) *TestUserModel {
	state := NewBaseState(locker)
	state.SetStateName("user")
	return &TestUserModel{BaseState: *state, Name: name, Server: server}
}

func (u *TestUserModel) WithStateFinalizer(finalizer Finalizer) *TestUserModel {
	u.finalizer = finalizer
	return u
}

func (u *TestUserModel) Get() error {
	stateID, err := u.GetIDComposer().ComposeStateID(u.StateIDComponents()...)
	if err != nil {
		return err
	}

	state, err := u.finalizer.LoadState(u.StateName(), stateID)
	if err != nil {
		return err
	}

	*u = *(state.(*TestUserModel))
	return nil
}

func (u *TestUserModel) GetIDComposer() IDComposer {
	return NewJsonIDComposer("-")
}

func (u *TestUserModel) StateIDComponents() []any {
	return []any{u.Name, u.Server}
}
