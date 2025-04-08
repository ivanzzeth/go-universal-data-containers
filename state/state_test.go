package state

import (
	"sync"
)

// Make sure (Name, Server) or (Name + Server) is unique, so that they can be composed as StateID
type TestUserModel struct {
	BaseState
	Name   string `gorm:"not null;unique"`
	Server string `gorm:"not null;unique"`
	Age    int
	Height int
}

func NewTestUserModel(locker sync.Locker, name, server string) *TestUserModel {
	state := NewBaseState(locker)
	state.SetStateName("user")
	return &TestUserModel{BaseState: *state, Name: name, Server: server}
}

func (u *TestUserModel) GetIDComposer() IDComposer {
	return NewJsonIDComposer("-")
}

func (u *TestUserModel) StateIDComponents() []any {
	return []any{u.Name, u.Server}
}
