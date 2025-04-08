package state

import "sync"

type TestUserModel struct {
	BaseState
	Name   string
	Age    int
	Height int
}

func NewTestUserModel() *TestUserModel {
	state := NewBaseState(&sync.Mutex{})
	state.SetStateName("user")
	return &TestUserModel{BaseState: *state}
}
