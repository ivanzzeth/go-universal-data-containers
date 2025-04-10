package state

import (
	"fmt"
	"sync"
)

// Make sure (Name, Server) or (Name + Server) is unique, so that they can be composed as StateID
type TestUserModel struct {
	GormModel
	BaseState
	finalizer Finalizer
	Name      string `gorm:"not null;uniqueIndex:idx_name_server"`
	Server    string `gorm:"not null;uniqueIndex:idx_name_server"`
	Age       int
	Height    int
}

func MustNewTestUserModel(locker sync.Locker, name, server string) *TestUserModel {
	state := NewBaseState(locker)
	// Make sure that it's compatible for all storages you want to use
	// For GORMStorage and MemoryStorage, it is ok.
	state.SetStateName("test_user_models")
	state.SetIDMarshaler(NewJsonIDMarshaler("-"))

	m := &TestUserModel{BaseState: *state, Name: name, Server: server}

	err := m.FillID(m)
	if err != nil {
		panic(fmt.Errorf("invalid stateID: %v", err))
	}

	return m
}

func (u *TestUserModel) WithStateFinalizer(finalizer Finalizer) *TestUserModel {
	u.finalizer = finalizer
	return u
}

func (u *TestUserModel) Get() error {
	stateID, err := u.GetIDMarshaler().MarshalStateID(u.StateIDComponents()...)
	if err != nil {
		return err
	}

	state, err := u.finalizer.LoadState(u.StateName(), stateID)
	if err != nil {
		return err
	}

	*u = *(state.(*TestUserModel))
	u.SetLocker((state.(*TestUserModel)).GetLocker())
	return nil
}

func (u *TestUserModel) StateIDComponents() []any {
	return []any{&u.Name, &u.Server}
}
