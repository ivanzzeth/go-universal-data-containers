package state

import (
	"fmt"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
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

func MustNewTestUserModel(lockerGenerator locker.SyncLockerGenerator, name, server string) *TestUserModel {
	// You must initialize all id components first
	m := &TestUserModel{GormModel: GormModel{Partition: "test_user_partition"}, Name: name, Server: server}

	// Make sure that it's compatible for all storages you want to use
	// For GORMStorage and MemoryStorage, it is ok.
	state, err := NewBaseState(lockerGenerator, "test_user_models", NewBase64IDMarshaler("-"), m.StateIDComponents())
	if err != nil {
		panic(fmt.Errorf("failed to create base state: %v", err))
	}

	m.BaseState = *state

	err = m.FillID(m)
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
	err = u.Initialize((state.(*TestUserModel)).GetLockerGenerator(), u.StateName(), u.GetIDMarshaler(), u.StateIDComponents())
	if err != nil {
		return err
	}

	return nil
}

func (u *TestUserModel) StateIDComponents() StateIDComponents {
	return []any{&u.Name, &u.Server}
}
