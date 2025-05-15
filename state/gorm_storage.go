package state

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/ivanzzeth/go-universal-data-containers/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	_ Storage = (*GORMStorage)(nil)
)

type GORMStorageFactory struct {
	db       *gorm.DB
	registry Registry
	locker.SyncLockerGenerator
	newSnapshot func(storageFactory StorageFactory) StorageSnapshot
	table       sync.Map
}

func NewGORMStorageFactory(db *gorm.DB, registry Registry, lockerGenerator locker.SyncLockerGenerator, newSnapshot func(storageFactory StorageFactory) StorageSnapshot) *GORMStorageFactory {
	return &GORMStorageFactory{db: db, registry: registry, SyncLockerGenerator: lockerGenerator, newSnapshot: newSnapshot}
}

func (f *GORMStorageFactory) GetOrCreateStorage(name string) (Storage, error) {
	// fmt.Printf("GetOrCreateStorage: %v\n", name)
	onceVal, _ := f.table.LoadOrStore(fmt.Sprintf("%v-once", name), &sync.Once{})

	var err error
	onceVal.(*sync.Once).Do(func() {
		if f.newSnapshot == nil {
			f.newSnapshot = func(storageFactory StorageFactory) StorageSnapshot {
				return NewSimpleStorageSnapshot(f.registry, f, f.SyncLockerGenerator, name)
			}
		}
		snapshot := f.newSnapshot(f)

		var storage Storage
		storage, err = NewGORMStorage(f.SyncLockerGenerator, f.db, f.registry, snapshot, name)
		storage = NewStorageWithMetrics(storage)

		f.table.LoadOrStore(name, storage)
	})
	if err != nil {
		f.table.Delete(fmt.Sprintf("%v-once", name))
		return nil, err
	}

	storeVal, loaded := f.table.Load(name)
	if !loaded {
		return nil, ErrStorageNotFound
	}

	return storeVal.(Storage), nil
}

type GormModel struct {
	ID        string `gorm:"primarykey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Partition string `gorm:"not null; index"`
}

func (m *GormModel) FillID(state State) error {
	if m.ID == "" {
		stateID, err := GetStateID(state)
		if err != nil {
			return err
		}
		m.ID = stateID
	}

	return nil
}

type idFiller interface {
	FillID(state State) error
}

// TODO: Unit test
// DO NOT use this to create snapshot.
type GORMStorage struct {
	locker sync.Locker
	db     *gorm.DB
	// Only used for simulating network latency
	delay time.Duration

	partition string
	Registry
	StorageSnapshot
}

type StateManagement struct {
	GormModel
	BaseState

	StateNamee string `gorm:"not null; index"`
	StateID    string `gorm:"not null; index"`
}

func MustNewStateManagement(lockerGenerator locker.SyncLockerGenerator, stateName, stateID, partition string) *StateManagement {
	m := &StateManagement{GormModel: GormModel{Partition: partition}, StateNamee: stateName, StateID: stateID}
	state, err := NewBaseState(lockerGenerator, "state_managements", NewJsonIDMarshaler("_"), m.StateIDComponents())
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

func (u *StateManagement) StateIDComponents() StateIDComponents {
	return []any{&u.stateName, &u.StateID, &u.Partition}
}

func NewGORMStorage(lockerGenerator locker.SyncLockerGenerator, db *gorm.DB, registry Registry, snapshot StorageSnapshot, partition string) (*GORMStorage, error) {
	if partition == "" {
		partition = "default"
	}

	locker, err := GetStorageLockerByName(lockerGenerator, partition)
	if err != nil {
		return nil, err
	}

	s := &GORMStorage{
		locker:          locker,
		db:              db,
		partition:       partition,
		Registry:        registry,
		StorageSnapshot: snapshot,
	}

	snapshot.SetStorageForSnapshot(s)
	s.StorageSnapshot = snapshot

	err = registry.RegisterState(MustNewStateManagement(lockerGenerator, "", "", partition))
	if err != nil {
		return nil, err
	}

	registeredStates := registry.GetRegisteredStates()
	models := []any{}
	for _, state := range registeredStates {
		models = append(models, state)
	}

	err = db.AutoMigrate(models...)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *GORMStorage) setDelay(delay time.Duration) {
	s.delay = delay
}

func (s *GORMStorage) StorageType() string {
	return fmt.Sprintf("gorm-%s", s.db.Dialector.Name())
}

func (s *GORMStorage) StorageName() string {
	return s.partition
}

func (s *GORMStorage) Lock() {
	s.locker.Lock()
}

func (s *GORMStorage) Unlock() {
	s.locker.Unlock()
}

func (s *GORMStorage) GetStateIDs(name string) ([]string, error) {
	states := []*StateManagement{}

	time.Sleep(s.delay)
	err := s.db.Where(&StateManagement{StateNamee: name, GormModel: GormModel{Partition: s.partition}}).Find(&states).Error
	if err != nil {
		return nil, err
	}

	res := []string{}
	for _, state := range states {
		res = append(res, state.StateID)
	}

	return res, nil
}

func (s *GORMStorage) GetStateNames() ([]string, error) {
	states := []*StateManagement{}
	time.Sleep(s.delay)

	err := s.db.Where(&StateManagement{GormModel: GormModel{Partition: s.partition}}).Distinct("state_namee").Find(&states).Error
	if err != nil {
		return nil, err
	}

	res := []string{}
	for _, state := range states {
		res = append(res, state.StateNamee)
	}

	return res, nil
}

func (s *GORMStorage) LoadAllStates() ([]State, error) {
	stateManagements := []*StateManagement{}
	time.Sleep(s.delay)

	err := s.db.Where(&StateManagement{GormModel: GormModel{Partition: s.partition}}).Find(&stateManagements).Error
	if err != nil {
		return nil, err
	}

	states := []State{}
	for _, sm := range stateManagements {
		// fmt.Printf("LoadAllStates: sm: %+v\n", sm)

		tableName := sm.StateNamee
		stateID := sm.StateID
		state, err := s.NewState(tableName)
		if err != nil {
			return nil, err
		}

		err = state.GetIDMarshaler().UnmarshalStateID(stateID, state.StateIDComponents()...)
		if err != nil {
			return nil, err
		}

		time.Sleep(s.delay)
		err = s.db.Table(tableName).Where(state).First(state).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, err
		}

		states = append(states, state)
	}

	return states, nil
}

func (s *GORMStorage) LoadState(name string, id string) (State, error) {
	state, err := s.NewState(name)
	if err != nil {
		return nil, err
	}

	err = state.GetIDMarshaler().UnmarshalStateID(id, state.StateIDComponents()...)
	if err != nil {
		return nil, err
	}

	time.Sleep(s.delay)
	err = s.db.Table(name).Where(state).First(state).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrStateNotFound
		}
		return nil, err
	}

	return state, nil
}

func (s *GORMStorage) SaveStates(states ...State) error {
	// fmt.Println()
	// fmt.Println("-----------------------------------------")
	// fmt.Printf("Storage Name: %v\n", s.partition)

	models := make([]any, 0, 2*len(states))
	for _, state := range states {
		stateID, err := GetStateID(state)
		if err != nil {
			return err
		}

		if filler, ok := state.(idFiller); ok {
			err = filler.FillID(state)
			if err != nil {
				return err
			}
		}

		models = append(models, state)

		sm := MustNewStateManagement(locker.NewMemoryLockerGenerator(), state.StateName(), stateID, s.partition)

		// fmt.Printf("StateManagement: %+v\n", sm)
		models = append(models, sm)

		// fmt.Printf("SaveStates: state: %+v, sm: %+v\n", state, sm)
	}

	return s.BatchSave(models...)
}

func (s *GORMStorage) BatchSave(models ...any) error {
	time.Sleep(s.delay)
	return execGormBatchOp(s.db, gormBatchOperationCreate, clause.OnConflict{UpdateAll: true}, models...)
}

func (s *GORMStorage) ClearStates(states ...State) error {
	models := make([]any, 0, len(states))
	for _, state := range states {
		models = append(models, state)

		stateID, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}

		models = append(models, &StateManagement{
			GormModel:  GormModel{Partition: s.partition},
			StateNamee: state.StateName(),
			StateID:    stateID,
		})
	}

	return s.BatchDelete(models...)
}

func (s *GORMStorage) ClearAllStates() error {
	// TODO: Optimize this.
	// stateNames, err := s.GetStateNames()
	// if err != nil {
	// 	return err
	// }

	// models := make([]any, 0, len(stateNames))

	// for _, stateName := range stateNames {
	// 	state, err := s.NewState(stateName)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	// state
	// 	models = append(models)
	// }

	// return s.BatchDelete(
	// 	&StateManagement{Partition: s.partition},
	// )

	states, err := s.LoadAllStates()
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return nil
	}

	return s.ClearStates(states...)
}

func (s *GORMStorage) BatchDelete(models ...any) error {
	time.Sleep(s.delay)
	return execGormBatchOp(s.db, gormBatchOperationDelete, clause.OnConflict{UpdateAll: true}, models...)
}

type gormBatchOperation string

const (
	gormBatchOperationCreate gormBatchOperation = "create"
	gormBatchOperationSave   gormBatchOperation = "save"
	gormBatchOperationDelete gormBatchOperation = "delete"
)

func execGormBatchOp(db *gorm.DB, op gormBatchOperation, conds clause.OnConflict, datas ...interface{}) error {
	if len(datas) == 0 {
		return nil
	}

	sqlStatements := []string{}
	for _, data := range datas {
		typ := reflect.TypeOf(data)
		val := reflect.ValueOf(data)
		if typ.Kind() == reflect.Array || typ.Kind() == reflect.Slice {
			if val.Len() == 0 {
				continue
			}
		}

		var sqlErr error
		sql := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			switch op {
			case gormBatchOperationCreate:
				return tx.Clauses(conds).Create(data)
			case gormBatchOperationSave:
				// err := setGormPrimaryKeyZeroValue(data)
				// if err != nil {
				// 	sqlErr = err
				// 	return tx
				// }
				return tx.Clauses(conds).Where(data).Save(data)
			case gormBatchOperationDelete:
				return tx.Clauses(conds).Unscoped().Where(data).Delete(data)
			default:
				return tx.Clauses(conds).Create(data)
			}
		})
		if sql == "" || sqlErr != nil {
			if sqlErr == nil {
				sqlErr = fmt.Errorf("empty sql")
			}
			return fmt.Errorf("create sql failed: %v", sqlErr)
		}

		sqlStatements = append(sqlStatements, sql)
	}

	sql := strings.Join(sqlStatements, ";")
	// fmt.Println("SQL:", sql)
	return db.Exec(sql).Error
}

func setGormPrimaryKeyZeroValue(data any) error {
	return utils.SetNestedField(data, "GormModel.[gorm]primarykey", nil)
}
