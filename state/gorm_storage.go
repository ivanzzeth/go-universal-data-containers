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
				return NewSimpleStorageSnapshot(f)
			}
		}
		snapshot := f.newSnapshot(f)
		var locker sync.Locker
		locker, err = f.SyncLockerGenerator.CreateSyncLocker(fmt.Sprintf("storage-locker-%v", name))
		if err != nil {
			return
		}
		var storage Storage
		storage, err = NewGORMStorage(locker, f.db, f.registry, snapshot, name)
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
}

func (m *GormModel) FillID(state State) error {
	if m.ID == "" {
		stateID, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}
		m.ID = stateID
	}

	return nil
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
	StateName string `gorm:"not null; uniqueIndex:statename_stateid_partition"`
	StateID   string `gorm:"not null; uniqueIndex:statename_stateid_partition"`
	Partition string `gorm:"not null; uniqueIndex:statename_stateid_partition"`
}

func NewGORMStorage(locker sync.Locker, db *gorm.DB, registry Registry, snapshot StorageSnapshot, partition string) (*GORMStorage, error) {
	if partition == "" {
		partition = "default"
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

	err := db.AutoMigrate(&StateManagement{})
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
	err := s.db.Where(&StateManagement{StateName: name, Partition: s.partition}).Find(&states).Error
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

	err := s.db.Where(&StateManagement{Partition: s.partition}).Distinct("state_name").Find(&states).Error
	if err != nil {
		return nil, err
	}

	res := []string{}
	for _, state := range states {
		res = append(res, state.StateName)
	}

	return res, nil
}

func (s *GORMStorage) LoadAllStates() ([]State, error) {
	stateManagements := []*StateManagement{}
	time.Sleep(s.delay)

	err := s.db.Where(&StateManagement{Partition: s.partition}).Find(&stateManagements).Error
	if err != nil {
		return nil, err
	}

	states := []State{}
	for _, sm := range stateManagements {
		tableName := sm.StateName
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
	models := make([]any, 0, 2*len(states))
	for _, state := range states {
		models = append(models, state)

		stateID, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}

		models = append(models, &StateManagement{
			StateName: state.StateName(),
			StateID:   stateID,
			Partition: s.partition,
		})
	}

	return s.BatchSave(models...)
}

func (s *GORMStorage) BatchSave(models ...any) error {
	time.Sleep(s.delay)
	return execGormBatchOp(s.db, gormBatchOperationSave, clause.OnConflict{UpdateAll: true}, models...)
}

func (s *GORMStorage) ClearAllStates() error {
	states, err := s.LoadAllStates()
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return nil
	}

	models := make([]any, 0, len(states))
	for _, state := range states {
		models = append(models, state)

		stateID, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}

		models = append(models, &StateManagement{
			StateName: state.StateName(),
			StateID:   stateID,
			Partition: s.partition,
		})
	}

	return s.BatchDelete(models...)
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
				err := setGormPrimaryKeyZeroValue(data)
				if err != nil {
					sqlErr = err
					return tx
				}
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
	// log.Println("SQL:", sql)
	return db.Exec(sql).Error
}

func setGormPrimaryKeyZeroValue(data any) error {
	return utils.SetNestedField(data, "GormModel.[gorm]primarykey", nil)
}
