package state

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/ivanzzeth/go-universal-data-containers/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	_ Storage = (*GORMStorage)(nil)
)

type GORMStorageFactory struct {
	db          *gorm.DB
	registry    Registry
	newSnapshot func(storageFactory StorageFactory) StorageSnapshot
	table       sync.Map
}

func NewGORMStorageFactory(db *gorm.DB, registry Registry, newSnapshot func(storageFactory StorageFactory) StorageSnapshot) *GORMStorageFactory {
	return &GORMStorageFactory{db: db, registry: registry, newSnapshot: newSnapshot}
}

func (f *GORMStorageFactory) GetOrCreateStorage(name string) (Storage, error) {
	// fmt.Printf("GetOrCreateStorage: %v\n", name)

	storeVal, _ := f.table.LoadOrStore(name, func() interface{} {
		if f.newSnapshot == nil {
			f.newSnapshot = func(storageFactory StorageFactory) StorageSnapshot {
				return NewBaseStorageSnapshot(f)
			}
		}
		snapshot := f.newSnapshot(f)
		storage, err := NewGORMStorage(f.db, name, f.registry, snapshot)
		if err != nil {
			return nil
		}
		return storage
	}())

	if storeVal == nil {
		return nil, fmt.Errorf("failed to create GORM storage")
	}

	// fmt.Printf("GetOrCreateStorage storage: %v\n", reflect.TypeOf(storeVal))
	return storeVal.(Storage), nil
}

// TODO: Unit test
// DO NOT use this to create snapshot.
type GORMStorage struct {
	db        *gorm.DB
	partition string
	Registry
	StorageSnapshot
}

type StateManagement struct {
	gorm.Model
	StateName string `gorm:"not null; uniqueIndex:statename_stateid_partition"`
	StateID   string `gorm:"not null; uniqueIndex:statename_stateid_partition"`
	Partition string `gorm:"not null; uniqueIndex:statename_stateid_partition"`
}

func NewGORMStorage(db *gorm.DB, partition string, registry Registry, snapshot StorageSnapshot) (*GORMStorage, error) {
	s := &GORMStorage{
		db:              db,
		partition:       partition,
		Registry:        registry,
		StorageSnapshot: snapshot,
	}

	snapshot.SetStorage(s)
	s.StorageSnapshot = snapshot

	err := db.AutoMigrate(&StateManagement{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *GORMStorage) GetStateIDs(name string) ([]string, error) {
	states := []*StateManagement{}
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
	err := s.db.Where(&StateManagement{Partition: s.partition}).Distinct("name").Find(&states).Error
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
		log.Println("execGormBatchOp", "for_range", data)

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
				log.Println("execGormBatchOp", "create_data", data)
				return tx.Clauses(conds).Create(data)
			case gormBatchOperationSave:
				log.Println("execGormBatchOp", "save_data", data)
				err := setGormPrimaryKeyZeroValue(data)
				if err != nil {
					sqlErr = err
					return nil
				}
				log.Println("execGormBatchOp", "save_data", data)

				return tx.Clauses(conds).Save(data)
			case gormBatchOperationDelete:
				log.Println("execGormBatchOp", "delete_data", data)
				return tx.Clauses(conds).Unscoped().Where(data).Delete(data)
				// return tx.Unscoped().Delete(data)
			default:
				log.Println("execGormBatchOp", "default_op", data)
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
	log.Println("execGormBatchOp", "op", op, "sql", sql)
	return db.Exec(sql).Error
}

func setGormPrimaryKeyZeroValue(data any) error {
	return utils.SetNestedField(data, "Model.[gorm]primarykey", nil)
}
