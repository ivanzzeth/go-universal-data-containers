package state

import (
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	_ Storage = (*GORMStorage)(nil)
)

// TODO: Unit test
type GORMStorage struct {
	db *gorm.DB
	Registry
	StorageSnapshot
}

type StateManagement struct {
	Name string `gorm:"not null; primaryKey; uniqueIndex:name_id"`
	ID   string `gorm:"not null; uniqueIndex:name_id"`
}

func NewGORMStorage(db *gorm.DB, registry Registry, snapshot StorageSnapshot) *GORMStorage {
	s := &GORMStorage{
		db:              db,
		Registry:        registry,
		StorageSnapshot: snapshot,
	}

	snapshot.SetStorage(s)
	s.StorageSnapshot = snapshot
	return s
}

func (s *GORMStorage) GetStateIDs(name string) ([]string, error) {
	states := []*StateManagement{}
	err := s.db.Where(&StateManagement{Name: name}).Find(&states).Error
	if err != nil {
		return nil, err
	}

	res := []string{}
	for _, state := range states {
		res = append(res, state.ID)
	}

	return res, nil
}

func (s *GORMStorage) GetStateNames() ([]string, error) {
	states := []*StateManagement{}
	err := s.db.Distinct("name").Find(states).Error
	if err != nil {
		return nil, err
	}

	res := []string{}
	for _, state := range states {
		res = append(res, state.Name)
	}

	return res, nil
}

func (s *GORMStorage) LoadAllStates() ([]State, error) {
	stateManagements := []*StateManagement{}
	err := s.db.Find(stateManagements).Error
	if err != nil {
		return nil, err
	}

	states := []State{}
	for _, sm := range stateManagements {
		tableName := sm.Name
		stateID := sm.ID
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
		return nil, err
	}

	return state, nil
}

func (s *GORMStorage) SaveStates(states ...State) error {
	models := make([]any, 0, len(states))
	for _, state := range states {
		models = append(models, state)
	}

	return s.BatchSave(models...)
}

func (s *GORMStorage) BatchSave(models ...any) error {
	return execGormBatchOp(s.db, gormBatchOperationSave, clause.OnConflict{UpdateAll: true}, models...)
}

func (s *GORMStorage) ClearAllStates() error {
	states, err := s.LoadAllStates()
	if err != nil {
		return err
	}

	models := make([]any, 0, len(states))
	for _, state := range states {
		models = append(models, state)
	}

	return s.BatchDelete(models)
}

func (s *GORMStorage) BatchDelete(models ...any) error {
	return execGormBatchOp(s.db, gormBatchOperationDelete, clause.OnConflict{UpdateAll: true}, models...)
}

type gormBatchOperation string

const (
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

		sql := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			switch op {
			case gormBatchOperationSave:
				return tx.Clauses(conds).Save(data)
			case gormBatchOperationDelete:
				return tx.Clauses(conds).Delete(data)
			default:
				return tx.Clauses(conds).Save(data)
			}
		})
		sqlStatements = append(sqlStatements, sql)
	}

	sql := strings.Join(sqlStatements, ";")
	// log.Info("BatchSave sql", "sql", sql)

	return db.Exec(sql).Error
}
