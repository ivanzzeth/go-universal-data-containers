package state

import (
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGORMStorage(t *testing.T) {
	testDb, err := setupDB()
	if err != nil {
		t.Fatal(err)
	}

	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, locker.NewMemoryLockerGenerator(), nil)
	snapshot := NewSimpleStorageSnapshot(storageFactory)

	storage, err := NewGORMStorage(&sync.Mutex{}, testDb, registry, snapshot, "default")
	if err != nil {
		t.Fatal(err)
	}

	SpecTestStorage(t, registry, storage)
}

func TestSetGormPrimaryKeyZeroValue(t *testing.T) {
	t.Run("Test embeded gorm.Model", func(t *testing.T) {
		type testModel struct {
			GormModel
			Name string
		}

		model := &testModel{GormModel: GormModel{ID: "100"}, Name: "test"}
		err := setGormPrimaryKeyZeroValue(model)
		assert.Equal(t, nil, err)
		assert.Equal(t, "", model.ID)
	})

	t.Run("Test embeded *gorm.Model", func(t *testing.T) {
		type testModel struct {
			*GormModel
			Name string
		}

		model := &testModel{GormModel: &GormModel{ID: "100"}, Name: "test"}
		err := setGormPrimaryKeyZeroValue(model)
		assert.Equal(t, nil, err)
		assert.Equal(t, "", model.ID)
	})
}

func BenchmarkGORMStorageWith20msLatency(b *testing.B) {
	testDb, err := setupDB()
	if err != nil {
		b.Fatal(err)
	}

	registry := NewSimpleRegistry()
	storageFactory := NewGORMStorageFactory(testDb, registry, locker.NewMemoryLockerGenerator(), nil)
	snapshot := NewSimpleStorageSnapshot(storageFactory)

	storage, err := NewGORMStorage(&sync.Mutex{}, testDb, registry, snapshot, "default")
	if err != nil {
		b.Fatal(err)
	}
	storage.setDelay(20 * time.Millisecond)
	SpecBenchmarkStorage(b, registry, storage)
}

func setupDB() (*gorm.DB, error) {
	testDb, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		// DisableForeignKeyConstraintWhenMigrating: true,
		Logger: logger.Discard,
	})
	if err != nil {
		return nil, err
	}

	err = testDb.AutoMigrate(&TestUserModel{})
	if err != nil {
		return nil, err
	}

	return testDb, nil
}
