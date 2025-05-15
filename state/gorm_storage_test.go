package state

import (
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGORMStorage(t *testing.T) {
	testDb, err := setupTestGormDB()
	if err != nil {
		t.Fatal(err)
	}

	registry := NewSimpleRegistry()
	lockerGenerator := locker.NewMemoryLockerGenerator()
	storageFactory := NewMemoryStorageFactory(registry, lockerGenerator, nil)
	snapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")

	storage, err := NewGORMStorage(lockerGenerator, testDb, registry, snapshot, "default")
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
	testDb, err := setupTestGormDB()
	if err != nil {
		b.Fatal(err)
	}

	registry := NewSimpleRegistry()

	lockerGenerator := locker.NewMemoryLockerGenerator()

	storageFactory := NewGORMStorageFactory(testDb, registry, lockerGenerator, nil)
	snapshot := NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator, "")

	storage, err := NewGORMStorage(lockerGenerator, testDb, registry, snapshot, "default")
	if err != nil {
		b.Fatal(err)
	}
	storage.setDelay(20 * time.Millisecond)

	// tables, err := testDb.Migrator().GetTables()
	// if err != nil {
	// 	b.Fatal(err)
	// }

	// sqlDB, _ := testDb.DB()

	// for _, table := range tables {
	// 	res, err := sqlDB.Query(fmt.Sprintf("PRAGMA table_info(%v)", table))
	// 	if err != nil {
	// 		b.Fatal(err)
	// 	}

	// 	b.Logf("table %v: %+v", table, res)
	// }

	SpecBenchmarkStorage(b, registry, storage)
}

func setupTestGormDB() (*gorm.DB, error) {
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
