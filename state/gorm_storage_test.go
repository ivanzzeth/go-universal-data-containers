package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestGORMStorage(t *testing.T) {
	testDb, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		// DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = testDb.AutoMigrate(&TestUserModel{})
	if err != nil {
		t.Fatal(err)
	}

	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, nil)
	snapshot := NewBaseStorageSnapshot(storageFactory)

	storage, err := NewGORMStorage(testDb, "default", registry, snapshot)
	if err != nil {
		t.Fatal(err)
	}

	SpecTestStorage(t, registry, storage)
}

func TestSetGormPrimaryKeyZeroValue(t *testing.T) {
	t.Run("Test embeded gorm.Model", func(t *testing.T) {
		type testModel struct {
			gorm.Model
			Name string
		}

		model := &testModel{Model: gorm.Model{ID: 100}, Name: "test"}
		err := setGormPrimaryKeyZeroValue(model)
		assert.Equal(t, nil, err)
		assert.Equal(t, uint(0), model.ID)
	})

	t.Run("Test embeded *gorm.Model", func(t *testing.T) {
		type testModel struct {
			*gorm.Model
			Name string
		}

		model := &testModel{Model: &gorm.Model{ID: 100}, Name: "test"}
		err := setGormPrimaryKeyZeroValue(model)
		assert.Equal(t, nil, err)
		assert.Equal(t, uint(0), model.ID)
	})
}
