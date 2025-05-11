package state

import (
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ivanzzeth/go-universal-data-containers/locker"
	redis "github.com/redis/go-redis/v9"
)

func TestRedisStorage(t *testing.T) {
	rdb := setupRdb(t)
	registry := NewSimpleRegistry()
	storageFactory := NewMemoryStorageFactory(registry, locker.NewMemoryLockerGenerator(), nil)
	snapshot := NewSimpleStorageSnapshot(registry, storageFactory)

	storage, err := NewRedisStorage(&sync.Mutex{}, rdb, registry, snapshot, "default")
	if err != nil {
		t.Fatal(err)
	}

	SpecTestStorage(t, registry, storage)
}

func BenchmarkRedisStorageWith2msLatency(b *testing.B) {
	rdb := setupRdb(b)

	registry := NewSimpleRegistry()
	storageFactory := NewRedisStorageFactory(rdb, registry, locker.NewMemoryLockerGenerator(), nil)
	snapshot := NewSimpleStorageSnapshot(registry, storageFactory)

	storage, err := NewRedisStorage(&sync.Mutex{}, rdb, registry, snapshot, "default")
	if err != nil {
		b.Fatal(err)
	}
	storage.setDelay(2 * time.Millisecond)
	SpecBenchmarkStorage(b, registry, storage)
}

func setupRdb(t miniredis.Tester) *redis.Client {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	return rdb
}
