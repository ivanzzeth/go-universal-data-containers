package locker

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	redis "github.com/redis/go-redis/v9"
)

func SpecTestLocker(t *testing.T, lockerGenerator SyncLockerGenerator) {
	t.Run("CreateSyncLocker", func(t *testing.T) {
		locker, err := lockerGenerator.CreateSyncLocker("test")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("locker: %T", locker)
	})
}

func TestMemoryLockerGenerator(t *testing.T) {
	SpecTestLocker(t, NewMemoryLockerGenerator())
}

func TestRedisLockerGenerator(t *testing.T) {
	rdb := setupRdb(t)
	redisPool := goredis.NewPool(rdb)
	SpecTestLocker(t, NewRedisLockerGenerator(redisPool))
}

func setupRdb(t miniredis.Tester) *redis.Client {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	return rdb
}
