package locker

import (
	"context"
	"sync"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis"
)

type RedSyncMutexWrapper struct {
	name  string
	mutex *redsync.Mutex
}

func NewRedSyncMutexWrapper(name string, mutex *redsync.Mutex) *RedSyncMutexWrapper {
	if mutex == nil {
		panic("redsync.Mutex is nil")
	}

	return &RedSyncMutexWrapper{
		name:  name,
		mutex: mutex,
	}
}

func (l *RedSyncMutexWrapper) Lock(ctx context.Context) error {
	err := l.mutex.LockContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (l *RedSyncMutexWrapper) Unlock(ctx context.Context) error {
	ok, err := l.mutex.UnlockContext(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return ErrLockNotOk
	}

	return nil
}

type RedisLockerGenerator struct {
	redisPool redsyncredis.Pool
	rsync     *redsync.Redsync
	table     sync.Map
}

func NewRedisLockerGenerator(redisPool redsyncredis.Pool) *RedisLockerGenerator {
	return &RedisLockerGenerator{
		redisPool: redisPool,
		rsync:     redsync.New(redisPool),
	}
}

type RedisMutextOption struct{}

func (o RedisMutextOption) Apply(m *redsync.Mutex) {
	// TODO:
}

func (g *RedisLockerGenerator) CreateSyncLocker(name string) (SyncLocker, error) {
	mv, _ := g.table.LoadOrStore(name, NewRedSyncMutexWrapper(name, g.rsync.NewMutex(name, &RedisMutextOption{})))
	return mv.(*RedSyncMutexWrapper), nil
}
