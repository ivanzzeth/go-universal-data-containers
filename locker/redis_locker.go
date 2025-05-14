package locker

import (
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

func (l *RedSyncMutexWrapper) Lock() {
	// fmt.Printf("RedSyncMutexWrapper require locker %v\n", l.name)
	err := l.mutex.Lock()
	if err != nil {
		// TODO: logging
		// fmt.Printf("RedSyncMutexWrapper required locker  %v, err: %v\n", l.name, err)
	}
}

func (l *RedSyncMutexWrapper) Unlock() {
	// fmt.Printf("RedSyncMutexWrapper release locker  %v\n", l.name)
	_, err := l.mutex.Unlock()
	if err != nil {
		// TODO: logging
		// fmt.Printf("RedSyncMutexWrapper release locker  %v, err: %v\n", l.name, err)
	}
}

type RedisLockerGenerator struct {
	redisPool redsyncredis.Pool
	rsync     *redsync.Redsync
}

func NewRedisLockerGenerator(redisPool redsyncredis.Pool) *RedisLockerGenerator {
	return &RedisLockerGenerator{
		redisPool: redisPool,
		rsync:     redsync.New(redisPool),
	}
}

func (g *RedisLockerGenerator) CreateSyncLocker(name string) (sync.Locker, error) {
	redmutex := g.rsync.NewMutex(name)
	return NewRedSyncMutexWrapper(name, redmutex), nil
}
