package locker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis"
)

type RedSyncMutexWrapper struct {
	name    string
	locking atomic.Bool
	mutex   *redsync.Mutex
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
	fmt.Printf("RedSyncMutexWrapper require locker %v %p\n", l.name, l)

	for {
		err := l.mutex.Lock()
		if err != nil {
			// TODO: logging
			fmt.Printf("RedSyncMutexWrapper require locker %v %p failed, err: %v\n", l.name, l, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		break
	}

	l.locking.Store(true)

	// go func() {
	// 	for l.locking.Load() {
	// 		util := l.mutex.Until()
	// 		fmt.Printf("RedSyncMutexWrapper extended locker %v, until: %v\n", l.name, util)

	// 		ok, err := l.mutex.Extend()
	// 		if err != nil || !ok {
	// 			fmt.Printf("RedSyncMutexWrapper extend locker failed %v, ok: %v, until: %v err: %v\n", l.name, ok, util, err)
	// 			time.Sleep(100 * time.Millisecond)
	// 			continue
	// 		}

	// 		time.Sleep(10 * time.Millisecond)
	// 	}
	// }()
}

func (l *RedSyncMutexWrapper) Unlock() {
	fmt.Printf("RedSyncMutexWrapper release locker %v %p\n", l.name, l)
	ok, err := l.mutex.Unlock()
	if err != nil || !ok {
		// TODO: logging
		fmt.Printf("RedSyncMutexWrapper release locker %v %p failed, err: %v, ok: %v\n", l.name, l, err, ok)
		time.Sleep(100 * time.Millisecond)
	}

	// for {
	// 	ok, err := l.mutex.Unlock()
	// 	if err != nil || !ok {
	// 		// TODO: logging
	// 		fmt.Printf("RedSyncMutexWrapper release locker %v failed, err: %v, ok: %v\n", l.name, err, ok)
	// 		time.Sleep(100 * time.Millisecond)
	// 		continue
	// 	}

	// 	break
	// }

	l.locking.Store(false)
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

func (g *RedisLockerGenerator) CreateSyncLocker(name string) (sync.Locker, error) {
	mv, _ := g.table.LoadOrStore(name, NewRedSyncMutexWrapper(name, g.rsync.NewMutex(name, &RedisMutextOption{})))
	return mv.(*RedSyncMutexWrapper), nil
}
