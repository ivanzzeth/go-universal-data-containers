package locker

import (
	"context"
	"errors"
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
	// fmt.Printf("RedSyncMutexWrapper Lock %v %p\n", l.name, l)

	err := l.mutex.LockContext(ctx)
	if err != nil {
		return l.convertError(err)
	}

	// fmt.Printf("RedSyncMutexWrapper Locked %v %p\n", l.name, l)

	return nil
}

func (l *RedSyncMutexWrapper) Unlock(ctx context.Context) error {
	// fmt.Printf("RedSyncMutexWrapper Unlock %v %p\n", l.name, l)
	ok, err := l.mutex.UnlockContext(ctx)
	if err != nil {
		return l.convertError(err)
	}

	if !ok {
		return ErrLockNotOk
	}

	// fmt.Printf("RedSyncMutexWrapper Unlocked %v %p\n", l.name, l)

	return nil
}

func (l *RedSyncMutexWrapper) convertError(err error) error {
	// fmt.Printf("Lock error: %T\n", err)

	lockTaken := &redsync.ErrTaken{}
	if errors.As(err, &lockTaken) {
		return ErrLockTaken
	}
	lockNodeTaken := &redsync.ErrNodeTaken{}
	if errors.As(err, &lockNodeTaken) {
		return ErrLockTaken
	}

	if errors.Is(err, redsync.ErrLockAlreadyExpired) {
		return ErrLockAlreadyExpired
	}

	if errors.Is(err, redsync.ErrFailed) {
		return ErrLockFailedToAcquire
	}

	return err
}

type RedisLockerGenerator struct {
	redisPool    redsyncredis.Pool
	rsync        *redsync.Redsync
	table        sync.Map
	mutexOptions []redsync.Option
}

// NewRedisLockerGenerator creates a Redis-backed locker generator.
// Optional redsync.Option values are applied to every mutex created by this generator.
// For example:
//
//	locker.NewRedisLockerGenerator(pool,
//	    redsync.WithTries(3),
//	    redsync.WithRetryDelay(50*time.Millisecond),
//	    redsync.WithExpiry(2*time.Second),
//	)
func NewRedisLockerGenerator(redisPool redsyncredis.Pool, opts ...redsync.Option) *RedisLockerGenerator {
	return &RedisLockerGenerator{
		redisPool:    redisPool,
		rsync:        redsync.New(redisPool),
		mutexOptions: opts,
	}
}

func (g *RedisLockerGenerator) CreateSyncLocker(name string) (SyncLocker, error) {
	mv, _ := g.table.LoadOrStore(name, NewRedSyncMutexWrapper(name, g.rsync.NewMutex(name, g.mutexOptions...)))
	return mv.(*RedSyncMutexWrapper), nil
}
