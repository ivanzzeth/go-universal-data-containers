package locker

import (
	"context"
	"fmt"
)

var (
	ErrLockAlreadyExpired = fmt.Errorf("lock already expired")
	ErrLockNotHeld        = fmt.Errorf("lock not held")
	ErrLockNotLocked      = fmt.Errorf("lock not locked")
	ErrLockNotOk          = fmt.Errorf("lock not ok") // For unknown error
)

type SyncLockerGenerator interface {
	CreateSyncLocker(name string) (SyncLocker, error)
}

type SyncLocker interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type SyncRWLockerGenerator interface {
	CreateSyncRWLocker(name string) (SyncRWLocker, error)
}

type SyncRWLocker interface {
	SyncLocker
	RLock(ctx context.Context) error
	RUnlock(ctx context.Context) error
}
