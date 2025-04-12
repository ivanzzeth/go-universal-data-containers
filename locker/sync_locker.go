package locker

import "sync"

type SyncLockerGenerator interface {
	CreateSyncLocker(name string) (sync.Locker, error)
}

type SyncRWLockerGenerator interface {
	CreateSyncRWLocker(name string) (sync.Locker, error)
}

type SyncRWLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}
