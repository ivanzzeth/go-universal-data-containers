package locker

import (
	"context"
	"sync"
)

var (
	_ SyncLockerGenerator   = (*MemoryLockerGenerator)(nil)
	_ SyncRWLockerGenerator = (*MemoryRWLockerGenerator)(nil)
)

type MemoryLockerGenerator struct {
	table sync.Map
}

func NewMemoryLockerGenerator() *MemoryLockerGenerator {
	return &MemoryLockerGenerator{}
}

func (g *MemoryLockerGenerator) CreateSyncLocker(name string) (SyncLocker, error) {
	lv, _ := g.table.LoadOrStore(name, NewMutexWrapper(&sync.Mutex{}))
	// fmt.Printf("CreateSyncLocker: name: %v, locker: %p, generator: %p\n", name, lv, g)

	return lv.(*MutexWrapper), nil
}

type MemoryRWLockerGenerator struct {
	table sync.Map
}

func NewMemoryRWLockerGenerator() *MemoryRWLockerGenerator {
	return &MemoryRWLockerGenerator{}
}

func (g *MemoryRWLockerGenerator) CreateSyncRWLocker(name string) (SyncRWLocker, error) {
	lv, _ := g.table.LoadOrStore(name, NewRWMutexWrapper(&sync.RWMutex{}))
	// fmt.Printf("CreateSyncRWLocker: name: %v, locker: %p\n", name, lv)
	return lv.(*RWMutexWrapper), nil
}

type MutexWrapper struct {
	m      sync.Mutex
	locker sync.Locker
}

func NewMutexWrapper(locker sync.Locker) *MutexWrapper {
	m := &MutexWrapper{}
	m.m.Lock()
	m.locker = locker
	m.m.Unlock()
	return m
}

func (m *MutexWrapper) Lock(ctx context.Context) error {
	m.m.Lock()
	locker := m.locker
	m.m.Unlock()
	locker.Lock()
	return nil
}

func (m *MutexWrapper) Unlock(ctx context.Context) error {
	m.m.Lock()
	locker := m.locker
	m.m.Unlock()
	locker.Unlock()
	return nil
}

type RWMutexWrapper struct {
	locker *sync.RWMutex
}

func NewRWMutexWrapper(m *sync.RWMutex) *RWMutexWrapper {
	return &RWMutexWrapper{locker: m}
}

func (m *RWMutexWrapper) Lock(ctx context.Context) error {
	m.locker.Lock()
	return nil
}

func (m *RWMutexWrapper) Unlock(ctx context.Context) error {
	m.locker.Unlock()
	return nil
}

func (m *RWMutexWrapper) RLock(ctx context.Context) error {
	m.locker.RLock()
	return nil
}

func (m *RWMutexWrapper) RUnlock(ctx context.Context) error {
	m.locker.RUnlock()
	return nil
}
