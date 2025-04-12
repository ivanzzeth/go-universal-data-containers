package locker

import "sync"

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

func (g *MemoryLockerGenerator) CreateSyncLocker(name string) (sync.Locker, error) {
	lv, _ := g.table.LoadOrStore(name, &sync.Mutex{})
	return lv.(*sync.Mutex), nil
}

type MemoryRWLockerGenerator struct {
	table sync.Map
}

func NewMemoryRWLockerGenerator() *MemoryRWLockerGenerator {
	return &MemoryRWLockerGenerator{}
}

func (g *MemoryRWLockerGenerator) CreateSyncRWLocker(name string) (sync.Locker, error) {
	lv, _ := g.table.LoadOrStore(name, &sync.Mutex{})
	return lv.(*sync.Mutex), nil
}
