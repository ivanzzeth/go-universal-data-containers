package time

import (
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/ivanzzeth/go-universal-data-containers/queue"
	"github.com/ivanzzeth/go-universal-data-containers/state"
)

func SpecTestTicker(t *testing.T, tickerFactory func() Ticker) {
	tickers := []Ticker{}
	count := 10
	for i := 0; i < count; i++ {
		tickers = append(tickers, tickerFactory())
	}

	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(i int) {
			ticker := tickers[i]
			tick := ticker.Tick()

			t.Logf("wait ticker#%d %p", i, ticker)
			time.Sleep(100 * time.Millisecond)

			for {
				select {
				case tickTime := <-tick:
					t.Logf("ticker#%d: %v", i, tickTime)
					// time.Sleep(time.Second)
					wg.Done()
				case <-time.After(200 * time.Millisecond):
					// t.Logf("ticker#%d timeout", i)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestDistributedTicker(t *testing.T) {
	lockerGenerator := locker.NewMemoryLockerGenerator()
	registry := state.NewSimpleRegistry()
	storageFactory := state.NewMemoryStorageFactory(registry, lockerGenerator, nil)
	snapshot := state.NewSimpleStorageSnapshot(registry, storageFactory, lockerGenerator)
	storage, err := state.NewMemoryStorage(lockerGenerator, registry, snapshot, "memory")
	if err != nil {
		t.Fatal(err)
	}

	qf := queue.NewMemoryFactory()

	SpecTestTicker(t, func() Ticker {
		dt, _ := NewDistributedTicker("test", 100*time.Millisecond, registry, storage, lockerGenerator, qf)

		return dt
	})
}
