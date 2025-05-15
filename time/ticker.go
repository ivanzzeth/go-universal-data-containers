package time

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	"github.com/ivanzzeth/go-universal-data-containers/queue"
	"github.com/ivanzzeth/go-universal-data-containers/state"
)

const (
	tickerInterval = 10 * time.Millisecond
)

type Ticker interface {
	Close()
	SetEnabled(enabled bool)
	Tick() <-chan time.Time
}

type DistributedTicker struct {
	name string
	q    queue.Queue
	// locker          SyncLocker
	lockerGenerator locker.SyncLockerGenerator
	registry        state.Registry
	storage         state.Storage

	enabled     atomic.Bool
	ticker      *time.Ticker
	tickOut     chan time.Time
	interval    time.Duration
	exitChannel chan struct{}
}

func NewDistributedTicker(partition, name string, d time.Duration, registry state.Registry, storage state.Storage, lockerGenerator locker.SyncLockerGenerator, qf queue.Factory) (*DistributedTicker, error) {
	// locker, err := lockerGenerator.CreateSyncLocker(fmt.Sprintf("ticker-locker-%v", name))
	// if err != nil {
	// 	return nil, err
	// }

	q, err := qf.GetOrCreateSafe(fmt.Sprintf("ticker-queue-%v-%v", partition, name))
	if err != nil {
		return nil, err
	}

	err = registry.RegisterState(MustNewTickerState(lockerGenerator, partition, name))
	if err != nil {
		return nil, err
	}

	t := &DistributedTicker{
		name: name,
		q:    q,
		// locker:          locker,
		lockerGenerator: lockerGenerator,
		registry:        registry,
		storage:         storage,
		enabled:         atomic.Bool{},
		ticker:          time.NewTicker(d),
		tickOut:         make(chan time.Time),
		interval:        d,
		exitChannel:     make(chan struct{}),
	}

	// log.Printf("New distributed ticker %p, queue: %p, storage: %p\n", t, q, storage)

	t.enabled.Store(true)

	go t.run()

	return t, nil
}

func (d *DistributedTicker) Close() {
	close(d.exitChannel)
}

func (d *DistributedTicker) SetEnabled(enabled bool) {
	d.enabled.Store(enabled)
}

func (d *DistributedTicker) Tick() <-chan time.Time {
	return d.tickOut
}

func (d *DistributedTicker) run() {
	go func() {
		// log.Printf("Subscribe to ticker queue, ticker %p, queue: %p\n", d, d.q)
		d.q.Subscribe(func(msg queue.Message) error {
			// log.Printf("Received tick, ticker %p, queue: %p\n", d, d.q)
			select {
			case d.tickOut <- time.Now():
				// log.Printf("Send tick\n")
			case <-time.NewTimer(d.interval / 2).C:
				// log.Printf("Send tick timeout\n")
				return nil
			}

			return nil
		})
	}()

	for {
		select {
		case <-d.exitChannel:
			return
		case <-d.ticker.C:
			err := func() (err error) {
				if !d.enabled.Load() {
					return fmt.Errorf("ticker is disabled")
				}

				stateTmp := MustNewTickerState(d.lockerGenerator, d.name, d.name)
				stateID, err := state.GetStateID(stateTmp)
				if err != nil {
					return
				}

				// log.Printf("tick3\n")

				stateLocker, err := state.GetStateLockerByName(d.lockerGenerator, stateTmp.StateName(), stateID)
				if err != nil {
					return
				}

				// log.Printf("tick4\n")

				stateLocker.Lock(context.TODO())
				defer stateLocker.Unlock(context.TODO())

				tState, err := d.storage.LoadState(context.TODO(), stateTmp.StateName(), stateID)
				if err != nil {
					if !errors.Is(err, state.ErrStateNotFound) {
						return
					}

					tState = stateTmp
				}

				// log.Printf("tick5\n")

				tickerState, ok := tState.(*TickerState)
				if !ok {
					return fmt.Errorf("failed to cast state to TickerState")
				}

				if tickerState.LastTickTime.Unix() <= 0 {
					tickerState.LastTickTime = time.Now()
				}

				if time.Since(tickerState.LastTickTime) <= d.interval {
					// return nil
					// return fmt.Errorf("not time to tick. elapsed: %v, lastTime: %v, now: %v", time.Since(tickerState.LastTickTime), tickerState.LastTickTime, time.Now())
				}

				// log.Printf("tick6\n")

				tickerState.LastTickTime = time.Now()

				err = d.q.Enqueue(context.TODO(), []byte{})
				if err != nil {
					return
				}

				// log.Printf("tick7\n")

				err = d.storage.SaveStates(context.TODO(), tickerState)
				if err != nil {
					return
				}

				// log.Printf("tick8\n")

				return
			}()
			if err != nil {
				// TODO: logging
				log.Printf("failed to tick: %v\n", err)
			}

			time.Sleep(tickerInterval)
		}
	}
}

type TickerState struct {
	state.GormModel
	state.BaseState
	Name         string
	LastTickTime time.Time
}

func MustNewTickerState(lockerGenerator locker.SyncLockerGenerator, partition, name string) *TickerState {
	f := &TickerState{GormModel: state.GormModel{Partition: partition}, Name: name}

	state, err := state.NewBaseState(lockerGenerator, "ticker_states", state.NewBase64IDMarshaler("_"), f.StateIDComponents())
	if err != nil {
		panic(fmt.Errorf("failed to create base state: %v", err))
	}

	f.BaseState = *state

	err = f.FillID(f)
	if err != nil {
		panic(fmt.Errorf("invalid stateID: %v", err))
	}

	return f
}

func (u *TickerState) StateIDComponents() state.StateIDComponents {
	return []any{&u.Name}
}
