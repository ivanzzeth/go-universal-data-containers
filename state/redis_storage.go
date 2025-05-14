package state

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
	redis "github.com/redis/go-redis/v9"
)

var (
	_ Storage = (*RedisStorage)(nil)
)

type RedisStorageFactory struct {
	redisClient *redis.Client
	registry    Registry
	locker.SyncLockerGenerator
	newSnapshot func(storageFactory StorageFactory) StorageSnapshot
	table       sync.Map
}

func NewRedisStorageFactory(redisClient *redis.Client, registry Registry, lockerGenerator locker.SyncLockerGenerator, newSnapshot func(storageFactory StorageFactory) StorageSnapshot) *RedisStorageFactory {
	return &RedisStorageFactory{redisClient: redisClient, registry: registry, SyncLockerGenerator: lockerGenerator, newSnapshot: newSnapshot}
}

func (f *RedisStorageFactory) GetOrCreateStorage(name string) (Storage, error) {
	// fmt.Printf("GetOrCreateStorage: %v\n", name)

	onceVal, _ := f.table.LoadOrStore(fmt.Sprintf("%v-once", name), &sync.Once{})

	var err error
	onceVal.(*sync.Once).Do(func() {
		if f.newSnapshot == nil {
			f.newSnapshot = func(storageFactory StorageFactory) StorageSnapshot {
				return NewSimpleStorageSnapshot(f.registry, f, f.SyncLockerGenerator)
			}
		}
		snapshot := f.newSnapshot(f)
		var storage Storage
		storage, err = NewRedisStorage(f.SyncLockerGenerator, f.redisClient, f.registry, snapshot, name)
		storage = NewStorageWithMetrics(storage)

		f.table.LoadOrStore(name, storage)
	})
	if err != nil {
		f.table.Delete(fmt.Sprintf("%v-once", name))
		return nil, err
	}

	storeVal, loaded := f.table.Load(name)
	if !loaded {
		return nil, ErrStorageNotFound
	}

	return storeVal.(Storage), nil
}

// Hash: Key Field Value
// NOTE: <state_name> COULD NOT be "id", ""
// <partition> is "default" by default
// State Storage: state_<state_name>_<partition> <state_id> state_value
// State Id Storage: state_id_<partition> <state_name> state_ids

type RedisStorage struct {
	redisClient *redis.Client
	registry    Registry

	locker sync.Locker
	StorageSnapshot

	partition string

	// Only used for simulating network latency
	delay time.Duration
}

func NewRedisStorage(lockerGenerator locker.SyncLockerGenerator, redisClient *redis.Client, registry Registry, snapshot StorageSnapshot, partition string) (*RedisStorage, error) {
	if partition == "" {
		partition = "default"
	}

	locker, err := GetStorageLockerByName(lockerGenerator, partition)
	if err != nil {
		return nil, err
	}

	s := &RedisStorage{
		redisClient: redisClient,
		locker:      locker,
		registry:    registry,

		partition: partition,
	}

	snapshot.SetStorageForSnapshot(s)
	s.StorageSnapshot = snapshot
	return s, nil
}

func (s *RedisStorage) setDelay(delay time.Duration) {
	s.delay = delay
}

func (s *RedisStorage) StorageType() string {
	return "redis"
}

func (s *RedisStorage) StorageName() string {
	return s.partition
}

func (s *RedisStorage) Lock() {
	s.locker.Lock()
}

func (s *RedisStorage) Unlock() {
	s.locker.Unlock()
}

func (s *RedisStorage) getStateKey(stateName string) string {
	return fmt.Sprintf("state_%s_%s", stateName, s.partition)
}

func (s *RedisStorage) getStateIdKey() string {
	return fmt.Sprintf("state_id_%s", s.partition)
}

func (s *RedisStorage) GetStateIDs(name string) ([]string, error) {
	time.Sleep(s.delay)

	idsStr, err := s.redisClient.HGet(context.Background(), s.getStateIdKey(), name).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, err
	}

	ids := strings.Split(idsStr, ",")

	// fmt.Printf("GetStateIDs: %v\n", idsStr)
	return ids, nil
}

func (s *RedisStorage) GetStateNames() ([]string, error) {
	time.Sleep(s.delay)

	names, err := s.redisClient.HKeys(context.Background(), s.getStateIdKey()).Result()
	if err != nil {
		return nil, err
	}

	return names, nil
}

func (s *RedisStorage) LoadAllStates() ([]State, error) {
	names, err := s.GetStateNames()
	if err != nil {
		return nil, err
	}

	var states []State

	for _, name := range names {
		time.Sleep(s.delay)

		results, err := s.redisClient.HVals(context.Background(), s.getStateKey(name)).Result()
		if err != nil {
			return nil, err
		}

		for _, res := range results {
			state, err := s.registry.NewState(name)
			if err != nil {
				return nil, err
			}

			err = json.Unmarshal([]byte(res), state)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func (s *RedisStorage) LoadState(name string, id string) (State, error) {
	state, err := s.registry.NewState(name)
	if err != nil {
		return nil, err
	}

	// err = state.GetIDMarshaler().UnmarshalStateID(id, state.StateIDComponents()...)
	// if err != nil {
	// 	return nil, err
	// }

	time.Sleep(s.delay)

	res, err := s.redisClient.HGet(context.Background(), s.getStateKey(name), id).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrStateNotFound
		}
		return nil, fmt.Errorf("HGET failed: %v", err)
	}

	// fmt.Printf("LoadState result: %v\n", res)
	if len(res) == 0 {
		return nil, ErrStateNotFound
	}

	err = json.Unmarshal([]byte(res), state)
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}

	return state, nil
}

func (s *RedisStorage) SaveStates(states ...State) error {
	statesToSave := make(map[string]map[string]string) // name => id => value

	for _, state := range states {
		id, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}

		if _, ok := statesToSave[state.StateName()]; !ok {
			statesToSave[state.StateName()] = make(map[string]string)
		}

		val, err := json.Marshal(state)
		if err != nil {
			return err
		}

		statesToSave[state.StateName()][id] = string(val)
	}

	for stateName, kv := range statesToSave {
		time.Sleep(s.delay)

		_, err := s.redisClient.HSet(context.Background(), s.getStateKey(stateName), kv).Result()
		if err != nil {
			return err
		}

		time.Sleep(s.delay)

		idsSlice, err := s.redisClient.HKeys(context.Background(), s.getStateKey(stateName)).Result()
		if err != nil {
			return err
		}

		ids := strings.Join(idsSlice, ",")

		time.Sleep(s.delay)

		// fmt.Printf("SaveStates: stateName=%v, kv=%v, ids=%v\n", stateName, kv, ids)
		_, err = s.redisClient.HSet(context.Background(), s.getStateIdKey(), stateName, ids).Result()
		if err != nil {
			return err
		}

		// idsSaved, _ := s.GetStateIDs(stateName)
		// fmt.Printf("SaveStates after: idsSaved=%v\n", idsSaved)
	}

	// names, _ := s.GetStateNames()

	// fmt.Printf("SaveStates after: names=%v\n", names)

	return nil
}

func (s *RedisStorage) ClearStates(states ...State) (err error) {
	for _, state := range states {
		id, err := state.GetIDMarshaler().MarshalStateID(state.StateIDComponents()...)
		if err != nil {
			return err
		}
		_, err = s.redisClient.HDel(context.Background(), s.getStateKey(state.StateName()), id).Result()
		if err != nil {
			return fmt.Errorf("clear all states failed: %v", err)
		}
	}

	return
}

func (s *RedisStorage) ClearAllStates() error {
	names, err := s.GetStateNames()
	if err != nil {
		return fmt.Errorf("get all state names failed: %v", err)
	}

	for _, name := range names {
		time.Sleep(s.delay)

		_, err = s.redisClient.Del(context.Background(), s.getStateKey(name)).Result()
		if err != nil {
			return fmt.Errorf("clear all states failed: %v", err)
		}
	}

	time.Sleep(s.delay)
	_, err = s.redisClient.Del(context.Background(), s.getStateIdKey()).Result()
	if err != nil {
		return fmt.Errorf("clear all IDs of states failed: %v", err)
	}

	return nil
}
