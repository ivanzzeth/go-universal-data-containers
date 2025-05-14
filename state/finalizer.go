package state

type Finalizer interface {
	StorageSnapshot
	LoadState(name string, id string) (State, error)
	SaveState(state State) error
	SaveStates(state ...State) error

	ClearCacheStates(states ...State) error
	ClearPersistStates(states ...State) error
	ClearStates(states ...State) error

	FinalizeSnapshot(snapshotID string) error
	FinalizeAllCachedStates() error
	ClearAllCachedStates() error
	EnableAutoFinalizeAllCachedStates(enable bool)

	Close()
}
