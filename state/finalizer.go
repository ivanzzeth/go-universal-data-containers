package state

type Finalizer interface {
	StorageSnapshot
	LoadState(name string, id string) (State, error)
	SaveState(state State) error

	FinalizeAllCachedStates() error
	ClearAllCachedStates() error
}
