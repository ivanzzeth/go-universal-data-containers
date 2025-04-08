package state

type Registry interface {
	RegisterState(state State) error
	NewState(name string) (State, error)
}
