package state

type Registry interface {
	RegisterState(state State) error
	GetRegisteredStates() []State
	NewState(name string) (State, error)
}
