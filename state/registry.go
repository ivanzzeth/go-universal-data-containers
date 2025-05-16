package state

var (
	defaultRegistry Registry = NewSimpleRegistry()
)

type Registry interface {
	RegisterState(state State) error
	GetRegisteredStates() []State
	NewState(name string) (State, error)
}

func GetDefaultRegistry() Registry {
	return defaultRegistry
}

func SetDefaultRegistry(registry Registry) {
	defaultRegistry = registry
}
