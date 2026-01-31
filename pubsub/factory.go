package pubsub

import (
	"fmt"
	"sync"
)

// Factory creates PubSub instances for a specific backend
type Factory func(cfg Config) (PubSub, error)

var (
	backends   = make(map[Backend]Factory)
	backendsMu sync.RWMutex
)

// RegisterBackend registers a backend factory.
// This should be called during init() by backend implementations.
// Registering the same backend twice will panic.
func RegisterBackend(name Backend, factory Factory) {
	if factory == nil {
		panic("pubsub: RegisterBackend factory is nil")
	}

	backendsMu.Lock()
	defer backendsMu.Unlock()

	if _, exists := backends[name]; exists {
		panic(fmt.Sprintf("pubsub: RegisterBackend called twice for backend %q", name))
	}

	backends[name] = factory
}

// NewPubSub creates a new PubSub instance using the configured backend.
// Returns ErrUnknownBackend if the backend is not registered.
func NewPubSub(cfg Config) (PubSub, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	backendsMu.RLock()
	factory, ok := backends[cfg.Backend]
	backendsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownBackend, cfg.Backend)
	}

	return factory(cfg)
}

// MustNewPubSub is like NewPubSub but panics on error.
// Use this only in initialization code where failure is unrecoverable.
func MustNewPubSub(cfg Config) PubSub {
	ps, err := NewPubSub(cfg)
	if err != nil {
		panic(fmt.Sprintf("pubsub: failed to create PubSub: %v", err))
	}
	return ps
}

// AvailableBackends returns a list of all registered backend names.
func AvailableBackends() []Backend {
	backendsMu.RLock()
	defer backendsMu.RUnlock()

	result := make([]Backend, 0, len(backends))
	for name := range backends {
		result = append(result, name)
	}
	return result
}

// IsBackendAvailable checks if a backend is registered.
func IsBackendAvailable(name Backend) bool {
	backendsMu.RLock()
	defer backendsMu.RUnlock()
	_, ok := backends[name]
	return ok
}

func validateConfig(cfg Config) error {
	if cfg.Backend == "" {
		return fmt.Errorf("%w: backend cannot be empty", ErrInvalidConfig)
	}

	if cfg.BufferSize < 0 {
		return fmt.Errorf("%w: buffer size cannot be negative", ErrInvalidConfig)
	}

	return nil
}

// init registers the built-in backends
func init() {
	RegisterBackend(BackendMemory, newMemoryPubSub)
	RegisterBackend(BackendRedis, newRedisPubSub)
}
