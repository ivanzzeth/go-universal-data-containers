package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFactory_AvailableBackends(t *testing.T) {
	backends := AvailableBackends()
	assert.Contains(t, backends, BackendMemory)
	assert.Contains(t, backends, BackendRedis)
}

func TestFactory_IsBackendAvailable(t *testing.T) {
	assert.True(t, IsBackendAvailable(BackendMemory))
	assert.True(t, IsBackendAvailable(BackendRedis))
	assert.False(t, IsBackendAvailable("nonexistent"))
}

func TestFactory_UnknownBackend(t *testing.T) {
	cfg := Config{
		Backend: "unknown",
	}

	_, err := NewPubSub(cfg)
	assert.ErrorIs(t, err, ErrUnknownBackend)
}

func TestFactory_InvalidConfig(t *testing.T) {
	cfg := Config{
		Backend:    BackendMemory,
		BufferSize: -10,
	}

	_, err := NewPubSub(cfg)
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

func TestFactory_MustNewPubSub_Panic(t *testing.T) {
	assert.Panics(t, func() {
		MustNewPubSub(Config{Backend: "invalid"})
	})
}

func TestFactory_MustNewPubSub_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		ps := MustNewPubSub(DefaultConfig())
		ps.Close()
	})
}

func TestFactory_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, BackendMemory, cfg.Backend)
	assert.Equal(t, 100, cfg.BufferSize)
	assert.Equal(t, OverflowDrop, cfg.OnFull)
}

func TestFactory_RegisterBackend(t *testing.T) {
	// Register a custom backend
	customBackend := Backend("custom")
	RegisterBackend(customBackend, func(cfg Config) (PubSub, error) {
		return nil, ErrInvalidConfig
	})

	assert.True(t, IsBackendAvailable(customBackend))
	assert.Contains(t, AvailableBackends(), customBackend)

	// Creating should fail with our custom error
	cfg := Config{
		Backend:    customBackend,
		BufferSize: 100,
	}
	_, err := NewPubSub(cfg)
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

func TestFactory_NewMemoryPubSub(t *testing.T) {
	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 50,
		OnFull:     OverflowBlock,
	})
	require.NoError(t, err)
	defer ps.Close()

	assert.NotNil(t, ps)
}

func TestFactory_ZeroBufferSize(t *testing.T) {
	// Zero buffer size should use default
	ps, err := NewPubSub(Config{
		Backend:    BackendMemory,
		BufferSize: 0,
	})
	require.NoError(t, err)
	defer ps.Close()

	assert.NotNil(t, ps)
}
