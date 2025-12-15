package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonMessage(t *testing.T) {
	msg := &JsonMessage[[]byte]{}

	assert.Equal(t, "v0.0.1", string(msg.Version()))

	SpecTestMessage(t, msg)
}
