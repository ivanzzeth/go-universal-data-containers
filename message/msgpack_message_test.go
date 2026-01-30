package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMsgpackMessage(t *testing.T) {
	msg := &MsgpackMessage[[]byte]{}

	assert.Equal(t, "v0.0.1", string(msg.Version()))

	SpecTestMessage(t, msg)
}
