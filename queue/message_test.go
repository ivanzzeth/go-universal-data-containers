package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonMessage(t *testing.T) {
	msg := NewJsonMessage(1)
	assert.Equal(t, 0, msg.RetryCount())

	msg.AddRetryCount()

	assert.Equal(t, 1, msg.RetryCount())

	msg.AddRetryCount()
	msg.AddRetryCount()

	assert.Equal(t, 3, msg.RetryCount())

	packedData, err := msg.Pack()
	if err != nil {
		t.Fatal(err)
	}

	newMsg := NewJsonMessage(1)
	err = newMsg.Unpack(packedData)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, newMsg.RetryCount())
}
