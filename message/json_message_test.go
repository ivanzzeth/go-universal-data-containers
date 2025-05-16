package message

import "testing"

func TestJsonMessage(t *testing.T) {
	SpecTestMessage(t, &JsonMessage[[]byte]{})
}
