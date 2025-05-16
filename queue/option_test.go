package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	q, err := f.GetOrCreate("queue",
		WithMaxSize(100),
		WithPollInterval(321*time.Second),
		WithMaxRetries(123),
		WithConsumerCount(567),
	)
	if err != nil {
		t.Fatal(err)
	}

	safeq := q.(*SimpleQueue[[]byte])
	instance := safeq.queue.(*MemoryQueue[[]byte])
	assert.Equal(t, 100, instance.config.MaxSize)
	assert.Equal(t, 321*time.Second, instance.config.PollInterval)
	assert.Equal(t, 123, instance.config.MaxRetries)
	assert.Equal(t, 567, instance.config.ConsumerCount)
}
