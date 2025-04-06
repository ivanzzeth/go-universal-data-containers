package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	f := NewMemoryFactory()
	q, err := f.GetOrCreate("queue",
		WithMaxSize(100),
		WithPollInterval(321*time.Second),
		WithMaxRetries(123),
		WithConsumerCount(567),
	)
	if err != nil {
		t.Fatal(err)
	}

	safeq := q.(*SafeQueue)
	instance := safeq.queue.(*MemoryQueue)
	assert.Equal(t, 100, instance.options.MaxSize)
	assert.Equal(t, 321*time.Second, instance.options.PollInterval)
	assert.Equal(t, 123, instance.options.MaxRetries)
	assert.Equal(t, 567, instance.options.ConsumerCount)
}
