package queue

import "testing"

func TestMemoryQueueSequencial(t *testing.T) {
	q := NewMemoryQueue("", &queueOptions)
	SpecTestQueueSequencial(t, q)
}

func TestMemoryQueueConcurrent(t *testing.T) {
	q := NewMemoryQueue("", &queueOptions)
	SpecTestQueueConcurrent(t, q)
}

func TestMemoryQueueSubscribeHandleReachedMaxFailures(t *testing.T) {
	f := NewMemoryFactory()
	SpecTestQueueSubscribeHandleReachedMaxFailures(t, f)
}

func TestMemoryQueueSubscribe(t *testing.T) {
	f := NewMemoryFactory()
	SpecTestQueueSubscribe(t, f)
}
