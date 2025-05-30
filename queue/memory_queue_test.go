package queue

import "testing"

func TestMemoryQueueSequencial(t *testing.T) {
	q, err := NewMemoryQueue("", NewJsonMessage([]byte{}), queueOptions)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestMemoryQueueConcurrent(t *testing.T) {
	q, err := NewMemoryQueue("", NewJsonMessage([]byte{}), queueOptions)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueConcurrent(t, q)
}

func TestMemoryQueueSubscribeHandleReachedMaxFailures(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueSubscribeHandleReachedMaxFailures(t, f)
}

func TestMemoryQueueSubscribe(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueSubscribe(t, f)
}

func TestMemoryQueueTimeout(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueTimeout(t, f)
}

func TestMemoryQueueStressTest(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueStressTest(t, f)
}

func TestMemoryQueueErrorHandling(t *testing.T) {
	f := NewMemoryFactory(NewJsonMessage([]byte{}))
	SpecTestQueueErrorHandling(t, f)
}
