package queue

import "testing"

func TestSafeQueueSequencial(t *testing.T) {
	q, err := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestSafeQueueConcurrent(t *testing.T) {
	q, err := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueConcurrent(t, q)
}
