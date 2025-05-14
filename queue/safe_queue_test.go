package queue

import "testing"

func TestSafeQueueSequencial(t *testing.T) {
	mq, _ := NewMemoryQueue("", &queueOptions)
	q, err := NewSimpleQueue(mq)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestSafeQueueConcurrent(t *testing.T) {
	mq, _ := NewMemoryQueue("", &queueOptions)
	q, err := NewSimpleQueue(mq)
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueConcurrent(t, q)
}
