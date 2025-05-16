package queue

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func TestRedisQueueSequencial(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
	q, err := f.GetOrCreate("queue")
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestRedisQueueConcurrent(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
	q, err := f.GetOrCreate("queue")
	if err != nil {
		t.Fatal(err)
	}

	SpecTestQueueConcurrent(t, q)
}

func TestRedisQueueSubscribeHandleReachedMaxFailures(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))
	SpecTestQueueSubscribeHandleReachedMaxFailures(t, f)
}

func TestRedisQueueSubscribe(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb, NewJsonMessage([]byte{}))

	SpecTestQueueSubscribe(t, f)
}
