package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	redis "github.com/redis/go-redis/v9"
)

var (
	queueOptions = QueueOptions{
		MaxSize:      255,
		PollInterval: DefaultPollInterval,
		MaxRetries:   DefaultMaxRetries,
	}
)

func init() {
	// handler := log.NewTerminalHandler(os.Stdout, true)
	// logger := log.NewLogger(handler)
	// log.SetDefault(logger)
}

func TestMemoryQueueSequencial(t *testing.T) {
	q := NewMemoryQueue("", &queueOptions)
	SpecTestQueueSequencial(t, q)
}

func TestRedisQueueSequencial(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb)
	q, err := f.GetOrCreate("queue")
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestSafeQueueSequencial(t *testing.T) {
	q, err := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueSequencial(t, q)
}

func TestQueueConcurrent(t *testing.T) {
	q := NewMemoryQueue("", &queueOptions)
	SpecTestQueueConcurrent(t, q)
}

func TestRedisQueueConcurrent(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb)
	q, err := f.GetOrCreate("queue")
	if err != nil {
		t.Fatal(err)
	}

	SpecTestQueueConcurrent(t, q)
}

func TestSafeQueueConcurrent(t *testing.T) {
	q, err := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	if err != nil {
		t.Fatal(err)
	}
	SpecTestQueueConcurrent(t, q)
}

func TestQueueSubscribe(t *testing.T) {
	f := NewMemoryFactory()
	SpecTestQueueSubscribe(t, f)
}

func TestRedisQueueSubscribe(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	f := NewRedisQueueFactory(rdb)

	SpecTestQueueSubscribe(t, f)
}

func SpecTestQueueSequencial(t *testing.T, q Queue) {
	maxSize := getMaxSize(q)
	for i := 0; i < maxSize; i++ {
		err := q.Enqueue([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	if q.MaxSize() != UnlimitedMaxSize {
		err := q.Enqueue([]byte{byte(maxSize + 1)})
		if !errors.Is(err, ErrQueueFull) {
			t.Fatal("expected", ErrQueueFull, "got", err)
		}
	}

	allData := [][]byte{}
	for i := 0; i < maxSize; i++ {
		data, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		allData = append(allData, data)
	}

	sort.Slice(allData, func(i, j int) bool {
		return bytes.Compare(allData[i], allData[j]) < 0
	})

	for i := 0; i < maxSize; i++ {
		if !bytes.Equal(allData[i], []byte{byte(i)}) {
			t.Fatal("expected", []byte{byte(i)}, "got", allData[i])
		}
	}

	data, err := q.Dequeue()
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatal("expected", ErrQueueEmpty, "got", err)
	}

	if data != nil {
		t.Fatal("expected", nil, "got", data)
	}
}

func SpecTestQueueConcurrent(t *testing.T, q Queue) {
	maxSize := getMaxSize(q)

	datas := [][]byte{}
	go func() {
		for {
			data, err := q.Dequeue()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				t.Logf("Dequeue failed: %v", err)
				continue
			}

			datas = append(datas, data)
		}
	}()

	var wg sync.WaitGroup
	errsChan := make(chan error, maxSize)
	for i := 0; i < maxSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Enqueue([]byte{byte(i)})
			errsChan <- err
		}()
	}

	wg.Wait()
	close(errsChan)

	for err := range errsChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	timeout := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-timeout.C:
			t.Fatalf("timeout, length=%v, maxSize=%v", len(datas), maxSize)
		default:
			if len(datas) >= maxSize {
				return
			}
		}
	}
}

func SpecTestQueueSubscribe(t *testing.T, f Factory) {
	type dataAndErr struct {
		Data string
		Err  string
	}

	testcases := []struct {
		data []dataAndErr
	}{
		{
			data: []dataAndErr{
				{"data1", ""},
				{"data2", ""},
				{"data3", ""},
				{"data4", ""},
				{"data5", ""},
			},
		},
		{
			data: []dataAndErr{
				{"data1", ""},
				{"data2", "err1"},
				{"data3", ""},
				{"data4", ""},
				{"data5", "err2"},
			},
		},
		{
			data: []dataAndErr{
				{"data1", ""},
				{"data2", "panic1"},
				{"data3", ""},
				{"data4", "err1"},
				{"data5", "panic2"},
			},
		},
	}

	for i, tt := range testcases {
		t.Run(fmt.Sprintf("#case%d", i), func(t *testing.T) {
			var mutex sync.Mutex
			allData := make(map[string]bool)
			isSubscribeErrReturned := make(map[string]bool)

			q, err := f.GetOrCreate(fmt.Sprintf("test-%d", i))
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()

			q.Subscribe(func(b []byte) error {
				t.Logf("Subscribe in test: %v", string(b))
				var data dataAndErr
				err := json.Unmarshal(b, &data)
				if err != nil {
					t.Fatal(err)
				}

				mutex.Lock()
				defer mutex.Unlock()
				if data.Err != "" && !isSubscribeErrReturned[data.Err] {
					isSubscribeErrReturned[data.Err] = true
					if strings.Contains(data.Err, "panic") {
						panic(data.Err)
					}

					return errors.New(data.Err)
				}

				t.Logf("Store data: %v", data.Data)
				allData[data.Data] = true
				return nil
			})

			for _, d := range tt.data {
				data, err := json.Marshal(&d)
				if err != nil {
					t.Fatal(err)
				}

				err = q.Enqueue(data)
				if err != nil {
					t.Fatalf("failed to enqueue: %v", err)
				}
			}

			time.Sleep(2 * time.Second)

			for _, d := range tt.data {
				if !allData[d.Data] {
					t.Fatalf("expected %v to be included, allData=%v", d.Data, allData)
				}
			}
		})
	}
}

func getMaxSize(q Queue) int {
	maxSize := q.MaxSize()
	if maxSize == UnlimitedMaxSize {
		maxSize = 10
	}

	return maxSize
}
