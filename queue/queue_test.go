package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	queueOptions = QueueOptions{
		MaxSize:      1000,
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

func TestSafeQueueSequencial(t *testing.T) {
	q := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	SpecTestQueueSequencial(t, q)
}

func TestQueueConcurrent(t *testing.T) {
	q := NewMemoryQueue("", &queueOptions)
	SpecTestQueueConcurrent(t, q)
}

func TestSafeQueueConcurrent(t *testing.T) {
	q := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	SpecTestQueueConcurrent(t, q)
}

func TestQueueSubscribe(t *testing.T) {
	q := NewSafeQueue(NewMemoryQueue("", &queueOptions))
	SpecTestQueueSubscribe(t, q)
}

func SpecTestQueueSequencial(t *testing.T, q Queue) {
	for i := 0; i < q.MaxSize(); i++ {
		err := q.Enqueue([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	err := q.Enqueue([]byte{byte(q.MaxSize() + 1)})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatal("expected", ErrQueueFull, "got", err)
	}

	for i := 0; i < q.MaxSize(); i++ {
		data, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if data[0] != byte(i) {
			t.Fatal("expected", i, "got", data[0])
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
	datas := [][]byte{}
	go func() {
		for {
			data, err := q.Dequeue()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			datas = append(datas, data)
		}
	}()

	var wg sync.WaitGroup
	errsChan := make(chan error, q.MaxSize())
	for i := 0; i < q.MaxSize(); i++ {
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
			t.Fatal("timeout")
		default:
			if len(datas) >= q.MaxSize() {
				return
			}
		}
	}
}

func SpecTestQueueSubscribe(t *testing.T, q Queue) {
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

			q.Subscribe(func(b []byte) error {
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
				} else {
					allData[data.Data] = true
				}
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

			time.Sleep(1 * time.Second)

			for _, d := range tt.data {
				if !allData[d.Data] {
					t.Fatalf("expected %v to be included", d.Data)
				}
			}
		})
	}
}
