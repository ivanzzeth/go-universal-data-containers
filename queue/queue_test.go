package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/locker"
)

var (
	queueOptions = Config{
		LockerGenerator: locker.NewMemoryLockerGenerator(),

		MaxSize:           255,
		MaxHandleFailures: 3,

		PollInterval: DefaultPollInterval,
		MaxRetries:   DefaultMaxRetries,

		ConsumerCount: 3,

		MessageIDGenerator: DefaultOptions.MessageIDGenerator,
	}
)

func init() {
	// handler := log.NewTerminalHandler(os.Stdout, true)
	// logger := log.NewLogger(handler)
	// log.SetDefault(logger)
}

func SpecTestQueueSequencial(t *testing.T, q Queue[[]byte]) {
	maxSize := getMaxSize(q)
	for i := 0; i < maxSize; i++ {
		err := q.Enqueue(context.Background(), []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	if q.MaxSize() != UnlimitedSize {
		err := q.Enqueue(context.Background(), []byte{byte(maxSize + 1)})
		if !errors.Is(err, ErrQueueFull) {
			t.Fatal("expected", ErrQueueFull, "got", err)
		}
	}

	allData := [][]byte{}
	for i := 0; i < maxSize; i++ {
		data, err := q.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		allData = append(allData, data.Data())
	}

	sort.Slice(allData, func(i, j int) bool {
		return bytes.Compare(allData[i], allData[j]) < 0
	})

	for i := 0; i < maxSize; i++ {
		if !bytes.Equal(allData[i], []byte{byte(i)}) {
			t.Fatal("expected", []byte{byte(i)}, "got", allData[i])
		}
	}

	data, err := q.Dequeue(context.Background())
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatal("expected", ErrQueueEmpty, "got", err)
	}

	if data != nil {
		t.Fatal("expected", nil, "got", data)
	}

	q.Close()

	err = q.Enqueue(context.Background(), []byte{byte(0)})
	if !errors.Is(err, ErrQueueClosed) {
		t.Fatal("expected", ErrQueueClosed, "got", err)
	}
}

func SpecTestQueueConcurrent(t *testing.T, q Queue[[]byte]) {
	maxSize := getMaxSize(q)

	datas := [][]byte{}
	go func() {
		for {
			data, err := q.Dequeue(context.Background())
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				t.Logf("Dequeue failed: %v", err)
				continue
			}

			datas = append(datas, data.Data())
		}
	}()

	var wg sync.WaitGroup
	errsChan := make(chan error, maxSize)
	for i := 0; i < maxSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Enqueue(context.Background(), []byte{byte(i)})
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

	timeout := time.NewTimer(2 * time.Second)
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-timeout.C:
			t.Fatalf("timeout, length=%v, maxSize=%v", len(datas), maxSize)
		default:
			if len(datas) >= maxSize {
				sort.Slice(datas, func(i, j int) bool {
					return bytes.Compare(datas[i], datas[j]) < 0
				})

				for i := 0; i < maxSize; i++ {
					if !bytes.Equal(datas[i], []byte{byte(i)}) {
						t.Fatal("expected", []byte{byte(i)}, "got", datas[i])
					}
				}
				return
			}
		}
	}
}

func SpecTestQueueSubscribeHandleReachedMaxFailures(t *testing.T, f Factory[[]byte]) {
	maxFailures := 3
	q, err := f.GetOrCreate("queue", WithMaxHandleFailures(maxFailures))
	if err != nil {
		t.Fatal(err)
	}

	var mutex sync.Mutex
	currFailures := 0
	q.Subscribe(func(msg Message[[]byte]) error {
		if currFailures < maxFailures {
			mutex.Lock()
			defer mutex.Unlock()
			currFailures++
			return errors.New("test max failures")
		}

		return nil
	})

	err = q.Enqueue(context.Background(), []byte(fmt.Sprintf("data")))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	if currFailures != maxFailures {
		t.Fatal("expected", maxFailures, "got", currFailures)
	}
}

func SpecTestQueueSubscribe(t *testing.T, f Factory[[]byte]) {
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

			q.Subscribe(func(msg Message[[]byte]) error {
				b := msg.Data()

				t.Logf("Subscribe in test: %v", msg)
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

				err = q.Enqueue(context.Background(), data)
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

func getMaxSize(q Queue[[]byte]) int {
	maxSize := q.MaxSize()
	if maxSize == UnlimitedSize {
		maxSize = 10
	}

	return maxSize
}
