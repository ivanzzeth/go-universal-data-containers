package queue

import (
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/message"
)

var (
	_ Message[any] = (*JsonMessage[any])(nil)
)

type Message[T any] interface {
	message.Message[T]

	RetryCount() int
	AddRetryCount()
	TotalRetryCount() int
	RefreshRetryCount()

	CreatedAt() time.Time
	UpdatedAt() time.Time

	RefreshUpdatedAt()
}

type JsonMessage[T any] struct {
	message.JsonMessage[T]
}

func NewJsonMessage[T any](data T) *JsonMessage[T] {
	msg := &JsonMessage[T]{}

	msg.SetData(data)

	msg.SetMetadata(map[string]interface{}{
		"retry_count":       int(0),
		"total_retry_count": int(0),
		"created_at":        time.Now(),
		"updated_at":        time.Now(),
	})
	return msg
}

func (m *JsonMessage[T]) RetryCount() int {
	if retry, ok := m.Metadata()["retry_count"]; ok {
		switch retry := retry.(type) {
		case int:
			return retry
		case float32:
			return int(retry)
		case float64:
			return int(retry)
		}
		panic("retry count is not int")
	}

	return 0
}

func (m *JsonMessage[T]) AddRetryCount() {
	metadata := m.Metadata()
	metadata["retry_count"] = m.RetryCount() + 1
	m.SetMetadata(metadata)
	m.RefreshUpdatedAt()
}

func (m *JsonMessage[T]) TotalRetryCount() int {
	if total, ok := m.Metadata()["total_retry_count"]; ok {
		switch total := total.(type) {
		case int:
			return total
		case float32:
			return int(total)
		case float64:
			return int(total)
		}
		panic("total retry count is not int")
	}

	return 0
}

func (m *JsonMessage[T]) RefreshRetryCount() {
	metadata := m.Metadata()
	metadata["retry_count"] = int(0)
	m.SetMetadata(metadata)
	m.RefreshUpdatedAt()
}

func (m *JsonMessage[T]) CreatedAt() time.Time {
	if created, ok := m.Metadata()["created_at"]; ok {
		return created.(time.Time)
	}

	return time.Time{}
}

func (m *JsonMessage[T]) UpdatedAt() time.Time {
	if updated, ok := m.Metadata()["updated_at"]; ok {
		return updated.(time.Time)
	}

	return time.Time{}
}

func (m *JsonMessage[T]) RefreshUpdatedAt() {
	metadata := m.Metadata()
	metadata["updated_at"] = time.Now()
	m.SetMetadata(metadata)
}
