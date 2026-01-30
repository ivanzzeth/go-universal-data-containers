package queue

import (
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/ivanzzeth/go-universal-data-containers/message"
)

var (
	_ Message[any] = (*JsonMessage[any])(nil)
	_ Message[any] = (*MsgpackMessage[any])(nil)

	// NOTE: if version updated, must update this value.
	// Make sure the version is greater than the current version
	// and backward compatible.
	currMsgVersion = common.MustNewSemanticVersion("0.0.1")
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

	msg.SetVersion(currMsgVersion)

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
	if m.Version().Cmp(currMsgVersion) != 0 {
		return 0
	}

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
	if m.Version().Cmp(currMsgVersion) != 0 {
		return
	}

	metadata := m.Metadata()
	metadata["retry_count"] = m.RetryCount() + 1
	m.SetMetadata(metadata)
	m.RefreshUpdatedAt()
}

func (m *JsonMessage[T]) TotalRetryCount() int {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return 0
	}

	if total, ok := m.Metadata()["total_retry_count"]; ok {
		switch total := total.(type) {
		case int:
			return total
		case float32:
			return int(total)
		case float64:
			return int(total)
		}
		panic("total retry count is not number")
	}

	return 0
}

func (m *JsonMessage[T]) RefreshRetryCount() {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return
	}

	metadata := m.Metadata()
	metadata["retry_count"] = int(0)
	m.SetMetadata(metadata)
	m.RefreshUpdatedAt()
}

func (m *JsonMessage[T]) CreatedAt() time.Time {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return time.Time{}
	}

	if created, ok := m.Metadata()["created_at"]; ok {
		switch v := created.(type) {
		case time.Time:
			return v
		case string:
			// Handle JSON deserialized time as string
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				// Try RFC3339 format as fallback
				t, err = time.Parse(time.RFC3339, v)
				if err != nil {
					return time.Time{}
				}
			}
			return t
		default:
			// Try to convert to time.Time if it's a numeric timestamp
			return time.Time{}
		}
	}

	return time.Time{}
}

func (m *JsonMessage[T]) UpdatedAt() time.Time {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return time.Time{}
	}

	if updated, ok := m.Metadata()["updated_at"]; ok {
		switch v := updated.(type) {
		case time.Time:
			return v
		case string:
			// Handle JSON deserialized time as string
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				// Try RFC3339 format as fallback
				t, err = time.Parse(time.RFC3339, v)
				if err != nil {
					return time.Time{}
				}
			}
			return t
		default:
			// Try to convert to time.Time if it's a numeric timestamp
			return time.Time{}
		}
	}

	return time.Time{}
}

func (m *JsonMessage[T]) RefreshUpdatedAt() {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return
	}

	metadata := m.Metadata()
	metadata["updated_at"] = time.Now()
	m.SetMetadata(metadata)
}

type MsgpackMessage[T any] struct {
	message.MsgpackMessage[T]
}

func NewMsgpackMessage[T any](data T) *MsgpackMessage[T] {
	msg := &MsgpackMessage[T]{}

	msg.SetVersion(currMsgVersion)

	msg.SetData(data)

	msg.SetMetadata(map[string]interface{}{
		"retry_count":       int(0),
		"total_retry_count": int(0),
		"created_at":        time.Now(),
		"updated_at":        time.Now(),
	})
	return msg
}

func (m *MsgpackMessage[T]) RetryCount() int {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return 0
	}

	if retry, ok := m.Metadata()["retry_count"]; ok {
		return toInt(retry)
	}

	return 0
}

func (m *MsgpackMessage[T]) AddRetryCount() {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return
	}

	metadata := m.Metadata()
	metadata["retry_count"] = m.RetryCount() + 1
	m.SetMetadata(metadata)
	m.RefreshUpdatedAt()
}

func (m *MsgpackMessage[T]) TotalRetryCount() int {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return 0
	}

	if total, ok := m.Metadata()["total_retry_count"]; ok {
		return toInt(total)
	}

	return 0
}

func (m *MsgpackMessage[T]) RefreshRetryCount() {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return
	}

	metadata := m.Metadata()
	metadata["retry_count"] = int(0)
	m.SetMetadata(metadata)
	m.RefreshUpdatedAt()
}

func (m *MsgpackMessage[T]) CreatedAt() time.Time {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return time.Time{}
	}

	if created, ok := m.Metadata()["created_at"]; ok {
		return toTime(created)
	}

	return time.Time{}
}

func (m *MsgpackMessage[T]) UpdatedAt() time.Time {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return time.Time{}
	}

	if updated, ok := m.Metadata()["updated_at"]; ok {
		return toTime(updated)
	}

	return time.Time{}
}

func (m *MsgpackMessage[T]) RefreshUpdatedAt() {
	if m.Version().Cmp(currMsgVersion) != 0 {
		return
	}

	metadata := m.Metadata()
	metadata["updated_at"] = time.Now()
	m.SetMetadata(metadata)
}

func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int8:
		return int(val)
	case int16:
		return int(val)
	case int32:
		return int(val)
	case int64:
		return int(val)
	case uint:
		return int(val)
	case uint8:
		return int(val)
	case uint16:
		return int(val)
	case uint32:
		return int(val)
	case uint64:
		return int(val)
	case float32:
		return int(val)
	case float64:
		return int(val)
	default:
		panic("value is not a numeric type")
	}
}

func toTime(v interface{}) time.Time {
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		t, err := time.Parse(time.RFC3339Nano, val)
		if err != nil {
			t, err = time.Parse(time.RFC3339, val)
			if err != nil {
				return time.Time{}
			}
		}
		return t
	default:
		return time.Time{}
	}
}
