package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJsonMessage(t *testing.T) {
	msg := NewJsonMessage(1)
	assert.Equal(t, 0, msg.RetryCount())

	msg.AddRetryCount()

	assert.Equal(t, 1, msg.RetryCount())

	msg.AddRetryCount()
	msg.AddRetryCount()

	assert.Equal(t, 3, msg.RetryCount())

	packedData, err := msg.Pack()
	if err != nil {
		t.Fatal(err)
	}

	newMsg := NewJsonMessage(1)
	err = newMsg.Unpack(packedData)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, newMsg.RetryCount())
}

// TestJsonMessage_CreatedAt_StringHandling tests that CreatedAt() correctly handles
// time.Time values and string values (from JSON deserialization)
func TestJsonMessage_CreatedAt_StringHandling(t *testing.T) {
	// Test 1: Normal time.Time value
	msg := NewJsonMessage("test")
	now := time.Now()
	msg.SetMetadata(map[string]interface{}{
		"created_at": now,
	})

	createdAt := msg.CreatedAt()
	assert.False(t, createdAt.IsZero())
	assert.WithinDuration(t, now, createdAt, time.Second)

	// Test 2: RFC3339Nano string format (from JSON deserialization)
	rfc3339NanoTime := time.Now().Format(time.RFC3339Nano)
	msg.SetMetadata(map[string]interface{}{
		"created_at": rfc3339NanoTime,
	})

	createdAt = msg.CreatedAt()
	assert.False(t, createdAt.IsZero())
	expected, _ := time.Parse(time.RFC3339Nano, rfc3339NanoTime)
	assert.WithinDuration(t, expected, createdAt, time.Second)

	// Test 3: RFC3339 string format (fallback)
	rfc3339Time := time.Now().Format(time.RFC3339)
	msg.SetMetadata(map[string]interface{}{
		"created_at": rfc3339Time,
	})

	createdAt = msg.CreatedAt()
	assert.False(t, createdAt.IsZero())
	expected, _ = time.Parse(time.RFC3339, rfc3339Time)
	assert.WithinDuration(t, expected, createdAt, time.Second)

	// Test 4: Invalid string format (should return zero time)
	msg.SetMetadata(map[string]interface{}{
		"created_at": "invalid-time-format",
	})

	createdAt = msg.CreatedAt()
	assert.True(t, createdAt.IsZero())

	// Test 5: Missing created_at (should return zero time)
	msg.SetMetadata(map[string]interface{}{})
	createdAt = msg.CreatedAt()
	assert.True(t, createdAt.IsZero())

	// Test 6: Pack/Unpack roundtrip (simulates queue serialization/deserialization)
	originalMsg := NewJsonMessage("test")
	originalTime := originalMsg.CreatedAt()

	// Pack the message (serialize)
	packedData, err := originalMsg.Pack()
	assert.NoError(t, err)

	// Unpack the message (deserialize - this simulates what happens in queue)
	deserializedMsg := NewJsonMessage("test")
	err = deserializedMsg.Unpack(packedData)
	assert.NoError(t, err)

	// After Pack/Unpack roundtrip, created_at should be a string, but CreatedAt() should still work
	createdAt = deserializedMsg.CreatedAt()
	assert.False(t, createdAt.IsZero())
	assert.WithinDuration(t, originalTime, createdAt, time.Second)
}

// TestJsonMessage_UpdatedAt_StringHandling tests that UpdatedAt() correctly handles
// time.Time values and string values (from JSON deserialization)
func TestJsonMessage_UpdatedAt_StringHandling(t *testing.T) {
	// Test 1: Normal time.Time value
	msg := NewJsonMessage("test")
	now := time.Now()
	msg.SetMetadata(map[string]interface{}{
		"updated_at": now,
	})

	updatedAt := msg.UpdatedAt()
	assert.False(t, updatedAt.IsZero())
	assert.WithinDuration(t, now, updatedAt, time.Second)

	// Test 2: RFC3339Nano string format
	rfc3339NanoTime := time.Now().Format(time.RFC3339Nano)
	msg.SetMetadata(map[string]interface{}{
		"updated_at": rfc3339NanoTime,
	})

	updatedAt = msg.UpdatedAt()
	assert.False(t, updatedAt.IsZero())
	expected, _ := time.Parse(time.RFC3339Nano, rfc3339NanoTime)
	assert.WithinDuration(t, expected, updatedAt, time.Second)

	// Test 3: RFC3339 string format (fallback)
	rfc3339Time := time.Now().Format(time.RFC3339)
	msg.SetMetadata(map[string]interface{}{
		"updated_at": rfc3339Time,
	})

	updatedAt = msg.UpdatedAt()
	assert.False(t, updatedAt.IsZero())
	expected, _ = time.Parse(time.RFC3339, rfc3339Time)
	assert.WithinDuration(t, expected, updatedAt, time.Second)

	// Test 4: Invalid string format (should return zero time)
	msg.SetMetadata(map[string]interface{}{
		"updated_at": "invalid-time-format",
	})

	updatedAt = msg.UpdatedAt()
	assert.True(t, updatedAt.IsZero())

	// Test 5: Missing updated_at (should return zero time)
	msg.SetMetadata(map[string]interface{}{})
	updatedAt = msg.UpdatedAt()
	assert.True(t, updatedAt.IsZero())
}

func TestMsgpackMessage(t *testing.T) {
	msg := NewMsgpackMessage(1)
	assert.Equal(t, 0, msg.RetryCount())

	msg.AddRetryCount()

	assert.Equal(t, 1, msg.RetryCount())

	msg.AddRetryCount()
	msg.AddRetryCount()

	assert.Equal(t, 3, msg.RetryCount())

	packedData, err := msg.Pack()
	if err != nil {
		t.Fatal(err)
	}

	newMsg := NewMsgpackMessage(1)
	err = newMsg.Unpack(packedData)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, newMsg.RetryCount())
}

// TestMsgpackMessage_CreatedAt_Handling tests that CreatedAt() correctly handles
// time.Time values (msgpack preserves time.Time type)
func TestMsgpackMessage_CreatedAt_Handling(t *testing.T) {
	// Test 1: Normal time.Time value
	msg := NewMsgpackMessage("test")
	now := time.Now()
	msg.SetMetadata(map[string]interface{}{
		"created_at": now,
	})

	createdAt := msg.CreatedAt()
	assert.False(t, createdAt.IsZero())
	assert.WithinDuration(t, now, createdAt, time.Second)

	// Test 2: Missing created_at (should return zero time)
	msg.SetMetadata(map[string]interface{}{})
	createdAt = msg.CreatedAt()
	assert.True(t, createdAt.IsZero())

	// Test 3: Pack/Unpack roundtrip (simulates queue serialization/deserialization)
	originalMsg := NewMsgpackMessage("test")
	originalTime := originalMsg.CreatedAt()

	// Pack the message (serialize)
	packedData, err := originalMsg.Pack()
	assert.NoError(t, err)

	// Unpack the message (deserialize - this simulates what happens in queue)
	deserializedMsg := NewMsgpackMessage("test")
	err = deserializedMsg.Unpack(packedData)
	assert.NoError(t, err)

	// After Pack/Unpack roundtrip, created_at should still work
	createdAt = deserializedMsg.CreatedAt()
	assert.False(t, createdAt.IsZero())
	assert.WithinDuration(t, originalTime, createdAt, time.Second)
}

// TestMsgpackMessage_UpdatedAt_Handling tests that UpdatedAt() correctly handles
// time.Time values (msgpack preserves time.Time type)
func TestMsgpackMessage_UpdatedAt_Handling(t *testing.T) {
	// Test 1: Normal time.Time value
	msg := NewMsgpackMessage("test")
	now := time.Now()
	msg.SetMetadata(map[string]interface{}{
		"updated_at": now,
	})

	updatedAt := msg.UpdatedAt()
	assert.False(t, updatedAt.IsZero())
	assert.WithinDuration(t, now, updatedAt, time.Second)

	// Test 2: Missing updated_at (should return zero time)
	msg.SetMetadata(map[string]interface{}{})
	updatedAt = msg.UpdatedAt()
	assert.True(t, updatedAt.IsZero())
}
