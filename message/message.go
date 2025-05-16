package message

// Message represents a message that can be sent to a queue, network, etc
// NOTE: It's not thread-safe
type Message[T any] interface {
	ID() []byte
	SetID(id []byte) error

	// Used for recording extra data, e.g, retry count
	Metadata() map[string]interface{}
	SetMetadata(metadata map[string]interface{}) error

	Data() T
	SetData(data T) error

	// Converts the message into a binary package
	Pack() ([]byte, error)

	// Converts a binary package into the message
	Unpack([]byte) error

	String() string
}

type MessageIDGenerator func() ([]byte, error)
