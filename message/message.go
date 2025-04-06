package message

// Message represents a message that can be sent to a queue, network, etc
// NOTE: It's not thread-safe
type Message interface {
	ID() []byte
	SetID(id []byte) error

	// Used for recording extra data, e.g, retry count
	Metadata() map[string]interface{}
	SetMetadata(metadata map[string]interface{}) error

	Data() []byte
	SetData(data []byte) error

	// Converts the message into a binary package
	Pack() ([]byte, error)

	// Converts a binary package into the message
	Unpack([]byte) error
}

type MessageIDGenerator func() ([]byte, error)
