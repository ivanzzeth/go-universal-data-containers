package message

import "encoding/json"

var (
	_ Message[any] = (*JsonMessage[any])(nil)
)

type JsonMessage[T any] struct {
	id       []byte
	metadata map[string]interface{}
	data     T
}

type jsonMessageMarshalling[T any] struct {
	ID       []byte                 `json:"id"`
	Metadata map[string]interface{} `json:"metadata"`
	Data     T                      `json:"data"` // TODO: use any
}

func (m *JsonMessage[T]) ID() []byte {
	return m.id
}

func (m *JsonMessage[T]) SetID(id []byte) error {
	m.id = id
	return nil
}

func (m *JsonMessage[T]) Metadata() map[string]interface{} {
	return m.metadata
}

func (m *JsonMessage[T]) SetMetadata(metadata map[string]interface{}) error {
	m.metadata = metadata
	return nil
}

func (m *JsonMessage[T]) Data() T {
	return m.data
}

func (m *JsonMessage[T]) SetData(data T) error {
	m.data = data
	return nil
}

func (m *JsonMessage[T]) Pack() ([]byte, error) {
	j := jsonMessageMarshalling[T]{
		ID:       m.id,
		Metadata: m.metadata,
		Data:     m.data,
	}
	return json.Marshal(&j)
}

func (m *JsonMessage[T]) Unpack(b []byte) error {
	j := jsonMessageMarshalling[T]{}
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}

	m.id = j.ID
	m.metadata = j.Metadata
	m.data = j.Data

	return nil
}

func (m *JsonMessage[T]) String() string {
	packed, _ := m.Pack()
	return string(packed)
}
