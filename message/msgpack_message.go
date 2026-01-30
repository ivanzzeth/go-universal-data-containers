package message

import (
	"bytes"

	"github.com/ivanzzeth/go-universal-data-containers/common"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	_ Message[any] = (*MsgpackMessage[any])(nil)
)

type MsgpackMessage[T any] struct {
	version  common.SemanticVersion
	id       []byte
	metadata map[string]interface{}
	data     T
}

type msgpackMessageMarshalling[T any] struct {
	Version  common.SemanticVersion `msgpack:"version"`
	ID       []byte                 `msgpack:"id"`
	Metadata map[string]interface{} `msgpack:"metadata"`
	Data     T                      `msgpack:"data"`
}

func (m *MsgpackMessage[T]) Version() common.SemanticVersion {
	if m.version == "" {
		m.version = defaultVersion
	}

	return m.version
}

func (m *MsgpackMessage[T]) SetVersion(version common.SemanticVersion) {
	m.version = version
}

func (m *MsgpackMessage[T]) ID() []byte {
	return m.id
}

func (m *MsgpackMessage[T]) SetID(id []byte) error {
	m.id = id
	return nil
}

func (m *MsgpackMessage[T]) Metadata() map[string]interface{} {
	return m.metadata
}

func (m *MsgpackMessage[T]) SetMetadata(metadata map[string]interface{}) error {
	m.metadata = metadata
	return nil
}

func (m *MsgpackMessage[T]) Data() T {
	return m.data
}

func (m *MsgpackMessage[T]) SetData(data T) error {
	m.data = data
	return nil
}

func (m *MsgpackMessage[T]) Pack() ([]byte, error) {
	msg := msgpackMessageMarshalling[T]{
		Version:  m.Version(),
		ID:       m.id,
		Metadata: m.metadata,
		Data:     m.data,
	}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	err := enc.Encode(&msg)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *MsgpackMessage[T]) Unpack(b []byte) error {
	msg := msgpackMessageMarshalling[T]{}
	err := msgpack.Unmarshal(b, &msg)
	if err != nil {
		return err
	}

	if msg.Version == "" {
		msg.Version = defaultVersion
	}

	m.version = msg.Version
	m.id = msg.ID
	m.metadata = msg.Metadata
	m.data = msg.Data

	return nil
}

func (m *MsgpackMessage[T]) String() string {
	packed, _ := m.Pack()
	return string(packed)
}
