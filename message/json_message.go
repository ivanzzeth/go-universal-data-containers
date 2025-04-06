package message

import "encoding/json"

var (
	_ Message = (*JsonMessage)(nil)
)

type JsonMessage struct {
	id       []byte
	metadata map[string]interface{}
	data     []byte
}

type jsonMessageMarshalling struct {
	ID       []byte                 `json:"id"`
	Metadata map[string]interface{} `json:"metadata"`
	Data     []byte                 `json:"data"`
}

func (m *JsonMessage) ID() []byte {
	return m.id
}

func (m *JsonMessage) SetID(id []byte) error {
	m.id = id
	return nil
}

func (m *JsonMessage) Metadata() map[string]interface{} {
	return m.metadata
}

func (m *JsonMessage) SetMetadata(metadata map[string]interface{}) error {
	m.metadata = metadata
	return nil
}

func (m *JsonMessage) Data() []byte {
	return m.data
}

func (m *JsonMessage) SetData(data []byte) error {
	m.data = data
	return nil
}

func (m *JsonMessage) Pack() ([]byte, error) {
	j := jsonMessageMarshalling{
		ID:       m.id,
		Metadata: m.metadata,
		Data:     m.data,
	}
	return json.Marshal(&j)
}

func (m *JsonMessage) Unpack(b []byte) error {
	j := jsonMessageMarshalling{}
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}

	m.id = j.ID
	m.metadata = j.Metadata
	m.data = j.Data

	return nil
}

func (m *JsonMessage) String() string {
	packed, _ := m.Pack()
	return string(packed)
}
