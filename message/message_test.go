package message

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func SpecTestMessage(t *testing.T, msg Message) {
	testcases := []struct {
		id       []byte
		metadata map[string]interface{}
		data     []byte
	}{
		{[]byte("id1"), map[string]interface{}{"a": 1}, []byte("data1")},
		{[]byte("id2"), map[string]interface{}{"a": 2}, []byte("data2")},
		{[]byte("id3"), map[string]interface{}{"a": 3}, []byte("data3")},
		{[]byte("id4"), map[string]interface{}{"a": 4}, []byte("data4")},
		{[]byte("id5"), map[string]interface{}{"a": 5}, []byte("data5")},
	}

	for i, tt := range testcases {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			err := msg.SetID(tt.id)
			if err != nil {
				t.Fatal(err)
			}

			err = msg.SetMetadata(tt.metadata)
			if err != nil {
				t.Fatal(err)
			}

			err = msg.SetData(tt.data)
			if err != nil {
				t.Fatal(err)
			}

			id := msg.ID()
			if !bytes.Equal(id, tt.id) {
				t.Fatal("ID: expected", tt.id, "got", id)
			}

			metadata := msg.Metadata()
			if !reflect.DeepEqual(metadata, tt.metadata) {
				t.Fatal("Metadata: expected", tt.metadata, "got", metadata)
			}

			data := msg.Data()
			if !bytes.Equal(data, tt.data) {
				t.Fatal("Data: expected", tt.data, "got", data)
			}

			packed, err := msg.Pack()
			if err != nil {
				t.Fatal(err)
			}

			newMsg := reflect.New(reflect.TypeOf(msg).Elem()).Interface().(Message)
			err = newMsg.Unpack(packed)
			if err != nil {
				t.Fatal(err)
			}

			newMsgPacked, err := newMsg.Pack()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(packed, newMsgPacked) {
				t.Fatal("Packed: expected", packed, "got", newMsgPacked)
			}
		})
	}
}
