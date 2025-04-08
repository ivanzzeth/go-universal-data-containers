package state

import (
	"encoding/json"
	"strings"
)

var (
	_ IDComposer = (*JsonIDComposer)(nil)
)

type IDComposer interface {
	ComposeStateID(fields ...any) (string, error)
}

type JsonIDComposer struct {
	separator string
}

func NewJsonIDComposer(separator string) *JsonIDComposer {
	return &JsonIDComposer{separator: separator}
}

func (c *JsonIDComposer) ComposeStateID(fields ...any) (string, error) {
	fieldsStrs := []string{}
	for _, field := range fields {
		fieldMarshaled, err := json.Marshal(field)
		if err != nil {
			return "", err
		}

		fieldsStrs = append(fieldsStrs, string(fieldMarshaled))
	}

	return strings.Join(fieldsStrs, c.separator), nil
}
