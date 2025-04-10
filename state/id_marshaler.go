package state

import (
	"encoding/json"
	"errors"
	"strings"
)

var (
	_ IDMarshaler = (*JsonIDMarshaler)(nil)
)

var (
	ErrStateIDAndFieldsMismatch = errors.New("state ID and fields mismatch")
)

type IDMarshaler interface {
	MarshalStateID(fields ...any) (string, error)
	UnmarshalStateID(ID string, fields ...any) error
}

type JsonIDMarshaler struct {
	separator string
}

func NewJsonIDMarshaler(separator string) *JsonIDMarshaler {
	return &JsonIDMarshaler{separator: separator}
}

func (c *JsonIDMarshaler) MarshalStateID(fields ...any) (string, error) {
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

func (c *JsonIDMarshaler) UnmarshalStateID(ID string, fields ...any) error {
	fieldsStrs := strings.Split(ID, c.separator)
	if len(fieldsStrs) != len(fields) {
		return ErrStateIDAndFieldsMismatch
	}

	for i, fieldStr := range fieldsStrs {
		field := fields[i]
		err := json.Unmarshal([]byte(fieldStr), field)
		if err != nil {
			return err
		}
	}

	return nil
}
