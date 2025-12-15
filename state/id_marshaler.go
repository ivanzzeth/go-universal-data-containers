package state

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

var (
	_ IDMarshaler = (*JsonIDMarshaler)(nil)
	_ IDMarshaler = (*Base64IDMarshaler)(nil)
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
	// fmt.Printf("UnmarshalStateID, seperator: %v, ID: %v, fieldsStrs: %v, len(fieldsStrs): %v, len(fields): %v\n",
	// 	c.separator, ID, fieldsStrs, len(fieldsStrs), len(fields))
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

type Base64IDMarshaler struct {
	*JsonIDMarshaler
}

func NewBase64IDMarshaler(seperator string) *Base64IDMarshaler {
	return &Base64IDMarshaler{NewJsonIDMarshaler(seperator)}
}

func (c *Base64IDMarshaler) MarshalStateID(fields ...any) (string, error) {
	id, err := c.JsonIDMarshaler.MarshalStateID(fields...)
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(id))

	return encoded, nil
}

func (c *Base64IDMarshaler) UnmarshalStateID(ID string, fields ...any) error {
	decoded, err := base64.StdEncoding.DecodeString(ID)
	if err != nil {
		return err
	}

	return c.JsonIDMarshaler.UnmarshalStateID(string(decoded), fields...)
}
