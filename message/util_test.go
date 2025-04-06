package message

import (
	"bytes"
	"fmt"
	"testing"
)

func TestGenerateSequencialID(t *testing.T) {
	idFunc := GenerateSequencialIDFunc()

	for i := 0; i < 10; i++ {
		id, err := idFunc()
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(id, []byte(fmt.Sprintf(`%d`, i+1))) {
			t.Fatal("expected", fmt.Sprintf(`%d`, i+1), "got", string(id))
		}
	}
}
