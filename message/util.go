package message

import (
	"fmt"
	"math"
	"math/rand"
)

var (
	_ MessageIDGenerator = GenerateRandomID
)

func GenerateRandomID() ([]byte, error) {
	return []byte(fmt.Sprintf(`%d`, rand.Intn(math.MaxInt))), nil
}

func GenerateSequencialIDFunc() MessageIDGenerator {
	var id int

	fn := func() ([]byte, error) {
		id++
		return []byte(fmt.Sprintf(`%d`, id)), nil
	}

	return fn
}
