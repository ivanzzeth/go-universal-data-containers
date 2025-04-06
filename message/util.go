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
