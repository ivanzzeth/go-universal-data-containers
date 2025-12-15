package common_test

import (
	"testing"

	"github.com/ivanzzeth/go-universal-data-containers/common"
)

func TestSemanticVersion(t *testing.T) {
	testcases := []struct {
		version1 string
		version2 string
		expected int
	}{
		{"0.0.1", "0.0.1", 0},
		{"v0.0.1", "v0.0.1", 0},

		{"0.0.1", "0.0.2", -1},
		{"0.0.2", "0.0.1", 1},
		{"0.1.0", "0.0.1", 1},
		{"0.0.1", "0.1.0", -1},
		{"0.1.1", "0.1.0", 1},
		{"0.1.0", "0.1.1", -1},
		{"1.0.0", "0.0.1", 1},
		{"0.0.1", "1.0.0", -1},
	}

	for _, tc := range testcases {
		t.Run(tc.version1+" vs "+tc.version2, func(t *testing.T) {
			v1 := common.MustNewSemanticVersion(tc.version1)
			v2 := common.MustNewSemanticVersion(tc.version2)
			if v1.Cmp(v2) != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, v1.Cmp(v2))
			}
		})
	}
}
