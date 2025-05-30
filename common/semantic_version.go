package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type SemanticVersion string

func MustNewSemanticVersion(version string) SemanticVersion {
	v, err := NewSemanticVersion(version)
	if err != nil {
		panic(err)
	}
	return v
}

func NewSemanticVersion(version string) (SemanticVersion, error) {
	major, minor, patch, err := SplitSemanticVersion(version)
	if err != nil {
		return "", err
	}

	return SemanticVersion(fmt.Sprintf("v%d.%d.%d", major, minor, patch)), nil
}

func (v SemanticVersion) Cmp(v2 SemanticVersion) int {
	major1, minor1, patch1, err := SplitSemanticVersion(string(v))
	if err != nil {
		panic(err)
	}

	major2, minor2, patch2, err := SplitSemanticVersion(string(v2))
	if err != nil {
		panic(err)
	}

	if major1 > major2 {
		return 1
	}

	if major1 < major2 {
		return -1
	}

	if minor1 > minor2 {
		return 1
	}

	if minor1 < minor2 {
		return -1
	}

	if patch1 > patch2 {
		return 1
	}

	if patch1 < patch2 {
		return -1
	}

	return 0
}

func SplitSemanticVersion(version string) (major, minor, patch int, err error) {
	version = strings.TrimPrefix(version, "v")

	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return 0, 0, 0, errors.New("invalid semantic version")
	}

	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, err
	}

	if major < 0 {
		return 0, 0, 0, fmt.Errorf("invalid major version %d", major)
	}

	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, err
	}

	if minor < 0 {
		return 0, 0, 0, fmt.Errorf("invalid minor version %d", minor)
	}

	patch, err = strconv.Atoi(parts[2])
	if err != nil {
		return 0, 0, 0, err
	}

	if patch < 0 {
		return 0, 0, 0, fmt.Errorf("invalid patch version %d", patch)
	}

	return
}
