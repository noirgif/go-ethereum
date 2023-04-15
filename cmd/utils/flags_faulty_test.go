package utils

import (
	"testing"
)

func Test_FaultyFlag(t *testing.T) {
	SetErrorInjectionConfig(nil, nil)
}
