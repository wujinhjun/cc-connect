//go:build !darwin && !linux && !freebsd

package core

import (
	"fmt"
	"os"
)

func lockExecutionFile(string) (*os.File, error) {
	return nil, fmt.Errorf("durable execution locking is unsupported on this OS")
}
