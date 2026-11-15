//go:build !linux

package main

import "errors"

func evict(roots []string) (int, int64, error) {
	return 0, 0, errors.New("evict needs Linux")
}
