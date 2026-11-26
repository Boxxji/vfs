// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package parasync

import (
	"syscall"
	"time"
)

// getProcessCPUTime returns the calling process's cumulative user and system
// CPU time via getrusage(RUSAGE_SELF). Works on both Linux and Darwin with
// CGO_ENABLED=0.
func getProcessCPUTime() (user, sys time.Duration) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	u := time.Duration(int64(ru.Utime.Sec))*time.Second + time.Duration(int64(ru.Utime.Usec))*time.Microsecond
	s := time.Duration(int64(ru.Stime.Sec))*time.Second + time.Duration(int64(ru.Stime.Usec))*time.Microsecond
	return u, s
}
