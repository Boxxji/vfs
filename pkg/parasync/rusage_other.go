// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package parasync

import "time"

func getProcessCPUTime() (user, sys time.Duration) {
	return 0, 0
}
