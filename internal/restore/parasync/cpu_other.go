//go:build !unix

package parasync

import "time"

func processCPUTime() time.Duration {
	return 0
}
