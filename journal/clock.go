package journal

import "time"

// Clock supplies acknowledgement times and deadline timers. Tests drive the
// journal with a clock they control.
type Clock interface {
	Now() time.Time
	// AfterFunc runs f once, in its own goroutine, after d.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a pending AfterFunc.
type Timer interface {
	// Stop prevents the function from running. It reports whether it did.
	Stop() bool
}

type wallClock struct{}

func (wallClock) Now() time.Time                            { return time.Now() }
func (wallClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }
