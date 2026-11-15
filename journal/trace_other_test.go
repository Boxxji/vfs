//go:build linux && !amd64 && !arm64

package journal_test

import "syscall"

// On an architecture the tracer does not know, no call is recognised and the
// tracing tests fail for want of the calls they look for.
type regs = syscall.PtraceRegs

const renameat2 = 1 << 31

func sysnum(*regs) uintptr { return 0 }

func sysarg(*regs, int) uintptr { return 0 }

func setResult(*regs, int) {}
