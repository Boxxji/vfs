package journal_test

import "syscall"

type regs = syscall.PtraceRegs

const (
	renameat  = syscall.SYS_RENAMEAT
	renameat2 = 276 // not in the frozen syscall package
)

func sysnum(r *regs) uintptr { return uintptr(r.Regs[8]) }

func sysarg(r *regs, i int) uintptr { return uintptr(r.Regs[i]) }

func setResult(r *regs, v int) { r.Regs[0] = uint64(int64(v)) }
