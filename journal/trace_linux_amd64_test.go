package journal_test

import "syscall"

type regs = syscall.PtraceRegs

const (
	renameat  = syscall.SYS_RENAMEAT
	renameat2 = 316 // not in the frozen syscall package
)

func sysnum(r *regs) uintptr { return uintptr(r.Orig_rax) }

func sysarg(r *regs, i int) uintptr {
	return [...]uintptr{uintptr(r.Rdi), uintptr(r.Rsi), uintptr(r.Rdx), uintptr(r.R10)}[i]
}

func setResult(r *regs, v int) { r.Rax = uint64(int64(v)) }
