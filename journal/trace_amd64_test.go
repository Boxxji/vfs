package journal_test

import "syscall"

type regs = syscall.PtraceRegs

func sysnum(r *regs) uintptr { return uintptr(r.Orig_rax) }

func sysarg(r *regs, i int) uintptr {
	return [...]uintptr{uintptr(r.Rdi), uintptr(r.Rsi), uintptr(r.Rdx), uintptr(r.R10)}[i]
}

func setResult(r *regs, v int) { r.Rax = uint64(int64(v)) }
