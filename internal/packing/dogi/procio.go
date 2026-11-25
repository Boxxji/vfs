package dogi

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// ProcIO captures real kernel I/O accounting counters from Linux /proc/self/io.
type ProcIO struct {
	Available  bool   `json:"available"`
	RChar      uint64 `json:"rchar"`
	WChar      uint64 `json:"wchar"`
	SysCR      uint64 `json:"syscr"`
	SysCW      uint64 `json:"syscw"`
	ReadBytes  uint64 `json:"read_bytes"`
	WriteBytes uint64 `json:"write_bytes"`
}

// ReadProcIO reads /proc/self/io when available on Linux.
func ReadProcIO() ProcIO {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return ProcIO{}
	}
	defer f.Close()

	var p ProcIO
	p.Available = true
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			continue
		}
		switch strings.TrimSpace(k) {
		case "rchar":
			p.RChar = n
		case "wchar":
			p.WChar = n
		case "syscr":
			p.SysCR = n
		case "syscw":
			p.SysCW = n
		case "read_bytes":
			p.ReadBytes = n
		case "write_bytes":
			p.WriteBytes = n
		}
	}
	return p
}

// Sub returns the saturating delta (end - start) of two ProcIO snapshots.
func (end ProcIO) Sub(start ProcIO) ProcIO {
	if !end.Available || !start.Available {
		return ProcIO{}
	}
	sub := func(a, b uint64) uint64 {
		if a >= b {
			return a - b
		}
		return 0
	}
	return ProcIO{
		Available:  true,
		RChar:      sub(end.RChar, start.RChar),
		WChar:      sub(end.WChar, start.WChar),
		SysCR:      sub(end.SysCR, start.SysCR),
		SysCW:      sub(end.SysCW, start.SysCW),
		ReadBytes:  sub(end.ReadBytes, start.ReadBytes),
		WriteBytes: sub(end.WriteBytes, start.WriteBytes),
	}
}
