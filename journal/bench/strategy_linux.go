package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
	"unsafe"
)

// strategy is the measurement behind the journal's write strategy: how long a
// durable write of each size takes with each way of making it durable, on a
// file that is fully written and on one that was only fallocated. Strategies
// are interleaved and repeated so that a disturbance hits all of them.
func strategy(work string) {
	const span = 1 << 30
	zero := filepath.Join(work, "zero")
	prealloc := filepath.Join(work, "fallocate")
	makeFile(zero, span, true)
	makeFile(prealloc, span, false)
	type way struct {
		name  string
		flags int
		sync  bool
		path  string
	}
	ways := []way{
		{"buffered+fdatasync, fully written", 0, true, zero},
		{"buffered+fdatasync, fallocated", 0, true, prealloc},
		{"O_DSYNC, fully written", syscall.O_DSYNC, false, zero},
		{"O_DIRECT+fdatasync, fully written", syscall.O_DIRECT, true, zero},
		{"O_DIRECT|O_DSYNC, fully written", syscall.O_DIRECT | syscall.O_DSYNC, false, zero},
	}
	reps := 5
	fmt.Printf("%-36s %9s %12s %12s %12s\n", "strategy ("+load()+")", "size", "p50 median", "p50 best", "MB/s median")
	for _, size := range []int{4096, 65536, 1 << 20} {
		p50s := make([][]time.Duration, len(ways))
		rates := make([][]float64, len(ways))
		for r := 0; r < reps; r++ {
			for i, w := range ways {
				p50, rate := timeWrites(w.path, w.flags, w.sync, size, 1200*time.Millisecond, span)
				p50s[i] = append(p50s[i], p50)
				rates[i] = append(rates[i], rate)
			}
		}
		for i, w := range ways {
			sort.Slice(p50s[i], func(a, b int) bool { return p50s[i][a] < p50s[i][b] })
			sort.Float64s(rates[i])
			fmt.Printf("%-36s %9d %12v %12v %12.1f\n", w.name, size, p50s[i][reps/2], p50s[i][0], rates[i][reps/2])
		}
	}
}

func aligned(n int) []byte {
	b := make([]byte, n+4096)
	off := int(4096 - uintptr(unsafe.Pointer(&b[0]))%4096)
	if off == 4096 {
		off = 0
	}
	return b[off : off+n]
}

func makeFile(path string, size int64, written bool) {
	os.Remove(path)
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR, 0o644)
	check(err)
	defer syscall.Close(fd)
	if written {
		z := aligned(1 << 20)
		for off := int64(0); off < size; off += int64(len(z)) {
			_, err := syscall.Pwrite(fd, z, off)
			check(err)
		}
	} else {
		check(syscall.Fallocate(fd, 0, 0, size))
	}
	check(syscall.Fsync(fd))
}

func timeWrites(path string, flags int, sync bool, size int, d time.Duration, span int64) (time.Duration, float64) {
	fd, err := syscall.Open(path, syscall.O_RDWR|flags, 0)
	check(err)
	defer syscall.Close(fd)
	buf := aligned(size)
	for i := range buf {
		buf[i] = byte(i * 7)
	}
	var lat []time.Duration
	var off int64
	start := time.Now()
	for time.Since(start) < d {
		t0 := time.Now()
		_, err := syscall.Pwrite(fd, buf, off)
		check(err)
		if sync {
			check(syscall.Fdatasync(fd))
		}
		lat = append(lat, time.Since(t0))
		if off += int64(size); off+int64(size) > span {
			off = 0
		}
	}
	el := time.Since(start).Seconds()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	return lat[len(lat)/2], float64(len(lat)) * float64(size) / el / 1e6
}
