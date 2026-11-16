package journal_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
)

func TestDiscardRacingAppendRefuses(t *testing.T) {
	dir := scratch(t)
	opt := journal.Options{Capacity: 16 * 4096, Segment: 4096}
	j, err := journal.Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	var appended atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c, cancel := context.WithTimeout(context.Background(), time.Second)
			seq, err := j.Append(c, []byte("racing append"))
			if err == nil {
				err = j.Sync(c, seq)
			}
			cancel()
			if errors.Is(err, journal.ErrFull) {
				j.Publish(j.Marks().Durable)
				j.Reclaim()
				continue
			}
			if err != nil {
				t.Errorf("Append/Sync: %v", err)
				return
			}
			appended.Store(seq)
		}
	}()
	locked := 0
	for i := 0; i < 30; i++ {
		last := appended.Load()
		err := journal.Discard(dir, opt, 1, max(last, 1))
		if err == nil {
			t.Errorf("Discard succeeded on a live journal at %d", last)
			break
		}
		if !errors.Is(err, journal.ErrLocked) {
			t.Errorf("Discard on a live journal: %v, want ErrLocked", err)
			break
		}
		locked++
	}
	close(stop)
	wg.Wait()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j2, err := journal.Open(dir, journal.Options{Capacity: opt.Capacity, Segment: opt.Segment, Published: j.Marks().Published})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	if r := j2.Recovery(); r.Last != appended.Load() {
		t.Errorf("after %d refused Discards the journal ends at %d, appended %d", locked, r.Last, appended.Load())
	}
}
