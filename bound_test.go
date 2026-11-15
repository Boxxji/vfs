package vfs_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

// The edge of the single-request bound: exactly 64 MiB, one below, one above.
// A conditional write that goes as parts is silently unconditional.
func TestConditionalWriteAtTheSingleRequestBound(t *testing.T) {
	real := storetest.RealS3(t)
	be, err := s3be.New(s3be.Config{Endpoint: real.Endpoint, Insecure: true, Region: "us-east-1", Bucket: real.Bucket, Prefix: real.Prefix("bound"), AccessKey: real.AccessKey, SecretKey: real.SecretKey})
	if err != nil {
		t.Fatal(err)
	}
	const MiB = 1 << 20
	for _, n := range []int{64*MiB - 1, 64 * MiB, 64*MiB + 1, 65 * MiB, 129 * MiB} {
		key := fmt.Sprintf("o%d", n)
		first := make([]byte, n)
		first[0] = 1
		_, err := be.PutIfAbsent(ctx, key, first)
		if err != nil {
			t.Logf("n=%d (%d MiB + %d): PutIfAbsent refused: %v", n, n/MiB, n%MiB, errors.Unwrap(err))
			if !errors.Is(err, backend.ErrTooLarge) {
				t.Errorf("n=%d: refused with %v, want ErrTooLarge", n, err)
			}
			// nothing may be there
			if _, _, gerr := be.Get(ctx, key, backend.Unbounded); !errors.Is(gerr, backend.ErrNotFound) {
				t.Errorf("n=%d: a refused write left an object (%v)", n, gerr)
			}
			continue
		}
		second := make([]byte, n)
		second[0] = 2
		_, err = be.PutIfAbsent(ctx, key, second)
		if !errors.Is(err, backend.ErrConflict) {
			t.Errorf("n=%d: second PutIfAbsent = %v, want ErrConflict (the condition was ignored)", n, err)
		}
		got, v, _ := be.Get(ctx, key, backend.Unbounded)
		if len(got) != n || got[0] != 1 {
			t.Errorf("n=%d: the object was overwritten (first byte %d)", n, got[0])
		}
		third := make([]byte, n)
		third[0] = 3
		if _, err = be.PutIfVersion(ctx, key, third, "nonsense"); !errors.Is(err, backend.ErrConflict) {
			t.Errorf("n=%d: PutIfVersion with a wrong version = %v, want ErrConflict", n, err)
		}
		if _, err = be.PutIfVersion(ctx, key, third, v); err != nil {
			t.Errorf("n=%d: PutIfVersion with the right version = %v", n, err)
		}
		t.Logf("n=%d: conditional writes honoured", n)
	}
}
