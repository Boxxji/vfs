package backend

import (
	"math"
	"strings"
	"testing"
)

func TestValidPrefix(t *testing.T) {
	for _, ok := range []string{"", "a", "org/vol", "org-1/vol_2/x.y", "a b/c", "a%2eb"} {
		if err := ValidPrefix(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"a//b", "/a", "a/", "//", "a/./b", "./a", "a/..", "../a", "a/../b", ".", "..",
		"a\x00b", "a\nb", "a\tb", "a\x7fb", strings.Repeat("x", 513),
	} {
		if err := ValidPrefix(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// TestPast: the byte count to read to learn an object is longer than max does not
// overflow when max is Unbounded.
func TestPast(t *testing.T) {
	if Past(10) != 11 || Past(0) != 1 {
		t.Fatal("Past(max) is not max+1")
	}
	if p := Past(Unbounded); p != math.MaxInt64 || p < 0 {
		t.Fatalf("Past(Unbounded) = %d", p)
	}
}
