package backend

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestValidPrefix(t *testing.T) {
	for _, ok := range []string{"", "a", "org/vol", "org-1/vol_2/x.y", "a b/c", "a%2eb", strings.Repeat("x", 512), strings.Repeat("x/", 255) + "x"} {
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

// TestRedact: what a URL carries before its last "@" is dropped from every error that names
// it, and only a well-formed scheme is kept.
func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"s3://AK:pw@h/b":         "s3://***@h/b",
		"s3+x.y-z://u:p@h":       "s3+x.y-z://***@h",
		"S3://u:p@h":             "S3://***@h",
		"AK:p://SECRET@h/b":      "***@h/b", // a colon is not in a scheme
		"u:SEC://RET@h/b":        "***@h/b",
		"1s3://u:p@h":            "***@h", // a scheme does not start with a digit
		"s 3://u:p@h":            "***@h",
		"://u:p@h":               "***@h",
		"s3//AK:pw@h":            "***@h",
		"s3:/AK:pw@h":            "***@h",
		"//AK:pw@h/b":            "***@h/b",
		"u:SECRET@x://y":         "***@x://y", // a "://" after the "@" is not the scheme's
		"s3://AK:p://SECRET@h/b": "s3://***@h/b",
		"s3://AK:pa@ss@word@h/b": "s3://***@h/b",
		"no credential anywhere": "no credential anywhere",
		"s3://bucket/prefix?x=1": "s3://bucket/prefix?x=1",
		"":                       "",
		"s3://u:p@h/%zz":         "s3://***@h/%zz",
	} {
		if got := redact(in); got != want {
			t.Errorf("redact(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestErrorsDoNotEchoACredentialInAPrefix: the prefix of a URL whose authority did not parse
// as one is in the error, and what precedes its "@" is not.
func TestErrorsDoNotEchoACredentialInAPrefix(t *testing.T) {
	for _, p := range []string{"/AK:SECRET@h/b", "a//SECRET@b", "x/../SECRET@y", "a\x00SECRET@b"} {
		err := ValidPrefix(p)
		if err == nil {
			t.Errorf("%q accepted", p)
			continue
		}
		if strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%q: the error prints the secret: %v", p, err)
		}
	}
}

func TestValidKey(t *testing.T) {
	for _, ok := range []string{"head", "a", "a-b/c-d", "meta/00/" + strings.Repeat("f", 64), "ext/x/" + strings.Repeat("0", 64),
		strings.Repeat("a", 255), strings.Repeat("a", 255) + "/" + strings.Repeat("b", 255)} {
		if err := ValidKey(ok); err != nil {
			t.Errorf("%.40q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Head", "a b", "a.b", ".", "..", "a/../b", "a//b", "/a", "a/", "a\\b", "a:b", "\u00e9", "a%2e", "a\x00",
		strings.Repeat("a", 256), strings.Repeat("a", 255) + "/" + strings.Repeat("b", 256), strings.Repeat("a/", 256), strings.Repeat("a", 512)} {
		if err := ValidKey(bad); !errors.Is(err, ErrKey) {
			t.Errorf("%.40q: %v, want ErrKey", bad, err)
		}
	}
	for _, ok := range []string{"", "a", "ext/y/a", "ext/y/", "ext/y"} {
		if err := ValidKeyPrefix(ok); err != nil {
			t.Errorf("prefix %q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"A", "a.", "/a", "a//b", "a b", strings.Repeat("a", 512)} {
		if err := ValidKeyPrefix(bad); !errors.Is(err, ErrKey) {
			t.Errorf("prefix %.20q: %v, want ErrKey", bad, err)
		}
	}
}
