package vfs_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

// the two ways to name an S3 place give one string; a relative directory gives an absolute one.
func TestPlaceStringsAreOneSpelling(t *testing.T) {
	for _, prefix := range []string{"", "vol1", "a/b"} {
		viaNew, err := s3be.New(s3be.Config{Endpoint: "h:1", Insecure: true, Bucket: "bkt", Prefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		viaURL, err := backend.Open(ctx, "s3://bkt/"+prefix+"?endpoint=h:1&insecure=1")
		if err != nil {
			t.Fatal(err)
		}
		if viaNew.String() != viaURL.String() {
			t.Errorf("prefix %q: New says %q, Open says %q: a Volume written through one does not open through the other", prefix, viaNew.String(), viaURL.String())
		}
	}
	dir := t.TempDir()
	wd, _ := os.Getwd()
	defer os.Chdir(wd)
	os.Chdir(dir)
	b, err := backend.Open(ctx, "file://./rel/store")
	if err != nil {
		t.Fatal(err)
	}
	if want := "file://" + filepath.Join(dir, "rel/store"); !strings.HasSuffix(b.String(), "/rel/store") || !filepath.IsAbs(strings.TrimPrefix(b.String(), "file://")) {
		t.Errorf("a relative directory gave the place %q, want an absolute one like %q", b.String(), want)
	}
}

func TestRedactionCases(t *testing.T) {
	const pw = "pa@ss@word"
	for _, raw := range []string{
		"s3://AK:" + pw + "@bucket/%zz",
		"//AK:" + pw + "@bucket/x",
		"//AK:SECRETPW@bucket/x",
	} {
		_, err := backend.Open(ctx, raw)
		if err == nil {
			t.Errorf("%q opened", raw)
			continue
		}
		for _, leak := range []string{"ss@word", "word", "SECRETPW", "pa@"} {
			if strings.Contains(err.Error(), leak) {
				t.Errorf("%q: the error prints %q: %v", raw, leak, err)
			}
		}
	}
}

func TestRedactionLeaks(t *testing.T) {
	for _, raw := range []string{
		"//AK:SECRETPW@bucket/x",
		"s3//AK:SECRETPW@bucket/x",
		"s3:/AK:SECRETPW@bucket/x",
		" s3://AK:SECRETPW@bucket/x",
		"s3://AK:SECRETPW@bucket/x\n",
		"S3://AK:SECRETPW@bucket/%",
		"://AK:SECRETPW@bucket/x",
		"s3://AK:SECRETPW@/x",
		"s3://AK:SECRETPW@bucket:99999999/x",
		"s3://AK:SECRETPW\x00@bucket/x",
		"s3://AK:SECRETPW%zz@bucket/x",
		"s3:///AK:SECRETPW@bucket/x",
		"file://AK:SECRETPW@h/x%zz",
	} {
		_, err := backend.Open(ctx, raw)
		msg := "<opened>"
		if err != nil {
			msg = err.Error()
		}
		if strings.Contains(msg, "SECRETPW") {
			t.Errorf("%q leaks the secret: %s", raw, msg)
		}
	}
}

// SPEC 2: a prefix with an empty segment is refused, not cleaned, and a URL that names a host
// the file backend cannot reach is refused, not read as a directory.
func TestPlacesAreRefusedNotCleaned(t *testing.T) {
	for _, prefix := range []string{"/vol1", "vol1/", "a//b", "/"} {
		if _, err := s3be.New(s3be.Config{Endpoint: "h:1", Insecure: true, Bucket: "bkt", Prefix: prefix}); err == nil {
			t.Errorf("s3.New accepted the prefix %q", prefix)
		}
	}
	for _, raw := range []string{"s3://bkt/vol1/?endpoint=h:1", "s3://bkt//vol1?endpoint=h:1", "s3://bkt/a//b?endpoint=h:1", "file://host/path", "file://rel/store"} {
		if b, err := backend.Open(ctx, raw); err == nil {
			t.Errorf("%q opened as %s", raw, b)
		}
	}
	for _, raw := range []string{"s3://bkt?endpoint=h:1", "s3://bkt/?endpoint=h:1", "file://localhost" + t.TempDir() + "/x"} {
		if _, err := backend.Open(ctx, raw); err != nil {
			t.Errorf("%q: %v", raw, err)
		}
	}
}

// No error from backend.Open, whatever the spelling, may contain the secret.
func TestRedactionOfEverySpelling(t *testing.T) {
	const sec = "SeCr3tPw"
	spell := []string{
		"s3://AK:%s@h/b", "s3//AK:%s@h/b", "s3:/AK:%s@h/b", "://AK:%s@h/b", "S3://AK:%s@h/b", "s3://AK:%s@h:bad/b",
		"s3://AK:%s@h/%%zz", "s3://AK:%s@[::1/b", "s3://AK:%s@h\n/b", "file://AK:%s@h/x", "s3://%%zz:%s@h/b",
		"http://AK:%s@h", "AK:%s@h/b", "s3 ://AK:%s@h/b", "s3://AK:%s@h@i/b", "s3://AK:p://%s@h/b", "x:y://AK:%s@h/b",
		"AK:p://%s@h/b", "1s3://AK:%s@h/b", "s3://AK:%s@h/b?endpoint=%s", "s3://h/b?endpoint=AK:%s@h:1&insecure=zz",
		"s3://h/b?access=%s&secret=%s&bad=%%zz", "//AK:%s@h/b", "\\\\AK:%s@h\\b", "s3://AK:%s@",
		"s3:///b?x=AK:%s@y%%", "s3://AK:%s%%40@h/b", "s3://AK:%s%%2540@h/b", "s3://AK%%3A%s@h/b",
	}
	for _, f := range spell {
		raw := f
		if n := strings.Count(f, "%s"); n > 0 {
			args := make([]any, n)
			for i := range args {
				args[i] = sec
			}
			raw = fmt.Sprintf(f, args...)
		}
		be, err := backend.Open(context.Background(), raw)
		if err == nil {
			be.Close()
			continue
		}
		if strings.Contains(err.Error(), sec) {
			t.Errorf("LEAK in %q\n  -> %v", raw, err)
		}
	}
}
