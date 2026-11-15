package storetest

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// S3 is an S3 protocol server over memory, path-style, one bucket: the subset a
// Volume uses. It implements the conditional-write contract of S3 (If-None-Match
// and If-Match on PUT answer 412 PreconditionFailed when they fail, decided under
// the same lock as the write) so the S3 backend can be tested through its real
// client, over HTTP, without a storage service. It does not check signatures.
type S3 struct {
	Bucket string
	srv    *httptest.Server

	mu       sync.Mutex
	objs     map[string][]byte
	requests int
}

// Requests returns how many object requests (not bucket queries) have been served.
func (s *S3) Requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// NewS3 starts the server for the life of the test. Endpoint is its host:port.
func NewS3(t *testing.T, bucket string) *S3 {
	s := &S3{Bucket: bucket, objs: map[string][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *S3) Endpoint() string { return strings.TrimPrefix(s.srv.URL, "http://") }

func (s *S3) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func quoted(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// body returns the request payload, decoding the aws-chunked framing a client
// uses to sign a streamed upload.
func body(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") &&
		!strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return io.ReadAll(r.Body)
	}
	br := bufio.NewReader(r.Body)
	var out []byte
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
		chunk := make([]byte, n)
		if _, err := io.ReadFull(br, chunk); err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		if _, err := br.Discard(2); err != nil { // the CRLF after the chunk
			return nil, err
		}
	}
}

func etagMatches(header, etag string) bool {
	for _, e := range strings.Split(header, ",") {
		if e = strings.TrimSpace(e); e == "*" || e == etag {
			return true
		}
	}
	return false
}

func (s *S3) serve(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if bucket != s.Bucket {
		s.fail(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	q := r.URL.Query()
	if key == "" {
		switch {
		case q.Has("location"):
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
		case q.Get("list-type") == "2":
			s.list(w, q.Get("prefix"))
		default:
			s.fail(w, http.StatusBadRequest, "InvalidRequest")
		}
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	cur, exists := s.objs[key]
	switch r.Method {
	case http.MethodPut:
		data, err := body(r)
		if err != nil {
			s.fail(w, http.StatusBadRequest, "IncompleteBody")
			return
		}
		if h := r.Header.Get("If-None-Match"); h != "" && exists && etagMatches(h, quoted(cur)) {
			s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		if h := r.Header.Get("If-Match"); h != "" && (!exists || !etagMatches(h, quoted(cur))) {
			s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		s.objs[key] = data
		w.Header().Set("ETag", quoted(data))
	case http.MethodGet, http.MethodHead:
		if !exists {
			s.fail(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", quoted(cur))
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Accept-Ranges", "bytes")
		data, status := cur, http.StatusOK
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int
			if _, err := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); err != nil || a > b || a >= len(cur) {
				s.fail(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
				return
			}
			b = min(b, len(cur)-1)
			data, status = cur[a:b+1], http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(cur)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		delete(s.objs, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		s.fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (s *S3) list(w http.ResponseWriter, prefix string) {
	s.mu.Lock()
	var keys []string
	for k := range s.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sizes := map[string]int{}
	for _, k := range keys {
		sizes[k] = len(s.objs[k])
	}
	s.mu.Unlock()
	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
	fmt.Fprintf(&out, "<KeyCount>%d</KeyCount>", len(keys))
	for _, k := range keys {
		var esc bytes.Buffer
		_ = xml.EscapeText(&esc, []byte(k))
		fmt.Fprintf(&out, "<Contents><Key>%s</Key><Size>%d</Size><LastModified>2006-01-02T15:04:05.000Z</LastModified></Contents>", esc.String(), sizes[k])
	}
	out.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(out.Bytes())
}
