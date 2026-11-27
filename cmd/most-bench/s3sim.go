// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"
)

// localS3Server is a minimal path-style HTTP S3 server used by cmd/most-bench
// so the real pkg/backend/s3 client executes live HTTP requests without
// requiring external cloud credentials.
type localS3Server struct {
	bucket string
	srv    *httptest.Server

	mu       sync.Mutex
	objs     map[string][]byte
	mod      map[string]time.Time
	requests int64
}

func newLocalS3Server(bucket string) *localS3Server {
	s := &localS3Server{
		bucket: bucket,
		objs:   make(map[string][]byte),
		mod:    make(map[string]time.Time),
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

func (s *localS3Server) Endpoint() string {
	return strings.TrimPrefix(s.srv.URL, "http://")
}

func (s *localS3Server) Requests() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

func (s *localS3Server) Close() {
	s.srv.Close()
}

func (s *localS3Server) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func quotedETag(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func readS3Body(r *http.Request) ([]byte, error) {
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
		sizeHex, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		n, err := strconv.ParseInt(sizeHex, 16, 64)
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
		if _, err := br.Discard(2); err != nil {
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

func (s *localS3Server) serve(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if bucket != s.bucket {
		s.fail(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	if key == "" {
		if r.URL.Query().Has("location") {
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
			return
		}
		s.fail(w, http.StatusBadRequest, "InvalidRequest")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	cur, exists := s.objs[key]

	switch r.Method {
	case http.MethodPut:
		data, err := readS3Body(r)
		if err != nil {
			s.fail(w, http.StatusBadRequest, "IncompleteBody")
			return
		}
		if h := r.Header.Get("If-None-Match"); h != "" && exists && etagMatches(h, quotedETag(cur)) {
			s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		if h := r.Header.Get("If-Match"); h != "" && (!exists || !etagMatches(h, quotedETag(cur))) {
			s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		s.objs[key] = data
		s.mod[key] = time.Now()
		w.Header().Set("ETag", quotedETag(data))
		w.WriteHeader(http.StatusOK)

	case http.MethodGet, http.MethodHead:
		if !exists {
			s.fail(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", quotedETag(cur))
		w.Header().Set("Last-Modified", s.mod[key].UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.Itoa(len(cur)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(cur)
		}

	case http.MethodDelete:
		delete(s.objs, key)
		delete(s.mod, key)
		w.WriteHeader(http.StatusNoContent)

	default:
		s.fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}
