// Copyright 2026 Hanzo AI Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	s3backend "github.com/hanzoai/vfs/pkg/backend/s3"
)

// miniS3Server is a lightweight in-process HTTP S3-compatible server (no
// testing package dependency) so cmd/parasync-bench exercises the real
// pkg/backend/s3 (github.com/hanzos3/go) client over HTTP loopback.
type miniS3Server struct {
	bucket string
	srv    *http.Server
	ln     net.Listener

	mu   sync.RWMutex
	objs map[string][]byte
	etag map[string]string
	mod  map[string]time.Time
}

func startMiniS3(bucket string) (*miniS3Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &miniS3Server{
		bucket: bucket,
		ln:     ln,
		objs:   make(map[string][]byte),
		etag:   make(map[string]string),
		mod:    make(map[string]time.Time),
	}
	s.srv = &http.Server{Handler: http.HandlerFunc(s.serve)}
	go func() {
		_ = s.srv.Serve(ln)
	}()
	return s, nil
}

func (s *miniS3Server) Endpoint() string {
	return s.ln.Addr().String()
}

func (s *miniS3Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func (s *miniS3Server) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func s3ETag(b []byte) string {
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
		if _, err := br.Discard(2); err != nil {
			return nil, err
		}
	}
}

func (s *miniS3Server) serve(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(p, "/")
	if bucket != s.bucket {
		s.fail(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	q := r.URL.Query()
	if key == "" {
		switch {
		case q.Has("location"):
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
		case q.Get("list-type") == "2":
			s.serveList(w, q.Get("prefix"))
		default:
			s.fail(w, http.StatusBadRequest, "InvalidRequest")
		}
		return
	}

	switch r.Method {
	case http.MethodPut:
		data, err := readS3Body(r)
		if err != nil {
			s.fail(w, http.StatusBadRequest, "IncompleteBody")
			return
		}
		tag := s3ETag(data)
		s.mu.Lock()
		s.objs[key] = data
		s.etag[key] = tag
		s.mod[key] = time.Now()
		s.mu.Unlock()
		w.Header().Set("ETag", tag)
	case http.MethodGet, http.MethodHead:
		s.mu.RLock()
		cur, exists := s.objs[key]
		tag := s.etag[key]
		modTime := s.mod[key]
		s.mu.RUnlock()
		if !exists {
			s.fail(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", tag)
		w.Header().Set("Last-Modified", modTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")
		data, status := cur, http.StatusOK
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int
			if _, err := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); err != nil || a > b || a >= len(cur) {
				s.fail(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
				return
			}
			if b >= len(cur) {
				b = len(cur) - 1
			}
			data, status = cur[a:b+1], http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(cur)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		s.mu.Lock()
		delete(s.objs, key)
		delete(s.etag, key)
		delete(s.mod, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		s.fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (s *miniS3Server) serveList(w http.ResponseWriter, prefix string) {
	s.mu.RLock()
	var keys []string
	for k := range s.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sizes := make(map[string]int, len(keys))
	mods := make(map[string]time.Time, len(keys))
	for _, k := range keys {
		sizes[k] = len(s.objs[k])
		mods[k] = s.mod[k]
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
	fmt.Fprintf(&out, "<KeyCount>%d</KeyCount>", len(keys))
	for _, k := range keys {
		var esc bytes.Buffer
		_ = xml.EscapeText(&esc, []byte(k))
		fmt.Fprintf(&out, "<Contents><Key>%s</Key><Size>%d</Size><LastModified>%s</LastModified></Contents>",
			esc.String(), sizes[k], mods[k].UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	out.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(out.Bytes())
}

func newLiveHTTPBackend(srv *miniS3Server, prefix string) (backend.Backend, error) {
	return s3backend.New(s3backend.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    srv.bucket,
		Prefix:    prefix,
		AccessKey: "k",
		SecretKey: "s",
	})
}

// linkPacer models a shared aggregate network link of fixed bytesPerSec across
// concurrent streams: each stream overlaps its RTT independently while all
// concurrent transfers share the aggregate bottleneck bandwidth honestly.
type linkPacer struct {
	mu          sync.Mutex
	bytesPerSec float64
	nextFree    time.Time
}

func newLinkPacer(bytesPerSec float64) *linkPacer {
	return &linkPacer{bytesPerSec: bytesPerSec}
}

func (p *linkPacer) waitForBytes(ctx context.Context, n int64) error {
	if p == nil || p.bytesPerSec <= 0 || n <= 0 {
		return nil
	}
	dur := time.Duration(float64(n) / p.bytesPerSec * float64(time.Second))
	p.mu.Lock()
	now := time.Now()
	start := p.nextFree
	if now.After(start) {
		start = now
	}
	end := start.Add(dur)
	p.nextFree = end
	p.mu.Unlock()

	wait := time.Until(end)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// calibratedBackend wraps any backend.Backend (such as live HTTP pkg/backend/s3)
// with per-request RTT latency and shared aggregate link bandwidth pacing.
type calibratedBackend struct {
	backend.Backend
	rtt    time.Duration
	pacer  *linkPacer
	getOps atomic.Int64
	bytes  atomic.Int64
}

func newCalibratedBackend(inner backend.Backend, rtt time.Duration, bwMiBPerSec float64) *calibratedBackend {
	var pacer *linkPacer
	if bwMiBPerSec > 0 {
		pacer = newLinkPacer(bwMiBPerSec * 1024 * 1024)
	}
	return &calibratedBackend{
		Backend: inner,
		rtt:     rtt,
		pacer:   pacer,
	}
}

func (c *calibratedBackend) sleepRTT(ctx context.Context) error {
	if c.rtt <= 0 {
		return nil
	}
	timer := time.NewTimer(c.rtt)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *calibratedBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := c.sleepRTT(ctx); err != nil {
		return nil, "", err
	}
	b, v, err := c.Backend.Get(ctx, key, max)
	if err != nil {
		return nil, "", err
	}
	if err := c.pacer.waitForBytes(ctx, int64(len(b))); err != nil {
		return nil, "", err
	}
	c.getOps.Add(1)
	c.bytes.Add(int64(len(b)))
	return b, v, nil
}

func (c *calibratedBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := c.sleepRTT(ctx); err != nil {
		return nil, err
	}
	b, err := c.Backend.GetRange(ctx, key, off, n)
	if err != nil {
		return nil, err
	}
	if err := c.pacer.waitForBytes(ctx, int64(len(b))); err != nil {
		return nil, err
	}
	c.getOps.Add(1)
	c.bytes.Add(int64(len(b)))
	return b, nil
}

func (c *calibratedBackend) ResetCounters() {
	c.getOps.Store(0)
	c.bytes.Store(0)
	if c.pacer != nil {
		c.pacer.mu.Lock()
		c.pacer.nextFree = time.Time{}
		c.pacer.mu.Unlock()
	}
}
