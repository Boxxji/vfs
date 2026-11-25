// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StoreMetrics records exact HTTP S3 request and byte counts observed by the
// client or server.
type StoreMetrics struct {
	PutRequests             int64 `json:"put_requests"`
	ConditionalPutRequests  int64 `json:"conditional_put_requests"`
	ConditionalPutConflicts int64 `json:"conditional_put_conflicts"`
	GetRequests             int64 `json:"get_requests"`
	RangeGetRequests        int64 `json:"range_get_requests"`
	HeadRequests            int64 `json:"head_requests"`
	BytesUploaded           int64 `json:"bytes_uploaded"`
	BytesDownloaded         int64 `json:"bytes_downloaded"`
}

// TotalRequests returns all object-store HTTP operations counted in StoreMetrics.
func (m StoreMetrics) TotalRequests() int64 {
	return m.PutRequests + m.ConditionalPutRequests + m.GetRequests + m.RangeGetRequests + m.HeadRequests
}

// TotalPutRequests returns unconditional + conditional PUT counts.
func (m StoreMetrics) TotalPutRequests() int64 {
	return m.PutRequests + m.ConditionalPutRequests
}

// TotalGetRequests returns full GET + HTTP Range GET counts.
func (m StoreMetrics) TotalGetRequests() int64 {
	return m.GetRequests + m.RangeGetRequests
}

// Sub returns the delta between two StoreMetrics snapshots.
func (m StoreMetrics) Sub(before StoreMetrics) StoreMetrics {
	return StoreMetrics{
		PutRequests:             m.PutRequests - before.PutRequests,
		ConditionalPutRequests:  m.ConditionalPutRequests - before.ConditionalPutRequests,
		ConditionalPutConflicts: m.ConditionalPutConflicts - before.ConditionalPutConflicts,
		GetRequests:             m.GetRequests - before.GetRequests,
		RangeGetRequests:        m.RangeGetRequests - before.RangeGetRequests,
		HeadRequests:            m.HeadRequests - before.HeadRequests,
		BytesUploaded:           m.BytesUploaded - before.BytesUploaded,
		BytesDownloaded:         m.BytesDownloaded - before.BytesDownloaded,
	}
}

// ObjectStore is the minimal S3-compatible object store contract used by the
// grouped checkpoint planner. It mirrors pkg/backend.Backend semantics:
// ranged reads plus strict atomic server-side conditional PUTs.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) (etag string, err error)
	PutIfAbsent(ctx context.Context, key string, data []byte) (etag string, err error)
	PutIfMatch(ctx context.Context, key string, data []byte, expectedETag string) (etag string, err error)
	Get(ctx context.Context, key string) (data []byte, etag string, err error)
	GetRange(ctx context.Context, key string, offset, length int64) (data []byte, err error)
	Metrics() StoreMetrics
}

// LoopbackS3Config configures the real loopback HTTP S3-compatible server.
type LoopbackS3Config struct {
	Bucket string

	// RequestRTT simulates per-request network + S3 frontend round-trip latency.
	// Set to 0 for native loopback speed or e.g. 2ms for intra-AZ S3 latency.
	RequestRTT time.Duration

	// PerStreamBandwidthBytesPerSec optionally caps per-HTTP-request transfer
	// rate (e.g. simulating S3 per-connection throughput ceiling so single-stream
	// monoliths cannot saturate multi-stream aggregate bandwidth). 0 means unthrottled.
	PerStreamBandwidthBytesPerSec int64

	// MaxConcurrentRequests limits simultaneous in-flight server requests when > 0.
	MaxConcurrentRequests int
}

type storedObject struct {
	data       []byte
	etag       string
	generation uint64
	modTime    time.Time
}

// LoopbackS3Server is a real net/http/httptest S3-compatible object store with
// atomic server-side If-None-Match / If-Match enforcement (HTTP 412 on conflict),
// RFC 9110 HTTP Range support (206 Partial Content), and exact request/byte counters.
type LoopbackS3Server struct {
	cfg LoopbackS3Config
	srv *httptest.Server
	sem chan struct{}

	mu   sync.RWMutex
	objs map[string]storedObject

	putReqs             atomic.Int64
	condPutReqs         atomic.Int64
	condPutConflicts    atomic.Int64
	getReqs             atomic.Int64
	rangeGetReqs        atomic.Int64
	headReqs            atomic.Int64
	bytesUploaded       atomic.Int64
	bytesDownloaded     atomic.Int64
	globalObjectVersion atomic.Uint64
}

// NewLoopbackS3Server starts a real loopback HTTP S3-compatible server.
func NewLoopbackS3Server(cfg LoopbackS3Config) *LoopbackS3Server {
	if cfg.Bucket == "" {
		cfg.Bucket = "hanzo-vfs-checkpoints"
	}
	s := &LoopbackS3Server{
		cfg:  cfg,
		objs: make(map[string]storedObject),
	}
	if cfg.MaxConcurrentRequests > 0 {
		s.sem = make(chan struct{}, cfg.MaxConcurrentRequests)
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s
}

// URL returns the base HTTP URL of the loopback S3 server.
func (s *LoopbackS3Server) URL() string { return s.srv.URL }

// Bucket returns the configured bucket name.
func (s *LoopbackS3Server) Bucket() string { return s.cfg.Bucket }

// Close shuts down the loopback HTTP server.
func (s *LoopbackS3Server) Close() { s.srv.Close() }

// Client returns an HTTPObjectStore connected to this loopback S3 server.
func (s *LoopbackS3Server) Client() *HTTPObjectStore {
	return NewHTTPObjectStore(s.srv.URL, s.cfg.Bucket, s.srv.Client())
}

// Metrics returns an atomic snapshot of server-side request and byte counters.
func (s *LoopbackS3Server) Metrics() StoreMetrics {
	return StoreMetrics{
		PutRequests:             s.putReqs.Load(),
		ConditionalPutRequests:  s.condPutReqs.Load(),
		ConditionalPutConflicts: s.condPutConflicts.Load(),
		GetRequests:             s.getReqs.Load(),
		RangeGetRequests:        s.rangeGetReqs.Load(),
		HeadRequests:            s.headReqs.Load(),
		BytesUploaded:           s.bytesUploaded.Load(),
		BytesDownloaded:         s.bytesDownloaded.Load(),
	}
}

// ResetMetrics zeroes server-side request and byte counters without deleting objects.
func (s *LoopbackS3Server) ResetMetrics() {
	s.putReqs.Store(0)
	s.condPutReqs.Store(0)
	s.condPutConflicts.Store(0)
	s.getReqs.Store(0)
	s.rangeGetReqs.Store(0)
	s.headReqs.Store(0)
	s.bytesUploaded.Store(0)
	s.bytesDownloaded.Store(0)
}

// ObjectKeys returns a sorted snapshot of all keys currently stored in the bucket.
func (s *LoopbackS3Server) ObjectKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.objs))
	for k := range s.objs {
		keys = append(keys, k)
	}
	return keys
}

func computeETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

func (s *LoopbackS3Server) paceRequest(ctx context.Context, payloadBytes int64) error {
	delay := s.cfg.RequestRTT
	if s.cfg.PerStreamBandwidthBytesPerSec > 0 && payloadBytes > 0 {
		xfer := time.Duration(float64(payloadBytes) / float64(s.cfg.PerStreamBandwidthBytesPerSec) * float64(time.Second))
		delay += xfer
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *LoopbackS3Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-r.Context().Done():
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, ok := strings.Cut(trimmed, "/")
	if !ok || bucket != s.cfg.Bucket || key == "" {
		http.Error(w, "NoSuchBucketOrKey", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodHead:
		s.handleHead(w, r, key)
	case http.MethodDelete:
		s.mu.Lock()
		delete(s.objs, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "MethodNotAllowed", http.StatusMethodNotAllowed)
	}
}

func (s *LoopbackS3Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.paceRequest(r.Context(), int64(len(body))); err != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}

	ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match"))
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	isConditional := ifNoneMatch != "" || ifMatch != ""

	if isConditional {
		s.condPutReqs.Add(1)
	} else {
		s.putReqs.Add(1)
	}

	etag := computeETag(body)

	// Atomic conditional precondition evaluation and write under one mutex.
	s.mu.Lock()
	existing, exists := s.objs[key]
	if ifNoneMatch == "*" && exists {
		s.mu.Unlock()
		s.condPutConflicts.Add(1)
		http.Error(w, "PreconditionFailed: object already exists", http.StatusPreconditionFailed)
		return
	}
	if ifMatch != "" {
		if !exists || existing.etag != ifMatch {
			s.mu.Unlock()
			s.condPutConflicts.Add(1)
			http.Error(w, "PreconditionFailed: ETag mismatch", http.StatusPreconditionFailed)
			return
		}
	}
	gen := s.globalObjectVersion.Add(1)
	copied := make([]byte, len(body))
	copy(copied, body)
	s.objs[key] = storedObject{
		data:       copied,
		etag:       etag,
		generation: gen,
		modTime:    time.Now().UTC(),
	}
	s.mu.Unlock()

	s.bytesUploaded.Add(int64(len(body)))
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Amz-Generation", strconv.FormatUint(gen, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *LoopbackS3Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	s.mu.RLock()
	obj, exists := s.objs[key]
	s.mu.RUnlock()
	if !exists {
		if err := s.paceRequest(r.Context(), 0); err != nil {
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}

	rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
	if rangeHeader != "" {
		start, end, ok := parseByteRange(rangeHeader, int64(len(obj.data)))
		if !ok {
			http.Error(w, "InvalidRange", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		slice := obj.data[start : end+1]
		if err := s.paceRequest(r.Context(), int64(len(slice))); err != nil {
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}
		s.rangeGetReqs.Add(1)
		s.bytesDownloaded.Add(int64(len(slice)))
		w.Header().Set("ETag", obj.etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(slice)
		return
	}

	if err := s.paceRequest(r.Context(), int64(len(obj.data))); err != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}
	s.getReqs.Add(1)
	s.bytesDownloaded.Add(int64(len(obj.data)))
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(obj.data)
}

func (s *LoopbackS3Server) handleHead(w http.ResponseWriter, r *http.Request, key string) {
	if err := s.paceRequest(r.Context(), 0); err != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}
	s.headReqs.Add(1)
	s.mu.RLock()
	obj, exists := s.objs[key]
	s.mu.RUnlock()
	if !exists {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	w.WriteHeader(http.StatusOK)
}

func parseByteRange(hdr string, total int64) (start, end int64, ok bool) {
	if !strings.HasPrefix(hdr, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(hdr, "bytes=")
	loStr, hiStr, found := strings.Cut(spec, "-")
	if !found || loStr == "" || hiStr == "" {
		return 0, 0, false
	}
	lo, err1 := strconv.ParseInt(loStr, 10, 64)
	hi, err2 := strconv.ParseInt(hiStr, 10, 64)
	if err1 != nil || err2 != nil || lo < 0 || hi < lo || hi >= total {
		return 0, 0, false
	}
	return lo, hi, true
}

// HTTPObjectStore is a stdlib net/http S3-compatible client with tuned
// connection pooling and exact atomic client-side telemetry.
type HTTPObjectStore struct {
	baseURL string
	bucket  string
	client  *http.Client

	putReqs          atomic.Int64
	condPutReqs      atomic.Int64
	condPutConflicts atomic.Int64
	getReqs          atomic.Int64
	rangeGetReqs     atomic.Int64
	headReqs         atomic.Int64
	bytesUploaded    atomic.Int64
	bytesDownloaded  atomic.Int64
}

// NewHTTPObjectStore builds a pooled HTTPObjectStore targeting baseURL/bucket.
func NewHTTPObjectStore(baseURL, bucket string, baseClient *http.Client) *HTTPObjectStore {
	tr := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		MaxConnsPerHost:     128,
		IdleConnTimeout:     60 * time.Second,
	}
	if baseClient != nil {
		if existingTr, ok := baseClient.Transport.(*http.Transport); ok && existingTr != nil {
			tr = existingTr.Clone()
			tr.MaxIdleConns = 128
			tr.MaxIdleConnsPerHost = 128
			tr.MaxConnsPerHost = 128
		}
	}
	return &HTTPObjectStore{
		baseURL: strings.TrimRight(baseURL, "/"),
		bucket:  bucket,
		client: &http.Client{
			Transport: tr,
			Timeout:   60 * time.Second,
		},
	}
}

func (c *HTTPObjectStore) objectURL(key string) string {
	return c.baseURL + "/" + c.bucket + "/" + strings.TrimLeft(key, "/")
}

// Put writes an immutable or unconditional object via HTTP PUT.
func (c *HTTPObjectStore) Put(ctx context.Context, key string, data []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(data))
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	c.putReqs.Add(1)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("s3 PUT %s: status %d", key, resp.StatusCode)
	}
	c.bytesUploaded.Add(int64(len(data)))
	return resp.Header.Get("ETag"), nil
}

// PutIfAbsent executes an atomic conditional PUT with If-None-Match: *.
func (c *HTTPObjectStore) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("If-None-Match", "*")
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	c.condPutReqs.Add(1)
	if resp.StatusCode == http.StatusPreconditionFailed {
		c.condPutConflicts.Add(1)
		return "", ErrHeadConflict
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("s3 PutIfAbsent %s: status %d", key, resp.StatusCode)
	}
	c.bytesUploaded.Add(int64(len(data)))
	return resp.Header.Get("ETag"), nil
}

// PutIfMatch executes an atomic conditional PUT with If-Match: expectedETag.
func (c *HTTPObjectStore) PutIfMatch(ctx context.Context, key string, data []byte, expectedETag string) (string, error) {
	if expectedETag == "" {
		return "", fmt.Errorf("%w: expectedETag must be non-empty for PutIfMatch", ErrInvalidConfig)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("If-Match", expectedETag)
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	c.condPutReqs.Add(1)
	if resp.StatusCode == http.StatusPreconditionFailed {
		c.condPutConflicts.Add(1)
		return "", ErrHeadConflict
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("s3 PutIfMatch %s: status %d", key, resp.StatusCode)
	}
	c.bytesUploaded.Add(int64(len(data)))
	return resp.Header.Get("ETag"), nil
}

// Get reads a full object and returns its bytes and ETag.
func (c *HTTPObjectStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, "", ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, "", fmt.Errorf("s3 GET %s: status %d", key, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	c.getReqs.Add(1)
	c.bytesDownloaded.Add(int64(len(data)))
	return data, resp.Header.Get("ETag"), nil
}

// GetRange reads exactly length bytes starting at offset using HTTP Range GET.
func (c *HTTPObjectStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("%w: invalid range offset=%d length=%d", ErrInvalidConfig, offset, length)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("s3 Range GET %s [%d:%d]: status %d", key, offset, offset+length, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("s3 Range GET %s: got %d bytes, want %d", key, len(data), length)
	}
	c.rangeGetReqs.Add(1)
	c.bytesDownloaded.Add(int64(len(data)))
	return data, nil
}

// Metrics returns an atomic snapshot of client-side request and byte counters.
func (c *HTTPObjectStore) Metrics() StoreMetrics {
	return StoreMetrics{
		PutRequests:             c.putReqs.Load(),
		ConditionalPutRequests:  c.condPutReqs.Load(),
		ConditionalPutConflicts: c.condPutConflicts.Load(),
		GetRequests:             c.getReqs.Load(),
		RangeGetRequests:        c.rangeGetReqs.Load(),
		HeadRequests:            c.headReqs.Load(),
		BytesUploaded:           c.bytesUploaded.Load(),
		BytesDownloaded:         c.bytesDownloaded.Load(),
	}
}
