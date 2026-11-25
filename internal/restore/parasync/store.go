package parasync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrObjectNotFound is returned by ObjectStore.Get when an object key does not exist.
var ErrObjectNotFound = errors.New("parasync: object not found")

// StoreStats snapshots object-store transfer telemetry.
type StoreStats struct {
	PutCount        int64
	GetCount        int64
	HeadCount       int64
	BytesUploaded   int64
	BytesDownloaded int64
}

// ObjectStore is the minimal ciphertext-only object storage boundary
// representing hanzoai/s3 beneath hanzoai/vfs.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Head(ctx context.Context, key string) (int64, bool, error)
	Stats() StoreStats
	ResetStats()
}

// HTTPStoreConfig configures the loopback S3-compatible HTTP object store.
type HTTPStoreConfig struct {
	// BaseRTT adds a per-request network round-trip delay to GET requests.
	BaseRTT time.Duration

	// BandwidthBytesPerSec caps aggregate HTTP GET download throughput across
	// all concurrent workers (0 means unthrottled loopback).
	BandwidthBytesPerSec int64

	// ShapePuts applies BaseRTT and BandwidthBytesPerSec to PUT requests too.
	// Defaults to false so fixture setup stays fast while restore GETs are shaped.
	ShapePuts bool
}

// HTTPObjectStore is a real loopback net/http/httptest S3-compatible store
// holding strictly opaque ciphertext objects.
type HTTPObjectStore struct {
	cfg    HTTPStoreConfig
	server *httptest.Server
	client *http.Client

	mu      sync.RWMutex
	objects map[string][]byte

	pacerMu   sync.Mutex
	nextReady time.Time

	putCount        atomic.Int64
	getCount        atomic.Int64
	headCount       atomic.Int64
	bytesUploaded   atomic.Int64
	bytesDownloaded atomic.Int64
}

// NewHTTPObjectStore launches a loopback HTTP object store server.
// Callers must call Close when done.
func NewHTTPObjectStore(cfg HTTPStoreConfig) *HTTPObjectStore {
	s := &HTTPObjectStore{
		cfg:     cfg,
		objects: make(map[string][]byte),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.handleHTTP))
	tr := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		MaxConnsPerHost:     128,
		IdleConnTimeout:     30 * time.Second,
	}
	s.client = &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}
	return s
}

// URL returns the loopback base URL of the S3-compatible HTTP store.
func (s *HTTPObjectStore) URL() string {
	return s.server.URL
}

// Close shuts down the loopback HTTP server and releases idle connections.
func (s *HTTPObjectStore) Close() {
	s.client.CloseIdleConnections()
	s.server.Close()
}

// SetShaping updates RTT and bandwidth shaping parameters between benchmark runs.
func (s *HTTPObjectStore) SetShaping(rtt time.Duration, bwBytesPerSec int64) {
	s.pacerMu.Lock()
	s.cfg.BaseRTT = rtt
	s.cfg.BandwidthBytesPerSec = bwBytesPerSec
	s.nextReady = time.Time{}
	s.pacerMu.Unlock()
}

func (s *HTTPObjectStore) sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *HTTPObjectStore) paceTransfer(ctx context.Context, nBytes int) error {
	s.pacerMu.Lock()
	rtt := s.cfg.BaseRTT
	bw := s.cfg.BandwidthBytesPerSec
	s.pacerMu.Unlock()

	if err := s.sleepWithContext(ctx, rtt); err != nil {
		return err
	}
	if bw <= 0 || nBytes <= 0 {
		return nil
	}
	txDur := time.Duration(float64(nBytes) / float64(bw) * float64(time.Second))
	if txDur <= 0 {
		return nil
	}
	s.pacerMu.Lock()
	now := time.Now()
	if s.nextReady.Before(now) {
		s.nextReady = now
	}
	waitUntil := s.nextReady.Add(txDur)
	s.nextReady = waitUntil
	s.pacerMu.Unlock()

	if wait := time.Until(waitUntil); wait > 0 {
		return s.sleepWithContext(ctx, wait)
	}
	return nil
}

func (s *HTTPObjectStore) handleHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/s3/")
	if key == "" || strings.Contains(key, "..") {
		http.Error(w, "invalid object key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body failed", http.StatusInternalServerError)
			return
		}
		if s.cfg.ShapePuts {
			if err := s.paceTransfer(r.Context(), len(body)); err != nil {
				http.Error(w, "request canceled", http.StatusRequestTimeout)
				return
			}
		}
		blob := append([]byte(nil), body...)
		s.mu.Lock()
		s.objects[key] = blob
		s.mu.Unlock()
		s.putCount.Add(1)
		s.bytesUploaded.Add(int64(len(blob)))
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		s.mu.RLock()
		blob, ok := s.objects[key]
		s.mu.RUnlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := s.paceTransfer(r.Context(), len(blob)); err != nil {
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}
		s.getCount.Add(1)
		s.bytesDownloaded.Add(int64(len(blob)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(blob)

	case http.MethodHead:
		s.mu.RLock()
		blob, ok := s.objects[key]
		s.mu.RUnlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s.headCount.Add(1)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blob)))
		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// Put stores an opaque ciphertext object at key over loopback HTTP.
func (s *HTTPObjectStore) Put(ctx context.Context, key string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.server.URL+"/s3/"+key, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("parasync: http PUT %s: %w", key, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("parasync: http PUT %s: status %d", key, resp.StatusCode)
	}
	return nil
}

// Get retrieves an opaque ciphertext object at key over loopback HTTP.
func (s *HTTPObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.server.URL+"/s3/"+key, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("parasync: http GET %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("parasync: http GET %s: status %d", key, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// Head checks existence and ciphertext byte length of key over loopback HTTP.
func (s *HTTPObjectStore) Head(ctx context.Context, key string) (int64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, s.server.URL+"/s3/"+key, nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return 0, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("parasync: http HEAD %s: status %d", key, resp.StatusCode)
	}
	return resp.ContentLength, true, nil
}

// Stats returns a snapshot of store I/O counters.
func (s *HTTPObjectStore) Stats() StoreStats {
	return StoreStats{
		PutCount:        s.putCount.Load(),
		GetCount:        s.getCount.Load(),
		HeadCount:       s.headCount.Load(),
		BytesUploaded:   s.bytesUploaded.Load(),
		BytesDownloaded: s.bytesDownloaded.Load(),
	}
}

// ResetStats zeroes all transfer counters (used before each measured restore run).
func (s *HTTPObjectStore) ResetStats() {
	s.putCount.Store(0)
	s.getCount.Store(0)
	s.headCount.Store(0)
	s.bytesUploaded.Store(0)
	s.bytesDownloaded.Store(0)
	s.pacerMu.Lock()
	s.nextReady = time.Time{}
	s.pacerMu.Unlock()
}

// ObjectCount returns the total number of ciphertext objects stored.
func (s *HTTPObjectStore) ObjectCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.objects)
}

// Keys returns all stored ciphertext object keys in sorted order.
func (s *HTTPObjectStore) Keys() []string {
	s.mu.RLock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	return keys
}

// AuditNoPlaintext scans every stored object key and raw object body in S3 to
// verify zero plaintext markers or file paths appear anywhere in storage.
func (s *HTTPObjectStore) AuditNoPlaintext(markers [][]byte) (leaks []string, objectsScanned int, totalCipherBytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, blob := range s.objects {
		objectsScanned++
		totalCipherBytes += int64(len(blob))
		kb := []byte(k)
		for _, m := range markers {
			if len(m) == 0 {
				continue
			}
			if bytes.Contains(kb, m) {
				leaks = append(leaks, fmt.Sprintf("key %s contains marker %q", k, string(m)))
			}
			if bytes.Contains(blob, m) {
				leaks = append(leaks, fmt.Sprintf("object %s contains plaintext marker %q", k, string(m)))
			}
		}
	}
	return leaks, objectsScanned, totalCipherBytes
}

// TamperObject flips bits in a stored ciphertext object to test tamper detection.
func (s *HTTPObjectStore) TamperObject(key string, offset int, xorMask byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, ok := s.objects[key]
	if !ok {
		return fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	if offset < 0 || offset >= len(blob) {
		return fmt.Errorf("parasync: tamper offset %d out of range [0,%d)", offset, len(blob))
	}
	mutated := append([]byte(nil), blob...)
	mutated[offset] ^= xorMask
	s.objects[key] = mutated
	return nil
}
