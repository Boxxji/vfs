// Package s3 is an S3 Backend on Hanzo S3.
//
// It speaks to github.com/hanzoai/s3 through the Hanzo S3 Go SDK
// (github.com/hanzos3/go). The fleet already standardises on that SDK (vm,
// commerce, licensing, ai); a second S3 client would mean a second credential
// model, retry policy and error taxonomy for a backend that makes a handful of
// calls.
//
// The caller names an endpoint, a bucket and a key prefix; the backend assumes
// nothing about which S3-compatible store answers. Conditional writes use the
// If-None-Match and If-Match request headers and the ETag a Get returns; a
// failed precondition (HTTP 412) is backend.ErrConflict.
//
// Objects up to partSize go in one request. Larger objects are uploaded in
// parts by the SDK, which aborts the multipart upload when it fails.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	hs3 "github.com/hanzos3/go"
	"github.com/hanzos3/go/pkg/credentials"

	"github.com/hanzoai/vfs/pkg/backend"
)

func init() {
	backend.Register("s3", openS3)
}

type s3Backend struct {
	client *hs3.Client
	bucket string
	prefix string
	desc   string

	closeOnce sync.Once
}

// partSize is the largest object sent as one request; above it the SDK splits
// the upload into parts of this size. A Volume's objects stay below it.
const partSize = 64 << 20

// Config names one prefix of one bucket on one S3-compatible endpoint.
type Config struct {
	Endpoint string // host[:port] of the S3 API
	Insecure bool   // plain HTTP instead of HTTPS
	Region   string // optional; empty lets the endpoint decide
	Bucket   string
	Prefix   string // key prefix of the Volume inside the bucket

	// Credentials are handed in by the caller, from wherever it keeps them. The
	// backend never looks for a credential anywhere else.
	AccessKey string
	SecretKey string
}

// New opens the backend a Config names.
func New(c Config) (backend.Backend, error) {
	if c.Endpoint == "" || c.Bucket == "" {
		return nil, fmt.Errorf("s3 backend: endpoint and bucket are required")
	}
	prefix := strings.Trim(c.Prefix, "/")
	if err := backend.ValidPrefix(prefix); err != nil {
		return nil, fmt.Errorf("s3 backend: %w", err)
	}
	cli, err := hs3.New(c.Endpoint, &hs3.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: !c.Insecure,
		Region: c.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 backend: %w", err)
	}
	return &s3Backend{
		client: cli,
		bucket: c.Bucket,
		prefix: prefix,
		desc:   "s3://" + c.Bucket + "/" + prefix,
	}, nil
}

// openS3 serves s3://bucket/prefix?endpoint=host:port[&insecure=1][&region=r]. A
// URL carries no credential: one in the userinfo would be copied into every log
// line and error that names the backend, so it is refused. The SDK resolves the
// credential from its standard chain (the S3 access key and secret environment
// variables, then an instance role); a service that holds its credential elsewhere
// calls New instead.
func openS3(_ context.Context, u *url.URL) (backend.Backend, error) {
	if u.User != nil {
		return nil, fmt.Errorf("s3 backend: a credential in the URL is refused; use the environment or s3.New")
	}
	if u.Host == "" {
		return nil, fmt.Errorf("s3 backend: missing bucket")
	}
	prefix := strings.Trim(u.Path, "/")
	if err := backend.ValidPrefix(prefix); err != nil {
		return nil, fmt.Errorf("s3 backend: %w", err)
	}
	q := u.Query()
	cli, err := hs3.New(q.Get("endpoint"), &hs3.Options{
		Creds:  credentials.NewChainCredentials([]credentials.Provider{&credentials.EnvAWS{}, &credentials.EnvMinio{}, &credentials.IAM{}}),
		Secure: q.Get("insecure") == "",
		Region: q.Get("region"),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 backend: %w", err)
	}
	return &s3Backend{
		client: cli,
		bucket: u.Host,
		prefix: prefix,
		desc:   "s3://" + u.Host + "/" + prefix,
	}, nil
}

func (s *s3Backend) k(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// notFound maps the SDK's error taxonomy onto backend.ErrNotFound so callers
// see one sentinel regardless of which storage answered.
func notFound(err error, key string) error {
	switch hs3.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchBucket", "NotFound":
		return fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	return err
}

// conflict maps a failed precondition (HTTP 412, or the 409 some stores answer
// to a conditional write that raced another) onto backend.ErrConflict.
func conflict(err error, key string) error {
	r := hs3.ToErrorResponse(err)
	if r.StatusCode == http.StatusPreconditionFailed || r.StatusCode == http.StatusConflict || r.Code == "PreconditionFailed" {
		return fmt.Errorf("%w: %s", backend.ErrConflict, key)
	}
	return err
}

func (s *s3Backend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.k(key), hs3.GetObjectOptions{})
	if err != nil {
		return nil, "", notFound(err, key)
	}
	defer obj.Close()
	// GetObject is lazy: the request goes out on the first Read, and a missing key
	// surfaces there. Read before Stat: a Stat first is its own HEAD, and the Read
	// after it a second GET conditioned on that ETag, which fails with 412 if the
	// object changed in between. Read first, the one GET carries the body and Stat
	// reports the ETag of that same response without another request. One byte past
	// max is read so a body that is too long is seen to be, and no further.
	b, err := io.ReadAll(io.LimitReader(obj, backend.Past(max)))
	if err != nil {
		return nil, "", notFound(err, key)
	}
	if int64(len(b)) > max {
		return nil, "", fmt.Errorf("%w: %s is larger than %d bytes", backend.ErrTooLarge, key, max)
	}
	info, err := obj.Stat()
	if err != nil {
		return nil, "", notFound(err, key)
	}
	return b, backend.Version(info.ETag), nil
}

func (s *s3Backend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	var opts hs3.GetObjectOptions
	if err := opts.SetRange(off, off+n-1); err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.k(key), opts)
	if err != nil {
		return nil, notFound(err, key)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		return nil, notFound(err, key)
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("s3 backend: range %d+%d of %s returned %d bytes", off, n, key, len(b))
	}
	return b, nil
}

func (s *s3Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, s.k(key),
		bytes.NewReader(data), int64(len(data)), hs3.PutObjectOptions{PartSize: partSize})
	return err
}

// tooLarge refuses a conditional write the single-request path cannot carry. The
// condition travels with the one PUT; a larger object goes in parts, and the parts
// ignore it, so such a write would silently be unconditional.
func tooLarge(key string, n int) error {
	return fmt.Errorf("%w: a conditional write of %d bytes to %s exceeds the %d bytes one request carries", backend.ErrTooLarge, n, key, partSize)
}

func (s *s3Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if len(data) > partSize {
		return "", tooLarge(key, len(data))
	}
	opts := hs3.PutObjectOptions{PartSize: partSize}
	opts.SetMatchETagExcept("*")
	info, err := s.client.PutObject(ctx, s.bucket, s.k(key), bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		return "", conflict(err, key)
	}
	return backend.Version(info.ETag), nil
}

func (s *s3Backend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if len(data) > partSize {
		return "", tooLarge(key, len(data))
	}
	opts := hs3.PutObjectOptions{PartSize: partSize}
	opts.SetMatchETag(string(v))
	info, err := s.client.PutObject(ctx, s.bucket, s.k(key), bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		// If-Match on an absent key is a lost precondition too, though some
		// stores say NoSuchKey instead of 412.
		if hs3.ToErrorResponse(err).Code == "NoSuchKey" {
			return "", fmt.Errorf("%w: %s is absent", backend.ErrConflict, key)
		}
		return "", conflict(err, key)
	}
	return backend.Version(info.ETag), nil
}

func (s *s3Backend) Delete(ctx context.Context, key string) error {
	// Idempotent by contract — an absent key is not an error.
	return s.client.RemoveObject(ctx, s.bucket, s.k(key), hs3.RemoveObjectOptions{})
}

func (s *s3Backend) List(ctx context.Context, prefix string) (<-chan string, <-chan error) {
	keys := make(chan string, 256)
	errCh := make(chan error, 1)
	go func() {
		defer close(keys)
		defer close(errCh)
		for obj := range s.client.ListObjects(ctx, s.bucket, hs3.ListObjectsOptions{
			Prefix:    s.k(prefix),
			Recursive: true,
		}) {
			if obj.Err != nil {
				errCh <- obj.Err
				return
			}
			k := obj.Key
			if s.prefix != "" {
				k = strings.TrimPrefix(k, s.prefix+"/")
			}
			select {
			case <-ctx.Done():
				return
			case keys <- k:
			}
		}
	}()
	return keys, errCh
}

func (s *s3Backend) Stat(ctx context.Context, key string) (int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, s.k(key), hs3.StatObjectOptions{})
	if err != nil {
		return 0, notFound(err, key)
	}
	return info.Size, nil
}

func (s *s3Backend) Close() error {
	s.closeOnce.Do(func() {})
	return nil
}

func (s *s3Backend) String() string { return s.desc }

var _ backend.Backend = (*s3Backend)(nil)
