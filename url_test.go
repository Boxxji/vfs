package vfs_test

import (
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
)

// TestBackendURLSecretNotInErrors: credentials do not travel in a backend URL,
// so they cannot be printed by an error or by Stats().Backend.
func TestBackendURLSecretNotInErrors(t *testing.T) {
	// A credential in a URL is refused, and the refusal does not repeat it.
	_, err := backend.Open(ctx, "s3://AKIAEXAMPLE:SuperSecretKey123@bucket/prefix?endpoint=127.0.0.1:1&insecure=1")
	if err == nil {
		t.Fatal("a URL with a credential in it was accepted")
	}
	if strings.Contains(err.Error(), "SuperSecretKey123") || strings.Contains(err.Error(), "AKIAEXAMPLE") {
		t.Errorf("the refusal prints the credential: %v", err)
	}
	// A backend names its bucket and prefix, and nothing else: not the endpoint, not
	// the query, so no error or Stats().Backend that prints the backend repeats them.
	// (Nothing is sent: the endpoint is unreachable, and an unreachable S3 endpoint
	// costs the SDK about half a minute of retries, a limit of its retry policy that
	// belongs to a later change.)
	be, err := backend.Open(ctx, "s3://bucket/vol/one?endpoint=127.0.0.1:1&insecure=1&region=r")
	if err != nil {
		t.Fatal(err)
	}
	if got := be.String(); got != "s3://bucket/vol/one" {
		t.Errorf("Backend.String() = %q, want s3://bucket/vol/one", got)
	}
}
