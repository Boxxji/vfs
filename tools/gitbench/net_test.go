package main

import (
	"testing"
	"time"
)

// The lines are from a clone of a repository served over HTTP/2 with protocol
// version 2: discovery, ls-refs, fetch, then a second connection for the
// partial clone's blob fetch.
const curlLog = `11:22:45.352838 http.c:845              == Info: Connected to example.test port 443
11:22:45.413246 http.c:792              => Send header, 0000000238 bytes (0x000000ee)
11:22:45.413249 http.c:804              => Send header: GET /o/r/info/refs?service=git-upload-pack HTTP/2
11:22:46.330168 http.c:804              <= Recv header: HTTP/2 200
11:22:46.330964 http.c:804              => Send header: POST /o/r/git-upload-pack HTTP/2
11:22:46.437909 http.c:804              <= Recv header: HTTP/2 200
11:22:46.441186 http.c:804              => Send header: POST /o/r/git-upload-pack HTTP/2
11:22:46.554648 http.c:804              <= Recv header: HTTP/2 200
11:22:46.662650 http.c:804              => Send header: GET /o/r/info/refs?service=git-upload-pack HTTP/2
11:22:47.159859 http.c:804              <= Recv header: HTTP/2 200
`

func TestRequestsPairSendWithFirstResponse(t *testing.T) {
	got := requests(curlLog)
	want := []request{
		{"get", 916919 * time.Microsecond},
		{"post", 106945 * time.Microsecond},
		{"post", 113462 * time.Microsecond},
		{"get", 497209 * time.Microsecond},
	}
	if len(got) != len(want) {
		t.Fatalf("requests = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d = %v, want %v", i, got[i], want[i])
		}
	}
	if len(requests("")) != 0 || len(requests("garbage\nlines\n")) != 0 {
		t.Fatal("a log with no requests must yield none")
	}
}

func TestProtocolVersion(t *testing.T) {
	if protocol("12:00:00.000001 pkt-line.c:85 packet:          git< version 2\n") != "2" {
		t.Error("version 2 not recognised")
	}
	if protocol("12:00:00.000001 pkt-line.c:85 packet:          git< version 1\n") != "1" {
		t.Error("version 1 not recognised")
	}
	if protocol("12:00:00.000001 pkt-line.c:85 packet:          git< 0000\n") != "0" {
		t.Error("no version line means protocol 0")
	}
}

func TestRemoteNeedsAURL(t *testing.T) {
	if err := remote(nil, nil); err == nil {
		t.Fatal("-url is required")
	}
}
