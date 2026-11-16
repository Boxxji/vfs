package storetest

import "testing"

// The server derives ports from the ones it is given: 10000 above, 20000 above (the master
// and the filer), and one more beside the S3 front's. A port that makes any of them pass
// 65535 wraps to a privileged one and the server dies at start, so every choice leaves room
// and none is near another.
func TestFreePortsLeaveRoomForTheServersOwn(t *testing.T) {
	for range 300 {
		ports, err := freePorts(4)
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range ports {
			if p < 1024 || p+20000 >= 32768 {
				t.Fatalf("port %d: the ports derived from it leave the range below the kernel's", p)
			}
			for _, q := range ports[:i] {
				if p-q < 2 && q-p < 2 {
					t.Fatalf("ports %d and %d are near each other", p, q)
				}
			}
		}
	}
}
