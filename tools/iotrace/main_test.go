package main

import (
	"os"
	"testing"
)

// TestMain lets the test binary stand in for iotrace when calibrate
// re-executes itself to run the probe.
func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "probe" {
		if err := probe(os.Args[2]); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestCalibrateMeasuresTheCostOfATracedCall(t *testing.T) {
	needTrace(t)
	c, err := calibrate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// the cost of a traced call is positive; whether a large read costs more
	// than a small one is a matter of the machine's timer, not of calibrate
	if c.Call <= 0 || c.Read <= 0 || c.N < 20000 {
		t.Errorf("%+v", c)
	}
}

func TestRootFlag(t *testing.T) {
	r := roots{}
	if err := r.Set("cache=/c"); err != nil || r["cache"] != "/c" {
		t.Errorf("%v %v", r, err)
	}
	for _, bad := range []string{"cache", "=/c", "cache="} {
		if err := r.Set(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
