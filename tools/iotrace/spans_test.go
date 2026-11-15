package main

import (
	"reflect"
	"testing"
)

func TestSpansMerge(t *testing.T) {
	var s spans
	s.add(10, 20)
	s.add(30, 40)
	s.add(20, 30) // bridges both
	if want := (spans{{10, 40}}); !reflect.DeepEqual(s, want) {
		t.Fatalf("bridge: %v", s)
	}
	s.add(0, 5)
	s.add(50, 60)
	s.add(5, 10)  // touches the left neighbour
	s.add(15, 25) // inside
	if want := (spans{{0, 40}, {50, 60}}); !reflect.DeepEqual(s, want) {
		t.Fatalf("touch: %v", s)
	}
	s.add(35, 55) // swallows the gap
	if want := (spans{{0, 60}}); !reflect.DeepEqual(s, want) {
		t.Fatalf("swallow: %v", s)
	}
	s.add(7, 7) // empty
	if s.size() != 60 {
		t.Fatalf("size %d", s.size())
	}
}

func TestSpansWithinGapsOverlap(t *testing.T) {
	s := spans{{10, 20}, {30, 40}, {50, 60}}
	if got := s.within(15, 55); got != 5+10+5 {
		t.Errorf("within %d", got)
	}
	if got := s.gaps(0, 70); !reflect.DeepEqual(got, []span{{0, 10}, {20, 30}, {40, 50}, {60, 70}}) {
		t.Errorf("gaps %v", got)
	}
	if got := s.gaps(12, 18); got != nil {
		t.Errorf("covered range has gaps %v", got)
	}
	if got := s.gaps(35, 55); !reflect.DeepEqual(got, []span{{40, 50}}) {
		t.Errorf("gaps %v", got)
	}
	o := spans{{0, 12}, {18, 35}, {55, 100}}
	if got := overlap(s, o); got != 2+2+5+5 {
		t.Errorf("overlap %d", got)
	}
	if overlap(s, o) != overlap(o, s) {
		t.Error("overlap is not symmetric")
	}
}

func TestSpansAlign(t *testing.T) {
	s := spans{{5, 10}, {4090, 4100}, {9000, 9001}}
	got := s.align(12)
	if want := (spans{{0, 4096 * 3}}); !reflect.DeepEqual(got, want) {
		// 5-10 -> page 0; 4090-4100 -> pages 0 and 1; 9000 -> page 2: one run
		t.Errorf("align %v, want %v", got, want)
	}
	c := cover{1: {{0, 100}}, 2: {{4096, 4097}}}
	if got := c.align(12).size(); got != 2*4096 {
		t.Errorf("cover align %d", got)
	}
	d := cover{1: {{50, 60}}, 3: {{0, 10}}}
	if got := c.overlap(d); got != 10 {
		// only file 1 is shared: [0,100) and [50,60)
		t.Errorf("cover overlap %d", got)
	}
}
