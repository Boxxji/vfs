package main

import "testing"

const unit = 1 << 20

func key(file int32, idx int64) uint64 { return uint64(file)<<32 | uint64(idx) }

func rq(t int64, keys ...uint64) Req {
	q := Req{T: t}
	for _, k := range keys {
		q.Ext = append(q.Ext, ext{k, 100})
	}
	return q
}

func big(int32) int64 { return 10 * unit }

func TestNoCacheFetchesEveryRead(t *testing.T) {
	r := simulate([]Req{rq(0, 1), rq(100, 1), rq(200, 2)}, big, Config{Fetch: 10})
	if r.Miss != 3 || r.Hit != 0 || r.Fetched != 300 || r.Stalled != 3 || r.Stall != 30 {
		t.Errorf("%+v", r)
	}
}

func TestLRUKeepsRecentlyUsed(t *testing.T) {
	// cache holds two extents. A is refreshed before C arrives, so B goes.
	r := simulate([]Req{rq(0, 1), rq(100, 2), rq(200, 1), rq(300, 3), rq(400, 1), rq(500, 2)}, big, Config{Cache: 250, Fetch: 10, Shift: 20})
	if r.Hit != 2 || r.Miss != 4 {
		t.Errorf("hit %d miss %d, want 2 and 4", r.Hit, r.Miss)
	}
}

func TestLRUSkipsWhatCannotFit(t *testing.T) {
	l := newlru(50)
	l.put(&node{key: 1, size: 100})
	if l.get(1) != nil || l.used != 0 {
		t.Error("an extent larger than the cache must not be stored")
	}
}

func TestLRUEvictionCallbackAndSizes(t *testing.T) {
	l := newlru(100)
	var gone []uint64
	l.evicted = func(n *node) { gone = append(gone, n.key) }
	l.put(&node{key: 1, size: 60})
	l.put(&node{key: 2, size: 30})
	l.get(1)
	l.put(&node{key: 3, size: 30}) // 120 > 100: evicts 2, the oldest
	if len(gone) != 1 || gone[0] != 2 || l.used != 90 {
		t.Errorf("evicted %v used %d", gone, l.used)
	}
}

func TestPrefetchArrivesLate(t *testing.T) {
	order := []ext{{1, 100}, {2, 100}}
	r := simulate([]Req{rq(5, 1), rq(100, 2)}, big, Config{Cache: 1 << 30, Fetch: 10, Conc: 1, Order: order, Hold: unread})
	if r.Late != 1 || r.Hit != 1 || r.Miss != 0 || r.Stall != 5 {
		t.Errorf("%+v", r)
	}
}

func TestPrefetchServesTheSameSetInAnotherOrder(t *testing.T) {
	var order []ext
	var reqs []Req
	for i := 1; i <= 100; i++ {
		order = append(order, ext{uint64(i), 100})
	}
	for i := 100; i >= 1; i-- { // reverse order, long after everything arrived
		reqs = append(reqs, Req{T: int64(1e9 + (100-i)*1000), Ext: []ext{{uint64(i), 100}}})
	}
	r := simulate(reqs, big, Config{Cache: 1 << 30, Fetch: 1000, Conc: 8, Order: order, Hold: unread})
	if r.Hit != 100 || r.Miss != 0 || r.Late != 0 {
		t.Errorf("%+v", r)
	}
}

func TestDivergedRunWastesAtMostTheBudget(t *testing.T) {
	var order []ext
	for i := 1; i <= 1000; i++ {
		order = append(order, ext{uint64(i), 1 << 20})
	}
	r := simulate([]Req{{T: 1e9, Ext: []ext{{99999, 1}}}}, big, Config{Cache: 8 << 30, Fetch: 1, Conc: 64, Order: order, Hold: 64 << 20})
	if r.Fetched != 64<<20+1 || r.Wasted != 64<<20 {
		t.Errorf("fetched %d wasted %d, want a 64 MiB budget", r.Fetched, r.Wasted)
	}
}

func TestUnreadPrefetchIsWaste(t *testing.T) {
	order := []ext{{1, 100}, {2, 100}, {3, 100}}
	r := simulate([]Req{rq(1000, 1)}, big, Config{Cache: 1 << 30, Fetch: 1, Conc: 4, Order: order, Hold: unread})
	if r.Wasted != 200 || r.Fetched != 300 || r.Hit != 1 {
		t.Errorf("%+v", r)
	}
}

func TestEvictedPrefetchIsWaste(t *testing.T) {
	order := []ext{{1, 100}, {2, 100}, {3, 100}}
	r := simulate([]Req{rq(1000, 3)}, big, Config{Cache: 150, Fetch: 1, Conc: 4, Order: order, Hold: unread})
	// 1 and 2 were evicted unread by their successors; 3 was read.
	if r.Wasted != 200 {
		t.Errorf("%+v", r)
	}
}

func TestReadAheadFollowsConfirmedSequences(t *testing.T) {
	seq := []Req{rq(0, key(1, 0)), rq(1000, key(1, 1)), rq(2000, key(1, 2)), rq(3000, key(1, 3))}
	r := simulate(seq, big, Config{Cache: 1 << 30, Fetch: 10, Conc: 4, Ahead: true, Shift: 20})
	if r.Miss != 2 || r.Hit != 2 {
		t.Errorf("sequential: %+v", r)
	}
	// 2 demand extents of 100 bytes, then idx 2,3 (window 2), 4,5,6 (window 4), 7,8,9 (window 8, to the end of the file)
	if want := int64(200 + 8*unit); r.Fetched != want {
		t.Errorf("fetched %d, want %d", r.Fetched, want)
	}
	if want := int64(6 * unit); r.Wasted != want {
		t.Errorf("wasted %d, want %d", r.Wasted, want)
	}
	jumpy := []Req{rq(0, key(1, 0)), rq(1000, key(1, 5)), rq(2000, key(1, 2)), rq(3000, key(1, 7))}
	r = simulate(jumpy, big, Config{Cache: 1 << 30, Fetch: 10, Conc: 4, Ahead: true, Shift: 20})
	if r.Miss != 4 || r.Fetched != 400 {
		t.Errorf("random: %+v", r)
	}
	r = simulate(seq, big, Config{Cache: 1 << 30, Fetch: 10, Conc: 4, Shift: 20})
	if r.Miss != 4 {
		t.Errorf("without read-ahead: %+v", r)
	}
}

func TestAWaitingReadNeverWaitsLongerThanAFetch(t *testing.T) {
	// One slot: read-ahead queues extents 2 and 3 behind each other.
	// Extent 3 would arrive at 1020; a read at 1001 fetches it itself.
	reqs := []Req{rq(0, key(1, 0)), rq(1000, key(1, 1)), rq(1001, key(1, 3))}
	r := simulate(reqs, big, Config{Cache: 1 << 30, Fetch: 10, Conc: 1, Ahead: true, Shift: 20})
	if r.Late != 1 || r.Stall != 10+10+10 {
		t.Errorf("%+v", r)
	}
}

func TestSameNodeKeepsWhatTheLastRunLeft(t *testing.T) {
	reqs := []Req{rq(0, 1), rq(1, 2)}
	cfg := Config{Cache: 1 << 30, Fetch: 10}
	if r := simulate(reqs, big, cfg); r.Miss != 2 {
		t.Errorf("cold: %+v", r)
	}
	if r := follow(reqs, reqs, big, cfg); r.Hit != 2 || r.Miss != 0 || r.Fetched != 0 {
		t.Errorf("warm: %+v", r)
	}
	small := Config{Cache: 150, Fetch: 10}
	if r := follow(reqs, reqs, big, small); r.Miss != 2 {
		// the first run left only extent 2; reading 1 evicts it before it is read
		t.Errorf("small: %+v", r)
	}
}

func TestPreloadedExtentsHit(t *testing.T) {
	r := simulate([]Req{rq(0, 1), rq(1, 2)}, big, Config{Cache: 1 << 30, Fetch: 10, Warm: []ext{{1, 100}}})
	if r.Hit != 1 || r.Miss != 1 {
		t.Errorf("%+v", r)
	}
}

func TestBusiestThreadStall(t *testing.T) {
	reqs := []Req{{T: 0, Tid: 1, Ext: []ext{{1, 1}}}, {T: 1, Tid: 1, Ext: []ext{{2, 1}}}, {T: 2, Tid: 2, Ext: []ext{{3, 1}}}}
	r := simulate(reqs, big, Config{Fetch: 10})
	if r.Stall != 30 || r.StallMax != 20 {
		t.Errorf("%+v", r)
	}
}

func TestRequestsFilterThroughThePageCache(t *testing.T) {
	run := &Run{Sizes: map[int32]int64{1: 3*unit + 10}, Base: []Access{
		{T: 1, File: 1, Lo: 0, Hi: 8192},     // pages 0 and 1
		{T: 2, File: 1, Lo: 4096, Hi: 12288}, // page 1 cached, page 2 new
		{T: 3, File: 1, Lo: 100, Hi: 200},    // cached
		{T: 4, File: 1, Lo: 3 * unit, Hi: 3*unit + 10, Kind: kindMap},
	}}
	got := requests(run, 1<<30, 20, true)
	if len(got) != 3 {
		t.Fatalf("%d requests: %+v", len(got), got)
	}
	if got[2].Ext[0].bytes != 10 {
		t.Errorf("last extent keeps its real size: %+v", got[2])
	}
	if all := requests(run, 0, 20, true); len(all) != 4 {
		t.Errorf("without a page cache every read reaches the engine: %d", len(all))
	}
	if reads := requests(run, 1<<30, 20, false); len(reads) != 2 {
		t.Errorf("maps excluded: %d", len(reads))
	}
	// a page cache of one page forgets page 0 before it is read again
	if tiny := requests(run, page, 20, true); len(tiny) != 4 {
		t.Errorf("tiny page cache: %d requests", len(tiny))
	}
}

func TestOrderIsFirstTouch(t *testing.T) {
	got := order([]Req{rq(0, 3, 1), rq(1, 1, 2), rq(2, 3)})
	if len(got) != 3 || got[0].key != 3 || got[1].key != 1 || got[2].key != 2 {
		t.Errorf("%+v", got)
	}
}

func TestEntriesThatFillTheCacheExactlyStay(t *testing.T) {
	l := newlru(100)
	l.put(&node{key: 1, size: 50})
	l.put(&node{key: 2, size: 50})
	if l.get(1) == nil || l.get(2) == nil {
		t.Error("two entries that exactly fill the cache must both stay")
	}
}

func TestPrefetchHoldsABoundedAmountUnread(t *testing.T) {
	const big = 200 << 20
	order := []ext{{1, big}, {2, big}, {3, big}, {4, big}, {5, big}}
	r := simulate([]Req{{T: 1e9, Ext: []ext{{1, big}}}}, func(int32) int64 { return 0 }, Config{Cache: 8 << 30, Fetch: 1, Conc: 8, Order: order, Hold: unread})
	// two entries make 400 MiB unread, over the 256 MiB budget; reading
	// one frees room for one more
	if r.Fetched != 3*big {
		t.Errorf("fetched %d MiB, want %d", r.Fetched>>20, 3*big>>20)
	}
}

func TestReadEntriesAreNotFetchedAgain(t *testing.T) {
	order := []ext{{1, 100}, {2, 100}, {3, 100}}
	reqs := []Req{{T: 0, Ext: []ext{{2, 100}}}, {T: 10, Ext: []ext{{9, 100}}}, {T: 2000, Ext: []ext{{1, 100}}}}
	r := simulate(reqs, big, Config{Cache: 100, Fetch: 1000, Conc: 1, Order: order, Hold: unread})
	// 1 at the start, miss 2, miss 9, 3 once the slot frees, then 1 again
	// as a miss: entry 2 was read already and is not prefetched after eviction
	if r.Fetched != 500 {
		t.Errorf("fetched %d", r.Fetched)
	}
}

func TestPrefetchUsesTheSlotThatFreesFirst(t *testing.T) {
	s := newsim(Config{Cache: 1 << 20, Fetch: 10, Conc: 2}, big)
	s.slots = []int64{50, 0}
	s.issue(0, ext{1, 1})
	if n := s.cache.m[1]; n == nil || n.ready != 10 {
		t.Errorf("%+v", n)
	}
	if s.freeAt() != 10 {
		t.Errorf("free at %d, want 10", s.freeAt())
	}
}

func TestLinkCarriesOneTransferAtATime(t *testing.T) {
	s := newsim(Config{Cache: 1 << 40, Fetch: 10, Conc: 4, Rate: 1e9}, big) // 1 byte per ns
	s.issue(0, ext{1, 100})
	s.issue(0, ext{2, 100})
	s.issue(0, ext{3, 5})
	// each fetch takes the latency of 10 ns at least; the link carries 100 bytes in 100 ns, one after another
	if a, b, c := s.cache.m[1].ready, s.cache.m[2].ready, s.cache.m[3].ready; a != 100 || b != 200 || c != 205 {
		t.Errorf("ready at %d %d %d, want 100 200 205", a, b, c)
	}
}

func TestDemandGoesAheadOfPrefetchAndDelaysIt(t *testing.T) {
	s := newsim(Config{Cache: 1 << 40, Fetch: 10, Conc: 4, Rate: 1e9}, big)
	s.issue(0, ext{1, 100}) // link busy until 100
	if got := s.demand(50, 20); got != 50+20 {
		t.Errorf("demand ready %d, want 70: it does not wait for the queue", got)
	}
	s.issue(0, ext{2, 10}) // the demand's 20 bytes pushed the link to 120
	if got := s.cache.m[2].ready; got != 130 {
		t.Errorf("prefetch ready %d, want 130", got)
	}
}

func TestMergedEntriesShareOneRequest(t *testing.T) {
	order := []ext{{1, 100}, {2, 100}, {3, 100}, {4, 100}}
	// one slot, 50 ns per request: unmerged, entry 4 arrives at 200; merged by 200 bytes, at 100
	one := simulate([]Req{{T: 1e6, Ext: []ext{{4, 100}}}}, big, Config{Cache: 1 << 30, Fetch: 50, Conc: 1, Order: order, Hold: unread})
	two := simulate([]Req{{T: 1e6, Ext: []ext{{4, 100}}}}, big, Config{Cache: 1 << 30, Fetch: 50, Conc: 1, Order: order, Hold: unread, Merge: 200})
	if one.Hit != 1 || two.Hit != 1 {
		t.Fatalf("%+v %+v", one, two)
	}
	s1 := newsim(Config{Cache: 1 << 30, Fetch: 50, Conc: 1, Order: order, Hold: unread}, big)
	s2 := newsim(Config{Cache: 1 << 30, Fetch: 50, Conc: 1, Order: order, Hold: unread, Merge: 200}, big)
	s1.pump(1e6, 0)
	s2.pump(1e6, 0)
	if s1.cache.m[4].ready != 200 || s2.cache.m[4].ready != 100 || s2.cache.m[3].ready != 100 {
		t.Errorf("ready %d, %d and %d", s1.cache.m[4].ready, s2.cache.m[4].ready, s2.cache.m[3].ready)
	}
}

func TestAMissWaitsForItsBytes(t *testing.T) {
	// 100 MB over a 1 GB/s link: 100 ms, far more than the 10 ms latency
	r := simulate([]Req{{T: 0, Ext: []ext{{1, 100e6}}}}, big, Config{Cache: 1 << 40, Fetch: 10e6, Rate: 1e9})
	if r.Stall != 100e6 {
		t.Errorf("stall %d ns, want 100 ms", r.Stall)
	}
}
