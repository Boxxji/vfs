package main

// A cache simulator over the engine's view of a run. The engine does
// not see every read: the guest's page cache answers a page it already
// holds. requests() applies that filter, then expresses what is left as
// requests for fixed-size extents, the unit a chunk store fetches.

// ext is one extent of one file: key is file<<32 | index.
type ext struct {
	key   uint64
	bytes int64
}

// Req is one read that reached the engine: the extents it needs.
type Req struct {
	T   int64
	Tid int32
	Ext []ext
}

type node struct {
	key        uint64
	size       int64
	ready      int64 // when the bytes arrive
	prefetched bool
	used       bool
	prev, next *node
}

// lru is a least-recently-used set bounded by the sum of sizes.
type lru struct {
	cap, used  int64
	m          map[uint64]*node
	head, tail *node // head is the most recent
	evicted    func(*node)
}

func newlru(cap int64) *lru { return &lru{cap: cap, m: map[uint64]*node{}} }

func (l *lru) unlink(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

func (l *lru) front(n *node) {
	n.next = l.head
	if l.head != nil {
		l.head.prev = n
	}
	l.head = n
	if l.tail == nil {
		l.tail = n
	}
}

// get returns the entry and marks it most recent.
func (l *lru) get(k uint64) *node {
	n := l.m[k]
	if n != nil && l.head != n {
		l.unlink(n)
		l.front(n)
	}
	return n
}

// put inserts n unless it can never fit.
func (l *lru) put(n *node) {
	if n.size > l.cap {
		return
	}
	if old := l.m[n.key]; old != nil {
		l.unlink(old)
		l.used -= old.size
	}
	l.m[n.key] = n
	l.front(n)
	l.used += n.size
	for l.used > l.cap && l.tail != nil && l.tail != n {
		v := l.tail
		l.unlink(v)
		delete(l.m, v.key)
		l.used -= v.size
		if l.evicted != nil {
			l.evicted(v)
		}
	}
}

// requests reduces a run to the reads that reach the engine. pages is
// the guest page cache in bytes (0 for none) and shift the log2 extent
// size. With maps false, mapped and executed ranges are left out.
func requests(r *Run, pages int64, shift uint, maps bool) []Req {
	pc := newlru(pages / page)
	var out []Req
	size := int64(1) << shift
	for _, a := range r.Base {
		if a.Kind == kindMap && !maps {
			continue
		}
		var q Req
		last := int64(-1)
		for p := a.Lo / page; p <= (a.Hi-1)/page; p++ {
			if pc.cap > 0 {
				k := uint64(a.File)<<40 | uint64(p)
				if pc.get(k) != nil {
					continue
				}
				pc.put(&node{key: k, size: 1})
			}
			idx := p * page >> shift
			if idx == last {
				continue
			}
			last = idx
			n := size
			if fs := r.Sizes[a.File]; fs > 0 && idx<<shift+n > fs {
				n = max(fs-idx<<shift, 1)
			}
			q.Ext = append(q.Ext, ext{uint64(a.File)<<32 | uint64(idx), n})
		}
		if len(q.Ext) > 0 {
			q.T, q.Tid = a.T, a.Tid
			out = append(out, q)
		}
	}
	return out
}

// Config is one cache policy.
type Config struct {
	Cache int64 // node cache bytes; 0 means no cache
	Fetch int64 // ns to fetch one cold extent
	Conc  int   // fetches the prefetcher keeps in flight
	Ahead bool  // sequential read-ahead
	Order []ext // ordered extents the previous run touched, for trace prefetch
	Warm  []ext // extents present before the run starts
	Shift uint
}

const (
	aheadMax   = 8         // read-ahead window ceiling, in extents
	traceWin   = 1024      // trace prefetch looks this many entries ahead
	traceBytes = 256 << 20 // and holds at most this much unused
	prefetchNS = int64(0)  // prefetch may begin when the run does
)

// Result is what a policy did over one run.
type Result struct {
	Reqs     int64 // reads that reached the engine
	Lookups  int64 // extent lookups
	Hit      int64 // extent present and arrived
	Late     int64 // extent still in flight: the read waited for it
	Miss     int64 // extent fetched on demand
	Stalled  int64 // reads that waited at all
	Fetched  int64 // bytes fetched, demand and prefetch
	Wasted   int64 // bytes prefetched and never read
	Stall    int64 // ns waited, all threads
	StallMax int64 // ns waited by the busiest thread
}

type readahead struct {
	last        int64
	run, window int
}

type sim struct {
	cfg    Config
	size   func(file int32) int64
	cache  *lru
	slots  []int64
	ahead  map[int32]*readahead
	res    Result
	unused int64 // bytes prefetched and not yet read

	order  []ext
	pos    map[uint64]int
	done   []bool
	cursor int
	next   int
}

func newsim(cfg Config, size func(int32) int64) *sim {
	s := &sim{cfg: cfg, size: size, cache: newlru(cfg.Cache), ahead: map[int32]*readahead{}}
	s.slots = make([]int64, max(cfg.Conc, 1))
	s.cache.evicted = func(n *node) {
		if n.prefetched && !n.used {
			s.res.Wasted += n.size
			s.unused -= n.size
		}
	}
	for _, e := range cfg.Warm {
		s.cache.put(&node{key: e.key, size: e.bytes})
	}
	if len(cfg.Order) > 0 {
		s.order = cfg.Order
		s.pos = make(map[uint64]int, len(cfg.Order))
		for i, e := range cfg.Order {
			s.pos[e.key] = i
		}
		s.done = make([]bool, len(cfg.Order))
	}
	return s
}

// issue starts a prefetch of e on the slot that frees first, no earlier
// than from. An extent already cached or in flight is left alone.
func (s *sim) issue(e ext, from int64) {
	if s.cache.cap == 0 || s.cache.m[e.key] != nil {
		return
	}
	i := 0
	for j := range s.slots {
		if s.slots[j] < s.slots[i] {
			i = j
		}
	}
	start := max(s.slots[i], from)
	s.slots[i] = start + s.cfg.Fetch
	s.cache.put(&node{key: e.key, size: e.bytes, ready: s.slots[i], prefetched: true})
	s.res.Fetched += e.bytes
	s.unused += e.bytes
}

// freeAt is when the first prefetch slot is free.
func (s *sim) freeAt() int64 {
	m := s.slots[0]
	for _, v := range s.slots[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

// pump issues trace prefetches that fit in the window, as slots free,
// up to time now. from is the earliest time the window had room.
func (s *sim) pump(now, from int64) {
	for s.order != nil && s.next < len(s.order) && s.next < s.cursor+traceWin && s.unused < traceBytes {
		start := max(s.freeAt(), from)
		if start > now {
			return
		}
		i := s.next
		s.next++
		if !s.done[i] {
			s.issue(s.order[i], from)
		}
	}
}

// consume records that the run read entry p of the prefetch order.
func (s *sim) consume(p int) {
	if p >= s.cursor+traceWin {
		s.cursor = p // the run jumped past the window: resynchronise
		s.next = max(s.next, p)
	}
	s.done[p] = true
	for s.cursor < len(s.done) && s.done[s.cursor] {
		s.cursor++
	}
	s.next = max(s.next, s.cursor)
}

func (s *sim) run(reqs []Req) Result {
	perThread := map[int32]int64{}
	s.pump(0, 0)
	for _, q := range reqs {
		s.pump(q.T, 0)
		s.res.Reqs++
		var wait int64
		for _, e := range q.Ext {
			s.res.Lookups++
			n := s.cache.get(e.key)
			switch {
			case s.cache.cap == 0 || n == nil:
				s.res.Miss++
				s.res.Fetched += e.bytes
				wait = max(wait, s.cfg.Fetch)
				s.cache.put(&node{key: e.key, size: e.bytes, ready: q.T + s.cfg.Fetch, used: true})
			case n.ready <= q.T:
				s.res.Hit++
			default:
				// A queued prefetch is promoted to a demand fetch if that is sooner.
				s.res.Late++
				w := min(n.ready-q.T, s.cfg.Fetch)
				n.ready = q.T + w
				wait = max(wait, w)
			}
			if n != nil && n.prefetched && !n.used {
				s.unused -= n.size
			}
			if n != nil {
				n.used = true
			}
			if s.pos != nil {
				if p, ok := s.pos[e.key]; ok {
					s.consume(p)
				}
			}
			if s.cfg.Ahead {
				s.readahead(e, q.T)
			}
		}
		if wait > 0 {
			s.res.Stalled++
			s.res.Stall += wait
			perThread[q.Tid] += wait
		}
		s.pump(q.T, q.T)
	}
	for _, v := range perThread {
		s.res.StallMax = max(s.res.StallMax, v)
	}
	for n := s.cache.head; n != nil; n = n.next {
		if n.prefetched && !n.used {
			s.res.Wasted += n.size
		}
	}
	return s.res
}

// readahead prefetches the extents after e once the file has been read
// in two consecutive extents, doubling the window up to aheadMax.
func (s *sim) readahead(e ext, now int64) {
	file, idx := int32(e.key>>32), int64(uint32(e.key))
	st := s.ahead[file]
	if st == nil {
		st = &readahead{last: -2}
		s.ahead[file] = st
	}
	switch {
	case idx == st.last+1:
		st.run++
	case idx != st.last:
		st.run, st.window = 0, 0
	}
	st.last = idx
	if st.run < 1 {
		return
	}
	st.window = min(max(st.window*2, 2), aheadMax)
	fs := s.size(file)
	unit := int64(1) << s.cfg.Shift
	for k := int64(1); k <= int64(st.window); k++ {
		start := (idx + k) * unit
		if fs > 0 && start >= fs {
			return
		}
		n := unit
		if fs > 0 && start+n > fs {
			n = fs - start
		}
		s.issue(ext{uint64(file)<<32 | uint64(idx+k), n}, now)
	}
}

// order lists the distinct extents of a request stream in first-touch
// order.
func order(reqs []Req) []ext {
	seen := map[uint64]bool{}
	var out []ext
	for _, q := range reqs {
		for _, e := range q.Ext {
			if !seen[e.key] {
				seen[e.key] = true
				out = append(out, e)
			}
		}
	}
	return out
}

// simulate runs one policy over a stream on an empty cache.
func simulate(reqs []Req, size func(int32) int64, cfg Config) Result {
	return newsim(cfg, size).run(reqs)
}

// follow runs prior and then reqs on one cache, as two runs back to back
// on the same node.
func follow(prior, reqs []Req, size func(int32) int64, cfg Config) Result {
	s := newsim(cfg, size)
	s.run(prior)
	s.res = Result{}
	s.slots = make([]int64, len(s.slots))
	for n := s.cache.head; n != nil; n = n.next {
		n.ready, n.used, n.prefetched = 0, false, false
	}
	s.ahead = map[int32]*readahead{}
	s.unused = 0
	return s.run(reqs)
}
