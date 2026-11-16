// The tests below were written by an adversarial review of the pool: provenance,
// the model run against a naive reference, the races between Put, Close and the
// timer, the pins of Shared, and what is cleared on return.
package bufpool

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sameArray(a, b []byte) bool { return len(a) > 0 && len(b) > 0 && &a[:1][0] == &b[:1][0] }
func full(b []byte) []byte       { return b[:cap(b)] }
func pat(b []byte, v byte) {
	for i := range b {
		b[i] = v
	}
}

// stamp and stamped touch every 61st byte and the ends: enough to see two owners of
// one array, at a cost the race detector can bear on slices of a megabyte.
func stamp(b []byte, v byte) {
	for i := 0; i < len(b); i += 61 {
		b[i] = v
	}
	if len(b) > 0 {
		b[len(b)-1] = v
	}
}

func stamped(b []byte, v byte) bool {
	for i := 0; i < len(b); i += 61 {
		if b[i] != v {
			return false
		}
	}
	return len(b) == 0 || b[len(b)-1] == v
}

func isPat(b []byte, v byte) bool {
	for _, x := range b {
		if x != v {
			return false
		}
	}
	return true
}

// Provenance: which slices does Put take.
func TestProvenance(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p, q := bud.Pool(false), bud.Pool(false)
	other := NewBudget(1<<30, 0).Pool(false)

	s := p.Get(10000) // class 10240
	if cap(s) != 10240 {
		t.Fatalf("cap %d", cap(s))
	}
	acc := func(label string, v []byte, wantAccepted bool) {
		t.Helper()
		before := p.Stats()
		p.Put(v)
		after := p.Stats()
		got := after.Idle > before.Idle
		if got != wantAccepted {
			t.Fatalf("%s: accepted=%v want %v (%+v -> %+v)", label, got, wantAccepted, before, after)
		}
		if !wantAccepted && cap(v) > 0 && after.Refused != before.Refused+1 {
			t.Fatalf("%s: refused not counted", label)
		}
		if wantAccepted {
			// take it back out so s is out again for the next case
			x := p.Get(10000)
			if !sameArray(x, s) {
				t.Fatalf("%s: re-Get got another array", label)
			}
		}
	}
	acc("interior start", s[1:], false)
	acc("interior start 4095", s[4095:], false)
	acc("three-index smaller cap", s[:10:4096], false)
	acc("three-index cap-1", s[:10:10239], false)
	acc("full-cap three-index", s[:7:10240], true)
	acc("len 0 full cap", s[:0], true)
	acc("zero cap", s[10000:10000], false) // cap 240 start interior
	acc("copy", append([]byte(nil), s...), false)
	acc("append reallocated", append(s[:10240:10240], 1), false)
	acc("other budget", func() []byte { return other.Get(10000) }(), false)
	qs := q.Get(10000)
	acc("other pool same budget", qs, false)
	if q.Stats().Idle != 0 {
		t.Fatal("q affected")
	}
	// stack/array slices
	var arr [10240]byte
	acc("stack array", arr[:], false)
	// finally the genuine put and then every old view must be refused
	views := [][]byte{s, s[:5], s[:0], s[:10240:10240]}
	p.Put(s)
	st := p.Stats()
	for i, v := range views {
		p.Put(v)
		if got := p.Stats(); got.Refused != st.Refused+uint64(i+1) || got.Idle != st.Idle {
			t.Fatalf("view %d of an idle slice was accepted: %+v", i, got)
		}
	}
	// the registry entry of the idle one is intact and Get gives it back once
	x := p.Get(10000)
	y := p.Get(10000)
	if !sameArray(x, s) || sameArray(y, s) {
		t.Fatal("idle slice not handed back exactly once")
	}
}

// Only one of N concurrent Puts of the same slice is taken.
func TestConcurrentDoublePut(t *testing.T) {
	for round := 0; round < 100; round++ {
		bud := NewBudget(1<<30, 0)
		p := bud.Pool(round%2 == 0)
		s := p.Get(100000)
		var wg sync.WaitGroup
		start := make(chan struct{})
		const N = 16
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; p.Put(s) }()
		}
		close(start)
		wg.Wait()
		st := p.Stats()
		if st.Refused != N-1 || st.Idle != int64(cap(s)) || bud.Stats().Idle != int64(cap(s)) {
			t.Fatalf("round %d: %+v", round, st)
		}
		a, b := p.Get(100000), p.Get(100000)
		if sameArray(a, b) {
			t.Fatal("same array twice")
		}
	}
}

// Model-based: compare the pool with a naive reference over a long random run.
type mEnt struct {
	pool, size int
	t          time.Time
}

func TestModel(t *testing.T) {
	seeds := 4
	if testing.Short() {
		seeds = 2
	}
	for seed := 0; seed < seeds; seed++ {
		runModel(t, int64(seed), 2500)
	}
}

type outS struct {
	s    []byte
	pool int
	v    byte
}

func runModel(t *testing.T, seed int64, steps int) {
	r := rand.New(rand.NewSource(seed))
	limit := int64(1+r.Intn(40)) << 18
	decay := time.Duration(r.Intn(3)) * 10 * time.Second // 0 never
	bud, clk := fakeBudget(limit, decay)
	const NP = 8
	pools := make([]*Pool, NP)
	zero := make([]bool, NP)
	for i := range pools {
		zero[i] = r.Intn(2) == 0
		pools[i] = bud.Pool(zero[i])
	}
	type pst struct{ gets, hits, drops, refused uint64 }
	ps := make([]pst, NP)
	var idle []mEnt // oldest first
	var evicted, decayed uint64
	var out []outS
	var back []outS // returned slices, kept to attempt stale puts
	idleBytes := func() (n int64) {
		for _, e := range idle {
			n += int64(e.size)
		}
		return
	}
	check := func(step int, op string) {
		t.Helper()
		if got := bud.Stats(); got.Idle != idleBytes() || got.Evicted != evicted || got.Decayed != decayed {
			t.Fatalf("seed %d step %d %s: budget %+v, model idle=%d evicted=%d decayed=%d", seed, step, op, got, idleBytes(), evicted, decayed)
		}
		for i, p := range pools {
			var want int64
			for _, e := range idle {
				if e.pool == i {
					want += int64(e.size)
				}
			}
			st := p.Stats()
			if st.Idle != want || st.Gets != ps[i].gets || st.Hits != ps[i].hits || st.Drops != ps[i].drops || st.Refused != ps[i].refused {
				t.Fatalf("seed %d step %d %s: pool %d %+v, model idle=%d %+v", seed, step, op, i, st, want, ps[i])
			}
		}
		wantTimers := 0
		if decay > 0 && len(idle) > 0 {
			wantTimers = 1
		}
		if got := clk.armed(); got != wantTimers {
			t.Fatalf("seed %d step %d %s: %d timers armed, want %d (idle %d)", seed, step, op, got, wantTimers, len(idle))
		}
		if got := bud.lru.Len(); got != len(idle) {
			t.Fatalf("lru len %d, model %d", got, len(idle))
		}
		if len(bud.reg) < len(idle) {
			t.Fatalf("registry smaller than idle set")
		}
	}
	evict := func() {
		for idleBytes() > limit {
			e := idle[0]
			idle = idle[1:]
			ps[e.pool].drops++
			evicted++
		}
	}
	sizes := []int{1, 100, 4096, 4097, 5000, 5120, 5121, 6144, 7000, 8192, 8193, 10000, 12288, 16384, 20000, 33000, 65536, 100000, 131073, 300000, 1 << 20, 1<<20 + 1}
	for step := 0; step < steps; step++ {
		op := ""
		switch x := r.Intn(100); {
		case x < 40: // Get
			op = "get"
			pi := r.Intn(NP)
			n := sizes[r.Intn(len(sizes))]
			if r.Intn(4) == 0 {
				n = 1 + r.Intn(600000)
			}
			_, size := classOf(n)
			hit := -1
			for i := len(idle) - 1; i >= 0; i-- {
				if idle[i].pool == pi && idle[i].size == size {
					hit = i
					break
				}
			}
			ps[pi].gets++
			if hit >= 0 {
				idle = append(idle[:hit], idle[hit+1:]...)
				ps[pi].hits++
			}
			s := pools[pi].Get(n)
			if len(s) != n || cap(s) != size {
				t.Fatalf("Get(%d): len %d cap %d, want cap %d", n, len(s), cap(s), size)
			}
			if zero[pi] && !stamped(full(s), 0) {
				t.Fatalf("seed %d step %d: zeroing pool returned dirty memory", seed, step)
			}
			for _, o := range out {
				if sameArray(o.s, s) {
					t.Fatalf("seed %d step %d: Get returned an array that is out", seed, step)
				}
			}
			v := byte(1 + r.Intn(255))
			stamp(full(s), v)
			out = append(out, outS{s, pi, v})
		case x < 70 && len(out) > 0: // Put
			op = "put"
			i := r.Intn(len(out))
			o := out[i]
			out = append(out[:i], out[i+1:]...)
			if !stamped(full(o.s), o.v) {
				t.Fatalf("seed %d step %d: an outstanding slice was disturbed (aliasing)", seed, step)
			}
			// sometimes put it back through a shorter view with full cap
			s := o.s
			if r.Intn(3) == 0 {
				s = s[:r.Intn(len(s)+1)]
			}
			pools[o.pool].Put(s)
			if zero[o.pool] && !stamped(full(o.s), 0) {
				t.Fatalf("seed %d step %d: zeroing pool kept plaintext", seed, step)
			}
			_, size := classOf(cap(o.s))
			idle = append(idle, mEnt{o.pool, size, clk.now})
			evict()
			back = append(back, o)
		case x < 80 && len(back) > 0: // stale or wrong Put
			op = "bad"
			o := back[r.Intn(len(back))]
			pi := r.Intn(NP)
			// is it out again (re-Got)? then Put is legitimate only for its current owner pool; skip those
			skip := false
			for _, q := range out {
				if sameArray(q.s, o.s) {
					skip = true
				}
			}
			if skip {
				break
			}
			var v []byte
			switch r.Intn(4) {
			case 0:
				v = o.s
			case 1:
				v = o.s[1:]
			case 2:
				v = make([]byte, 4096+r.Intn(5000))
			case 3:
				v = o.s[:1:1]
			}
			// a stale whole slice that is idle again is refused (held); a stale whole slice
			// that was evicted is refused (not registered)
			before := pools[pi].Stats()
			pools[pi].Put(v)
			if v != nil && cap(v) > 0 {
				ps[pi].refused++
			}
			if pools[pi].Stats().Idle != before.Idle {
				t.Fatalf("seed %d step %d: a bad Put was accepted", seed, step)
			}
			// but the exception: whole stale slice of an evicted/unknown: counted refused already
		case x < 90: // time
			op = "tick"
			d := time.Duration(1+r.Intn(15)) * time.Second
			clk.advance(d)
			if decay > 0 {
				for len(idle) > 0 && clk.now.Sub(idle[0].t) >= decay {
					ps[idle[0].pool].drops++
					decayed++
					idle = idle[1:]
				}
			}
		default: // put every outstanding of one pool via Buffer-like growth: put-and-get same class
			op = "cycle"
			if len(out) == 0 {
				break
			}
			i := r.Intn(len(out))
			o := out[i]
			out = append(out[:i], out[i+1:]...)
			pools[o.pool].Put(o.s)
			_, size := classOf(cap(o.s))
			idle = append(idle, mEnt{o.pool, size, clk.now})
			evict()
			back = append(back, o)
			// immediately Get the same class: must be a hit unless it was evicted
			ps[o.pool].gets++
			hit := -1
			for i := len(idle) - 1; i >= 0; i-- {
				if idle[i].pool == o.pool && idle[i].size == size {
					hit = i
					break
				}
			}
			if hit >= 0 {
				idle = append(idle[:hit], idle[hit+1:]...)
				ps[o.pool].hits++
			}
			s := pools[o.pool].Get(cap(o.s))
			if len(s) != cap(o.s) {
				t.Fatalf("cycle get len")
			}
			v := byte(1 + r.Intn(255))
			stamp(full(s), v)
			out = append(out, outS{s, o.pool, v})
		}
		if len(back) > 64 {
			back = back[len(back)-64:]
		}
		check(step, op)
	}
	// return everything and let decay run out
	for _, o := range out {
		pools[o.pool].Put(o.s)
	}
	if decay > 0 {
		clk.advance(decay * 3)
		for _, p := range pools {
			_ = p
		}
		if got := bud.Stats(); got.Idle != 0 {
			t.Fatalf("seed %d: idle %d left after decay", seed, got.Idle)
		}
		if clk.armed() != 0 || bud.tm != nil {
			t.Fatalf("seed %d: timer left armed with nothing idle", seed)
		}
	}
	// a Close empties everything
	for _, p := range pools {
		p.Close()
	}
	if bud.Stats().Idle != 0 || bud.lru.Len() != 0 || clk.armed() != 0 {
		t.Fatalf("seed %d: not empty after Close: %+v lru %d timers %d", seed, bud.Stats(), bud.lru.Len(), clk.armed())
	}
}

// Timers: a flood of Puts at one instant arms one timer; a churn with no time passing does not arm more.
func TestTimerStorm(t *testing.T) {
	bud, clk := fakeBudget(1<<30, 10*time.Second)
	p := bud.Pool(false)
	for i := 0; i < 2000; i++ {
		s := p.Get(4096 + i%7000)
		p.Put(s)
		x := p.Get(4096 + i%7000)
		p.Put(x)
	}
	if n := len(clk.timers); n > 3 {
		t.Fatalf("%d timers created for a churn at one instant", n)
	}
	// slow drip: one Put per second for 100 seconds with decay 10s; timers created should be ~ number of expiries, not number of Puts
	bud2, clk2 := fakeBudget(1<<30, 10*time.Second)
	p2 := bud2.Pool(false)
	var live [][]byte
	for i := 0; i < 1000; i++ {
		live = append(live, p2.Get(4096+i*1024))
	}
	for i := range live {
		p2.Put(live[i])
		clk2.advance(time.Second)
	}
	if n := len(clk2.timers); n > 1100 {
		t.Fatalf("timer storm: %d timers for 1000 puts", n)
	}
	t.Logf("timers: churn %d, drip %d", len(clk.timers), len(clk2.timers))
	clk2.advance(time.Minute)
	if bud2.Stats().Idle != 0 || clk2.armed() != 0 {
		t.Fatalf("left over: %+v armed %d", bud2.Stats(), clk2.armed())
	}
}

// A negative limit must not bring the process down.
func TestNegativeLimit(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic with a negative limit: %v", r)
		}
	}()
	p := NewBudget(-1, 0).Pool(false)
	p.Put(p.Get(4096))
}

// A zero limit keeps nothing.
func TestZeroLimit(t *testing.T) {
	bud := NewBudget(0, time.Second)
	p := bud.Pool(true)
	for i := 0; i < 100; i++ {
		p.Put(p.Get(5000 + i))
	}
	if bud.Stats().Idle != 0 || p.Stats().Idle != 0 || bud.tm != nil {
		t.Fatalf("%+v %+v", bud.Stats(), p.Stats())
	}
}

// Heavy race: 128 pools on one budget, real timers, pattern checks; the idle bytes never exceed the limit.
func TestHeavyRace(t *testing.T) {
	const limit = 8 << 20
	bud := NewBudget(limit, 3*time.Millisecond)
	const NP = 128
	pools := make([]*Pool, NP)
	for i := range pools {
		pools[i] = bud.Pool(i%2 == 0)
	}
	var over atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if s := bud.Stats(); s.Idle > limit {
				over.Store(s.Idle)
			}
			var sum int64
			for _, p := range pools {
				sum += p.Stats().Idle
			}
			_ = sum
			runtime.Gosched()
		}
	}()
	dur := time.Second
	deadline := time.Now().Add(dur)
	var bad atomic.Int64
	var gw sync.WaitGroup
	for g := 0; g < 3*runtime.NumCPU(); g++ {
		gw.Add(1)
		go func(g int) {
			defer gw.Done()
			r := rand.New(rand.NewSource(int64(g)))
			var mine [][]byte
			var pi []int
			for time.Now().Before(deadline) {
				switch {
				case len(mine) < 6 && r.Intn(3) > 0:
					k := r.Intn(NP)
					n := 1 + r.Intn(1<<20)
					if r.Intn(10) == 0 {
						n = 1 + r.Intn(5<<20)
					}
					s := pools[k].Get(n)
					if pools[k].zero && !stamped(full(s), 0) {
						bad.Add(1)
					}
					stamp(full(s), byte(g+1))
					mine = append(mine, s)
					pi = append(pi, k)
				case len(mine) > 0:
					i := r.Intn(len(mine))
					if !stamped(full(mine[i]), byte(g+1)) {
						bad.Add(1)
					}
					pools[pi[i]].Put(mine[i])
					mine = append(mine[:i], mine[i+1:]...)
					pi = append(pi[:i], pi[i+1:]...)
				}
			}
			for i := range mine {
				pools[pi[i]].Put(mine[i])
			}
		}(g)
	}
	gw.Wait()
	close(stop)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d aliasing or dirty-zero events", bad.Load())
	}
	if over.Load() != 0 {
		t.Fatalf("idle %d exceeded limit %d", over.Load(), limit)
	}
	var sum int64
	for _, p := range pools {
		sum += p.Stats().Idle
	}
	if got := bud.Stats(); got.Idle != sum || got.Idle > limit {
		t.Fatalf("accounting drift: budget %d, pools %d", got.Idle, sum)
	}
	time.Sleep(50 * time.Millisecond)
	bud.mu.Lock()
	idle, tm, lru := bud.idle, bud.tm, bud.lru.Len()
	bud.mu.Unlock()
	if idle != 0 || tm != nil || lru != 0 {
		t.Fatalf("after decay: idle %d timer %v lru %d", idle, tm, lru)
	}
}

// Shared under churn on a zeroing pool: a pinned reader must never see its bytes cleared or reused.
func TestSharedStress(t *testing.T) {
	bud := NewBudget(64<<20, 0)
	p := bud.Pool(true)
	dur := time.Second
	deadline := time.Now().Add(dur)
	var bad, pins, misses atomic.Int64
	var wg sync.WaitGroup
	cur := make([]atomic.Pointer[Shared], 4)
	// owners
	for o := 0; o < 4; o++ {
		wg.Add(1)
		go func(o int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(o)))
			for time.Now().Before(deadline) {
				s := p.Get(4096 + r.Intn(100000))
				stamp(s, byte(o+1))
				sh := p.Share(s)
				old := cur[o].Swap(sh)
				if old != nil {
					old.Release()
					if r.Intn(5) == 0 {
						old.Release() // idempotent
					}
				}
				runtime.Gosched()
			}
		}(o)
	}
	// readers
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				o := g % 4
				sh := cur[o].Load()
				if sh == nil {
					continue
				}
				b, ok := sh.Pin()
				if !ok {
					misses.Add(1)
					continue
				}
				pins.Add(1)
				for k := 0; k < 3; k++ {
					if !stamped(b, byte(o+1)) {
						bad.Add(1)
					}
					runtime.Gosched()
				}
				if g%2 == 0 {
					sh.Unpin()
				} else {
					go sh.Unpin() // unpin from another goroutine
				}
			}
		}(g)
	}
	// churn: other users of the pool to try to take recycled arrays
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				s := p.Get(4096 + rand.Intn(100000))
				stamp(s, 0xEE)
				p.Put(s)
			}
		}()
	}
	wg.Wait()
	t.Logf("pins %d misses %d", pins.Load(), misses.Load())
	if bad.Load() != 0 {
		t.Fatalf("%d pinned reads saw foreign bytes", bad.Load())
	}
}

// Pin counts: underflow and overflow behaviour.
func TestPinCounts(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p := bud.Pool(true)
	sh := p.Share(p.Get(5000))
	if _, ok := sh.Pin(); !ok {
		t.Fatal("pin")
	}
	sh.Release()
	sh.Release()
	if p.Stats().Idle != 0 {
		t.Fatal("returned while pinned")
	}
	sh.Unpin()
	if p.Stats().Idle == 0 {
		t.Fatal("not returned at last unpin")
	}
	idle := p.Stats().Idle
	// extra unpins after the end: no double return, no resurrection
	for i := 0; i < 5; i++ {
		sh.Unpin()
	}
	if p.Stats().Idle != idle || p.Stats().Refused != 0 {
		t.Fatalf("extra Unpin changed the pool: %+v", p.Stats())
	}
	if _, ok := sh.Pin(); ok {
		t.Fatal("pinned a released slice after extra unpins")
	}
	// After the slice is handed to a new owner, the old Shared must stay dead
	x := p.Get(5000)
	if _, ok := sh.Pin(); ok {
		t.Fatal("old Shared pinned a recycled array")
	}
	sh.Unpin()
	p.Put(x)
	// Release without pins returns at once
	sh2 := p.Share(p.Get(5000))
	sh2.Release()
	if p.Stats().Idle == 0 {
		t.Fatal("release did not return")
	}
}

// Zero on return: every way a slice goes back leaves no byte behind, and a non-zeroing pool's slice cannot be laundered through a zeroing one.
func TestZeroOnReturn(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	zp, np := bud.Pool(true), bud.Pool(false)

	// Put into the wrong pool is refused and nothing is cleared (no laundering, no cross-clear)
	n := np.Get(8192)
	pat(full(n), 9)
	zp.Put(n)
	if !isPat(full(n), 9) {
		t.Fatal("zeroing pool cleared a foreign slice")
	}
	z := zp.Get(8192)
	pat(full(z), 7)
	np.Put(z)
	if !isPat(full(z), 7) {
		t.Fatal("non-zeroing pool touched foreign slice")
	}
	zp.Put(z)
	if !isPat(full(z), 0) {
		t.Fatal("not cleared")
	}

	// Buffer paths
	b := zp.Buffer(100)
	secret := bytes.Repeat([]byte("SECRET"), 5000)
	b.Write(secret[:1000])
	v1 := b.Bytes()
	b.Write(secret) // grows
	if !isPat(full(v1), 0) {
		t.Fatal("grow left plaintext in the old storage")
	}
	v2 := b.Bytes()
	b.Reset()
	if !isPat(v2[:cap(v2)][:len(secret)+1000], 0) {
		t.Fatal("Reset left plaintext")
	}
	b.Write(secret[:300])
	d := b.Detach()
	if !bytes.Equal(d, secret[:300]) {
		t.Fatal("detach content")
	}
	zp.Put(d)
	if !isPat(full(d), 0) {
		t.Fatal("detached then Put left plaintext")
	}
	b.Write(secret[:10])
	w := b.Bytes()
	b.Release()
	b.Release()
	if !isPat(full(w), 0) {
		t.Fatal("Release left plaintext")
	}

	// eviction/decay do not matter; Close: idle slices cleared on Put already
	// Shared: final drop clears
	sh := zp.Share(zp.Get(9000))
	bb, _ := sh.Pin()
	pat(bb, 5)
	sh.Release()
	if !isPat(bb, 5) {
		t.Fatal("cleared while pinned")
	}
	sh.Unpin()
	if !isPat(full(bb), 0) {
		t.Fatal("last unpin left plaintext")
	}

	// A full-cap slice that was written beyond its length is cleared whole.
	s := zp.Get(5000)
	pat(full(s), 3)
	zp.Put(s[:1])
	if !isPat(full(s), 0) {
		t.Fatal("bytes past len survived")
	}
}

// Buffer.Write of a view of its own storage across a growth.
func TestBufferSelfWrite(t *testing.T) {
	for _, zero := range []bool{true, false} {
		p := NewBudget(1<<30, 0).Pool(zero)
		b := p.Buffer(4096)
		src := bytes.Repeat([]byte{0xA5}, 4096)
		b.Write(src)
		b.Write(b.Bytes()[:200]) // appends 200 bytes copied from itself; Len 4096 -> must grow
		got := b.Bytes()
		if len(got) != 4296 || !bytes.Equal(got[4096:], src[:200]) {
			t.Fatalf("zero=%v: Write of its own bytes across a growth gave %x...", zero, got[4096:4100])
		}
	}
}

// Put racing Close / Get on the same pool: the registry and the lists stay consistent.
func TestPutCloseRace(t *testing.T) {
	for round := 0; round < 60; round++ {
		bud := NewBudget(1<<30, time.Millisecond)
		p := bud.Pool(true)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					s := p.Get(5000 + g*100 + i)
					p.Put(s)
					if g == 0 && i == 25 {
						p.Close()
					}
				}
			}(g)
		}
		wg.Wait()
		p.Close()
		bud.mu.Lock()
		idle, lru, tm := bud.idle, bud.lru.Len(), bud.tm
		pidle := p.idle
		bud.mu.Unlock()
		if idle != 0 || lru != 0 || tm != nil || pidle != 0 {
			t.Fatalf("round %d: idle %d lru %d tm %v pool idle %d", round, idle, lru, tm, pidle)
		}
	}
}

// A registry flood of odd sizes that are never put back does not grow without bound once collected.
func TestRegistryFlood(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p := bud.Pool(false)
	for i := 0; i < 8000; i++ {
		_ = p.Get(4097 + (i*7919)%200000) // dropped, never put
		if i%2000 == 0 {
			runtime.GC()
		}
	}
	runtime.GC()
	for i := 0; i < 200; i++ { // a few misses to trigger sweeps
		_ = p.Get(4097 + (i*104729)%(1<<20))
		runtime.GC()
	}
	bud.mu.Lock()
	n := len(bud.reg)
	bud.mu.Unlock()
	t.Logf("registry %d after flood", n)
	if n > 6000 {
		t.Fatalf("registry kept %d records of collected slices", n)
	}
}

func BenchmarkGetPutContended(b *testing.B) {
	for _, np := range []int{1, 8, 128} {
		b.Run(fmt.Sprint("pools", np), func(b *testing.B) {
			bud := NewBudget(256<<20, time.Minute)
			pools := make([]*Pool, np)
			for i := range pools {
				pools[i] = bud.Pool(i%2 == 0)
			}
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					p := pools[i%np]
					i++
					s := p.Get(64 << 10)
					s[0] = 1
					p.Put(s)
				}
			})
		})
	}
}

// A Put that is clearing its slice when the pool is closed must not leave the slice idle afterwards.
func TestPutThenClose(t *testing.T) {
	for round := 0; round < 3; round++ {
		bud := NewBudget(1<<30, 0)
		p := bud.Pool(true)
		s := p.Get(MaxSize) // 64 MiB: the clear takes milliseconds
		pat(s[:1<<20], 7)
		done := make(chan struct{})
		go func() { p.Put(s); close(done) }()
		for { // wait until the Put has claimed the slice and is clearing it
			bud.mu.Lock()
			e := bud.reg[start(s)]
			held := e != nil && e.held
			bud.mu.Unlock()
			if held {
				break
			}
			runtime.Gosched()
		}
		p.Close()
		<-done
		if st := p.Stats(); st.Idle != 0 || bud.Stats().Idle != 0 || bud.lru.Len() != 0 {
			t.Fatalf("round %d: a closed pool kept a slice: %+v %+v", round, st, bud.Stats())
		}
	}
}

// Eviction that empties the idle set must stop the timer (it is armed only while something is idle).
func TestEvictAllDisarms(t *testing.T) {
	bud, clk := fakeBudget(100<<10, 10*time.Second)
	p := bud.Pool(false)
	p.Put(p.Get(8 << 10))
	if clk.armed() != 1 {
		t.Fatalf("armed %d, want 1", clk.armed())
	}
	p.Put(p.Get(1 << 20)) // bigger than the limit: everything idle goes, itself included
	if st := bud.Stats(); st.Idle != 0 {
		t.Fatalf("%+v", st)
	}
	if clk.armed() != 0 || bud.tm != nil {
		t.Fatalf("timer armed with nothing idle: %d", clk.armed())
	}
}

// After Close the pool neither keeps what it is given nor hands out registered slices.
func TestUseAfterClose(t *testing.T) {
	bud := NewBudget(1<<30, 0)
	p := bud.Pool(true)
	p.Put(p.Get(5000))
	p.Close()
	s := p.Get(5000)
	p.Put(s)
	if st := p.Stats(); st.Idle != 0 || st.Refused != 1 {
		t.Fatalf("%+v", st)
	}
	if bud.Stats().Idle != 0 {
		t.Fatal("closed pool idle")
	}
}
