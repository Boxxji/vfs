// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// fakeCAS is an in-process model of a keyed object store's atomic conditional
// write (S3 If-Match / GCS generation-match): one independent (data, generation)
// slot per key. A single mutex makes the compare-and-set in PutIfVersion
// indivisible from every caller's point of view — exactly like the real store's
// server-side atomicity, NOT a client-side "read then write" pair with a gap.
//
// beforeCAS, when set, runs OUTSIDE the lock right before PutIfVersion acquires
// it, so a test can force two goroutines to finish their Get before either write
// proceeds — turning a timing-dependent race into a deterministic, repeatable one.
type fakeCAS struct {
	mu        sync.Mutex
	objects   map[string]*fakeObj
	beforeCAS func(key string)
}

type fakeObj struct {
	data []byte
	ver  int // generation counter; the version token is its decimal string.
}

func newFakeCAS() *fakeCAS { return &fakeCAS{objects: map[string]*fakeObj{}} }

func (s *fakeCAS) Get(_ context.Context, key string) ([]byte, backend.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key]
	if !ok {
		return nil, "", backend.ErrNotFound
	}
	return append([]byte(nil), obj.data...), backend.Version(strconv.Itoa(obj.ver)), nil
}

func (s *fakeCAS) PutIfAbsent(_ context.Context, key string, data []byte) (backend.Version, error) {
	return s.cas(key, data, "", true)
}

func (s *fakeCAS) PutIfVersion(_ context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	return s.cas(key, data, v, false)
}

func (s *fakeCAS) cas(key string, data []byte, expect backend.Version, absent bool) (backend.Version, error) {
	if s.beforeCAS != nil {
		s.beforeCAS(key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key]
	switch {
	case absent && ok, !absent && !ok:
		return "", fmt.Errorf("fakeCAS: %w", backend.ErrConflict)
	case !absent && backend.Version(strconv.Itoa(obj.ver)) != expect:
		return "", fmt.Errorf("fakeCAS: %w: have %d want %q", backend.ErrConflict, obj.ver, expect)
	}
	if !ok {
		obj = &fakeObj{}
		s.objects[key] = obj
	}
	obj.ver++
	obj.data = append([]byte(nil), data...)
	return backend.Version(strconv.Itoa(obj.ver)), nil
}

// barrierAt2 blocks the first two callers until both arrive, then releases both
// at once; later calls (retries) pass straight through.
func barrierAt2() func(string) {
	var mu sync.Mutex
	arrived := 0
	release := make(chan struct{})
	return func(string) {
		mu.Lock()
		arrived++
		n := arrived
		mu.Unlock()
		if n == 2 {
			close(release)
		}
		<-release
	}
}

func mustPut(t *testing.T, f *Store, key, payload string, round uint64) {
	t.Helper()
	if err := f.Put(context.Background(), key, []byte(payload), round); err != nil {
		t.Fatalf("Put(%s, round=%d): unexpected error: %v", key, round, err)
	}
}

func read(t *testing.T, f *Store, key string) (uint64, string) {
	t.Helper()
	payload, round, err := f.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	return round, string(payload)
}

// TestFencedPutRejectsLowerRound is the deposed-writer proof: once a successor
// advanced the recorded round, the predecessor's ship at its (now-lower) round is
// refused with ErrStaleRound and the object is left untouched.
func TestFencedPutRejectsLowerRound(t *testing.T) {
	f := New(newFakeCAS())
	mustPut(t, f, "orgs/acme/kv.db", "v-round6", 6) // new owner sealed at round 6.

	err := f.Put(context.Background(), "orgs/acme/kv.db", []byte("v-stale"), 3)
	if !errors.Is(err, ErrStaleRound) {
		t.Fatalf("lower-round ship: got %v, want ErrStaleRound", err)
	}
	if round, payload := read(t, f, "orgs/acme/kv.db"); round != 6 || payload != "v-round6" {
		t.Fatalf("object mutated by a fenced write: round=%d payload=%q, want 6/v-round6", round, payload)
	}
}

// TestFencedPutAdmitsSameRoundReship proves the `>=` rule: the CURRENT owner
// re-ships the same object at its stable lease round many times, and every such
// overwrite is admitted (the last one wins) — this is why the fence uses `>=`,
// not the one-shot `>`.
func TestFencedPutAdmitsSameRoundReship(t *testing.T) {
	f := New(newFakeCAS())
	mustPut(t, f, "orgs/acme/kv.db", "tick-1", 4)
	mustPut(t, f, "orgs/acme/kv.db", "tick-2", 4)
	mustPut(t, f, "orgs/acme/kv.db", "tick-3", 4)
	if round, payload := read(t, f, "orgs/acme/kv.db"); round != 4 || payload != "tick-3" {
		t.Fatalf("same-round re-ship: round=%d payload=%q, want 4/tick-3", round, payload)
	}
}

// TestFencedPutHigherRoundLocksOutLower is the handoff proof: a successor at a
// strictly higher round is admitted (sealing the object), after which the
// predecessor at the old round is fenced forever.
func TestFencedPutHigherRoundLocksOutLower(t *testing.T) {
	f := New(newFakeCAS())
	mustPut(t, f, "orgs/acme/kv.db", "old-owner", 5) // predecessor at round 5.
	mustPut(t, f, "orgs/acme/kv.db", "new-owner", 6) // successor claims round 6.

	if err := f.Put(context.Background(), "orgs/acme/kv.db", []byte("old-owner-late"), 5); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("predecessor after handoff: got %v, want ErrStaleRound", err)
	}
	if round, payload := read(t, f, "orgs/acme/kv.db"); round != 6 || payload != "new-owner" {
		t.Fatalf("after handoff: round=%d payload=%q, want 6/new-owner", round, payload)
	}
}

// TestFencedPutConcurrentSameRoundOwnerReship: the current owner ships the same
// object concurrently from two goroutines at its lease round. Both must succeed
// (idempotent overwrite) and the object must decode cleanly to ONE of the two
// payloads — never a byte-mix.
func TestFencedPutConcurrentSameRoundOwnerReship(t *testing.T) {
	store := newFakeCAS()
	store.beforeCAS = barrierAt2()
	f := New(store)

	results := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := range 2 {
		go func() {
			defer wg.Done()
			results[i] = f.Put(context.Background(), "orgs/acme/kv.db", []byte(fmt.Sprintf("payload-%d", i)), 7)
		}()
	}
	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Fatalf("same-round concurrent ship %d failed: %v (owner re-ship must be idempotent)", i, err)
		}
	}
	round, payload := read(t, f, "orgs/acme/kv.db")
	if round != 7 || (payload != "payload-0" && payload != "payload-1") {
		t.Fatalf("object corrupted by concurrent same-round ship: round=%d payload=%q", round, payload)
	}
}

// TestFencedPutConcurrentHigherVsLower is the partition proof, made deterministic:
// a minority writer (stuck at round 5 because it cannot reach the linearizable
// source to advance) and the majority's new owner (round 6) race their ships, both
// having read the same prior state. The SAFETY invariant, whichever wins the first
// CAS: the higher round ALWAYS wins the final durable state (the successor's round
// 6 either lands directly, or retries past the minority's transient round-5 write),
// and round 5 can never be the surviving state. The minority's Put may return nil
// (its write landed transiently before being overwritten) — that is not unsafe
// here because the minority never ACKs on a bare ship; acknowledgement is gated on
// the full lease+idempotency protocol (see cloud's handoff test). What must never
// happen is round 5 surviving.
func TestFencedPutConcurrentHigherVsLower(t *testing.T) {
	store := newFakeCAS()
	f := New(store)
	mustPut(t, f, "orgs/acme/kv.db", "base", 5) // both racers read this as the prior state.

	store.beforeCAS = barrierAt2()
	results := map[uint64]error{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, round := range []uint64{5, 6} { // 5 = fenced minority, 6 = majority successor.
		wg.Go(func() {
			err := f.Put(context.Background(), "orgs/acme/kv.db", []byte(fmt.Sprintf("round-%d", round)), round)
			mu.Lock()
			results[round] = err
			mu.Unlock()
		})
	}
	wg.Wait()

	if results[6] != nil {
		t.Fatalf("majority successor (round 6) must always win, got %v", results[6])
	}
	if round, payload := read(t, f, "orgs/acme/kv.db"); round != 6 || payload != "round-6" {
		t.Fatalf("partition resolved wrong: final state round=%d payload=%q, want 6/round-6 (higher round must survive)", round, payload)
	}
}

// TestCarryForwardPreservesLastWriteThenFences is the safe-handoff proof at the
// store layer: a predecessor's last landed ship (X) is carried forward by the
// successor's takeover seal at the higher round — never lost — and every
// subsequent predecessor ship is then fenced.
func TestCarryForwardPreservesLastWriteThenFences(t *testing.T) {
	f := New(newFakeCAS())
	key := "orgs/acme/kv.db"
	mustPut(t, f, key, "base", 5)
	mustPut(t, f, key, "base+X", 5) // predecessor's last acknowledged ship at round 5.

	// Successor takes over at lease round 6, carrying forward the LATEST payload and
	// hydrating it (proving it sees X).
	var hydrated string
	if err := f.CarryForward(context.Background(), key, 6, func(p []byte, _ uint64) error { hydrated = string(p); return nil }); err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if hydrated != "base+X" {
		t.Fatalf("successor hydrated %q, want base+X (predecessor's last write must not be lost)", hydrated)
	}
	if round, payload := read(t, f, key); round != 6 || payload != "base+X" {
		t.Fatalf("after takeover: round=%d payload=%q, want 6/base+X", round, payload)
	}
	// Predecessor, still running at round 5, is now fenced.
	if err := f.Put(context.Background(), key, []byte("base+X+Y"), 5); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("predecessor after takeover: got %v, want ErrStaleRound", err)
	}
}

// TestCarryForwardVsConcurrentPredecessorShipNoLoss is the no-acknowledged-write-
// lost proof under a genuine race (lease violation: both writers active). A
// predecessor ships X at round 5 while the successor takes over at round 6, both
// having read the same prior version. The store's single-object CAS totally orders
// them, so the biconditional MUST hold: the successor's round-6 state contains X
// IFF the predecessor's ship succeeded (nil). There is no interleaving where the
// predecessor's ship returns nil (would be acknowledged) yet X is lost.
func TestCarryForwardVsConcurrentPredecessorShipNoLoss(t *testing.T) {
	store := newFakeCAS()
	f := New(store)
	key := "orgs/acme/kv.db"
	mustPut(t, f, key, "base", 5) // both read this version.

	store.beforeCAS = barrierAt2()
	var predErr, succErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); predErr = f.Put(context.Background(), key, []byte("base+X"), 5) }()
	go func() {
		defer wg.Done()
		succErr = f.CarryForward(context.Background(), key, 6, nil)
	}()
	wg.Wait()

	if succErr != nil {
		t.Fatalf("successor takeover must succeed, got %v", succErr)
	}
	round, payload := read(t, f, key)
	if round != 6 {
		t.Fatalf("final round=%d, want 6", round)
	}
	containsX := payload == "base+X"
	predAcked := predErr == nil
	if containsX != predAcked {
		t.Fatalf("acknowledged-write-lost: predecessor acked=%v but final-contains-X=%v (payload=%q) — must be equal",
			predAcked, containsX, payload)
	}
}

// TestFencedGetRoundTripAndAbsent covers the read path: payload+round round-trip,
// absent key is ErrNotFound, and Round reports 0 for an absent key (the value a
// first owner increments to 1 to claim).
func TestFencedGetRoundTripAndAbsent(t *testing.T) {
	f := New(newFakeCAS())

	if _, _, err := f.Get(context.Background(), "orgs/absent/kv.db"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("absent Get: got %v, want ErrNotFound", err)
	}
	if r, err := f.Round(context.Background(), "orgs/absent/kv.db"); err != nil || r != 0 {
		t.Fatalf("absent Round: got (%d,%v), want (0,nil)", r, err)
	}

	mustPut(t, f, "orgs/acme/kv.db", "hello", 9)
	payload, round, err := f.Get(context.Background(), "orgs/acme/kv.db")
	if err != nil || string(payload) != "hello" || round != 9 {
		t.Fatalf("round-trip: payload=%q round=%d err=%v, want hello/9/nil", payload, round, err)
	}
}

// TestFencedPutCreateOnlyFirstWrite proves the absent slot is a create-only put
// (expectVersion ""): two racers both seeing the empty slot cannot both create it.
func TestFencedPutCreateOnlyFirstWrite(t *testing.T) {
	store := newFakeCAS()
	store.beforeCAS = barrierAt2()
	f := New(store)

	results := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := range 2 {
		go func() {
			defer wg.Done()
			results[i] = f.Put(context.Background(), "orgs/new/kv.db", []byte(fmt.Sprintf("first-%d", i)), 1)
		}()
	}
	wg.Wait()

	// Same round (1) for both: one creates, the loser retries the CAS and — since
	// 1 >= recorded 1 — is admitted as an idempotent overwrite. Both succeed; the
	// object is one clean payload at round 1. (A create-only slot cannot be
	// double-created; the retry path is what makes the loser converge.)
	for i, err := range results {
		if err != nil {
			t.Fatalf("create-race %d: %v", i, err)
		}
	}
	if round, payload := read(t, f, "orgs/new/kv.db"); round != 1 || (payload != "first-0" && payload != "first-1") {
		t.Fatalf("create race left object round=%d payload=%q", round, payload)
	}
}

// TestFencedRejectsCorruptForeignObject proves fail-closed decoding: a non-fence
// object sitting at the key is not misread as some absurd round — Get and Put both
// surface a corruption error rather than guess.
func TestFencedRejectsCorruptForeignObject(t *testing.T) {
	store := newFakeCAS()
	// A foreign writer put raw bytes (no fence header) at the key.
	if _, err := store.PutIfAbsent(context.Background(), "orgs/acme/kv.db", []byte("not-a-fence-record")); err != nil {
		t.Fatalf("seed foreign object: %v", err)
	}
	f := New(store)

	if _, _, err := f.Get(context.Background(), "orgs/acme/kv.db"); !errors.Is(err, errCorrupt) {
		t.Fatalf("Get of foreign object: got %v, want errCorrupt", err)
	}
	if err := f.Put(context.Background(), "orgs/acme/kv.db", []byte("x"), 1); !errors.Is(err, errCorrupt) {
		t.Fatalf("Put over foreign object: got %v, want errCorrupt", err)
	}
}

var _ Backend = (*fakeCAS)(nil)

// TestCreateIsCreateOnly proves Create births an object at round 0 exactly once:
// a second Create, and a Create racing an existing round-0 object, are refused,
// where Put would admit round 0 over round 0.
func TestCreateIsCreateOnly(t *testing.T) {
	f := New(newFakeCAS())
	ctx := context.Background()
	if err := f.Create(ctx, "head", []byte("first")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := f.Create(ctx, "head", []byte("second")); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("second Create: got %v, want ErrConflict", err)
	}
	if round, payload := read(t, f, "head"); round != 0 || payload != "first" {
		t.Fatalf("Create overwrote: round=%d payload=%q, want 0/first", round, payload)
	}
}

// TestCarryForwardHydrateSeesRecordedRound proves hydrate receives the round
// recorded with the payload, and that a hydrate error refuses the takeover and
// leaves the object untouched.
func TestCarryForwardHydrateSeesRecordedRound(t *testing.T) {
	f := New(newFakeCAS())
	ctx := context.Background()
	mustPut(t, f, "k", "p", 4)
	refuse := errors.New("refused")
	err := f.CarryForward(ctx, "k", 5, func(_ []byte, recorded uint64) error {
		if recorded != 4 {
			t.Errorf("hydrate saw recorded=%d, want 4", recorded)
		}
		return refuse
	})
	if !errors.Is(err, refuse) {
		t.Fatalf("CarryForward: got %v, want the hydrate error", err)
	}
	if round, payload := read(t, f, "k"); round != 4 || payload != "p" {
		t.Fatalf("refused takeover mutated the object: round=%d payload=%q", round, payload)
	}
}

// TestCarryForwardRetriesWhenAWriteLandsFirst is the lost race made certain, not
// left to a scheduler: a predecessor's write lands after the successor has read and
// before its compare-and-set. The successor must retry, hydrate the newer payload and
// seal that.
func TestCarryForwardRetriesWhenAWriteLandsFirst(t *testing.T) {
	store := newFakeCAS()
	f := New(store)
	ctx := context.Background()
	key := "orgs/acme/kv.db"
	mustPut(t, f, key, "base", 5)

	var once sync.Once
	store.beforeCAS = func(string) {
		once.Do(func() {
			store.beforeCAS = nil
			mustPut(t, f, key, "base+X", 5) // the predecessor's last write, between read and seal
		})
	}
	var hydrated []string
	if err := f.CarryForward(ctx, key, 6, func(p []byte, _ uint64) error { hydrated = append(hydrated, string(p)); return nil }); err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if len(hydrated) != 2 || hydrated[0] != "base" || hydrated[1] != "base+X" {
		t.Fatalf("hydrated %v, want [base base+X]: the takeover did not re-read after losing the race", hydrated)
	}
	if round, payload := read(t, f, key); round != 6 || payload != "base+X" {
		t.Fatalf("after takeover: round=%d payload=%q, want 6/base+X", round, payload)
	}
}

// TestPutRetriesWhenAWriteLandsFirst: the owner's own write loses a race to another
// same-round write; it re-reads and writes again rather than failing or clobbering.
func TestPutRetriesWhenAWriteLandsFirst(t *testing.T) {
	store := newFakeCAS()
	f := New(store)
	key := "orgs/acme/kv.db"
	mustPut(t, f, key, "base", 4)
	var once sync.Once
	store.beforeCAS = func(string) {
		once.Do(func() {
			store.beforeCAS = nil
			mustPut(t, f, key, "interloper", 4)
		})
	}
	if err := f.Put(context.Background(), key, []byte("ours"), 4); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if round, payload := read(t, f, key); round != 4 || payload != "ours" {
		t.Fatalf("round=%d payload=%q, want 4/ours", round, payload)
	}
}
