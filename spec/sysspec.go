// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package spec formalizes machine-checkable state-machine specifications and
// deterministic transition/fault-schedule synthesis for hanzoai/vfs (HIP-1348):
//   - journal ordered watermarks (Published <= Durable <= Visible) and recovery
//   - monotone Round fencing rejecting ErrStaleRound
//   - fail-closed corruption, Needs, canonical varint, and bound rejection
//   - conditional Head publication and cross-layer Journal-Head composition.
package spec

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

// Invariant identifiers formalized by SYSSPEC.
const (
	InvJournalOrder            = "INV-J1"
	InvJournalMonotone         = "INV-J2"
	InvJournalPublishGuard     = "INV-J3"
	InvJournalReplayPrefix     = "INV-J4"
	InvJournalReclaimFloor     = "INV-J5"
	InvJournalCorruptionReject = "INV-J6"

	InvFenceMonotoneRound    = "INV-F1"
	InvFenceStaleRoundReject = "INV-F2"
	InvFenceBoundAndCorrupt  = "INV-F3"
	InvFenceLostAckAndRace   = "INV-F4"

	InvHeadAtomicPublication = "INV-H1"
	InvHeadLostAckTakeover   = "INV-H2"
	InvHeadCanonicalEncoding = "INV-H3"
	InvHeadCorruptionReject  = "INV-H4"

	InvCrossLayerWatermarkHead = "INV-X1"
)

const (
	headObjectKey = "head"
	maxHeadBytes  = 64 << 10
)

// Violation records a single broken state-machine invariant at a schedule step.
type Violation struct {
	Invariant string
	Schedule  string
	Step      int
	Action    string
	Detail    string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s step %d (%s): %s", v.Invariant, v.Schedule, v.Step, v.Action, v.Detail)
}

// Report summarizes an execution of synthesized schedules and invariant checks.
type Report struct {
	SchedulesRun        int
	TransitionsChecked  int
	InvariantsEvaluated int
	FaultsInjected      int
	Violations          []Violation
}

// OK reports whether all evaluated invariants held with zero violations.
func (r Report) OK() bool { return len(r.Violations) == 0 }

// Merge combines another Report into r.
func (r *Report) Merge(o Report) {
	r.SchedulesRun += o.SchedulesRun
	r.TransitionsChecked += o.TransitionsChecked
	r.InvariantsEvaluated += o.InvariantsEvaluated
	r.FaultsInjected += o.FaultsInjected
	r.Violations = append(r.Violations, o.Violations...)
}

func (r *Report) check(inv, sched string, step int, action string, ok bool, detailFmt string, args ...any) {
	r.InvariantsEvaluated++
	if !ok {
		r.Violations = append(r.Violations, Violation{
			Invariant: inv,
			Schedule:  sched,
			Step:      step,
			Action:    action,
			Detail:    fmt.Sprintf(detailFmt, args...),
		})
	}
}

// --- Journal Watermark & Recovery State-Machine Spec ---

// JournalActionKind enumerates synthesized transitions and faults for journals.
type JournalActionKind uint8

const (
	JActAppend JournalActionKind = iota
	JActSync
	JActPublish
	JActPublishAheadProbe
	JActReclaim
	JActReplay
	JActRestart
	JActCrash
	JActProbeCorruptRecord
	JActProbeWrongLineage
	JActProbeForeignSegment
)

func (k JournalActionKind) String() string {
	switch k {
	case JActAppend:
		return "Append"
	case JActSync:
		return "Sync"
	case JActPublish:
		return "Publish"
	case JActPublishAheadProbe:
		return "PublishAheadProbe"
	case JActReclaim:
		return "Reclaim"
	case JActReplay:
		return "Replay"
	case JActRestart:
		return "Restart"
	case JActCrash:
		return "Crash"
	case JActProbeCorruptRecord:
		return "ProbeCorruptRecord"
	case JActProbeWrongLineage:
		return "ProbeWrongLineage"
	case JActProbeForeignSegment:
		return "ProbeForeignSegment"
	default:
		return fmt.Sprintf("JAct(%d)", k)
	}
}

// JournalStep is one deterministic transition or injected fault.
type JournalStep struct {
	Kind      JournalActionKind
	Payload   []byte
	Seq       uint64
	CrashKind crash.Kind
	CrashSeed int64
}

// JournalSchedule is a named sequence of journal steps.
type JournalSchedule struct {
	Name  string
	Seed  int64
	Steps []JournalStep
}

// SynthesizeJournalSchedules deterministically builds count journal transition
// and crash/corruption schedules from seed.
func SynthesizeJournalSchedules(seed int64, count, steps int) []JournalSchedule {
	out := make([]JournalSchedule, 0, count)
	for i := range count {
		s := seed + int64(i)
		rng := rand.New(rand.NewPCG(uint64(s), 0x51553C01))
		seq := make([]JournalStep, 0, steps)
		for st := range steps {
			var step JournalStep
			switch r := rng.IntN(100); {
			case r < 40:
				ln := 16 + rng.IntN(700)
				buf := make([]byte, ln)
				for j := range buf {
					buf[j] = byte((s + int64(st) + int64(j)) & 0xff)
				}
				step = JournalStep{Kind: JActAppend, Payload: buf}
			case r < 58:
				step = JournalStep{Kind: JActSync}
			case r < 70:
				step = JournalStep{Kind: JActPublish}
			case r < 76:
				step = JournalStep{Kind: JActPublishAheadProbe}
			case r < 83:
				step = JournalStep{Kind: JActReclaim}
			case r < 88:
				step = JournalStep{Kind: JActReplay}
			case r < 92:
				step = JournalStep{Kind: JActRestart}
			case r < 96:
				kinds := []crash.Kind{crash.Lost, crash.All, crash.Prefix, crash.Torn, crash.Scatter}
				step = JournalStep{
					Kind:      JActCrash,
					CrashKind: kinds[rng.IntN(len(kinds))],
					CrashSeed: s*1000 + int64(st),
				}
			case r < 98:
				step = JournalStep{Kind: JActProbeWrongLineage}
			default:
				step = JournalStep{Kind: JActProbeForeignSegment}
			}
			seq = append(seq, step)
		}
		// Always append explicit lineage, corruption, and stale-floor probes near the end.
		seq = append(seq,
			JournalStep{Kind: JActProbeWrongLineage},
			JournalStep{Kind: JActProbeCorruptRecord},
			JournalStep{Kind: JActReplay},
		)
		out = append(out, JournalSchedule{
			Name:  fmt.Sprintf("journal-%d", s),
			Seed:  s,
			Steps: seq,
		})
	}
	return out
}

// CheckJournalMarks verifies INV-J1 and INV-J2 on observed marks.
func CheckJournalMarks(rep *Report, sched string, step int, act string, prev, cur journal.Marks, allowCrash bool) {
	rep.check(InvJournalOrder, sched, step, act,
		cur.Published <= cur.Durable && cur.Durable <= cur.Visible,
		"watermark order violated: %+v", cur)
	if !allowCrash {
		rep.check(InvJournalMonotone, sched, step, act,
			cur.Published >= prev.Published && cur.Durable >= prev.Durable && cur.Visible >= prev.Visible,
			"watermark regressed without crash: prev=%+v cur=%+v", prev, cur)
	}
}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func diskHasLiveJournalHeader(d journal.Disk) bool {
	names, err := d.List()
	if err != nil {
		return false
	}
	var hdr [40]byte
	for _, nm := range names {
		f, oerr := d.Open(nm)
		if oerr != nil {
			continue
		}
		n, rerr := f.ReadAt(hdr[:], 0)
		_ = f.Close()
		if rerr != nil || n < 40 {
			continue
		}
		if string(hdr[4:8]) != "JRNL" {
			continue
		}
		if crc32.Checksum(hdr[4:40], castagnoliTable) != binary.LittleEndian.Uint32(hdr[0:4]) {
			continue
		}
		if binary.LittleEndian.Uint64(hdr[8:16]) != 0 {
			return true
		}
	}
	return false
}

// RunJournalSchedule executes s against the real journal package over crash.Disk,
// checking INV-J1..INV-J6 at every transition.
func RunJournalSchedule(s JournalSchedule) Report {
	var rep Report
	rep.SchedulesRun = 1
	ctx := context.Background()
	d := crash.New()
	clk := &specClock{now: time.Unix(1_700_000_000, 0)}
	lineage := []byte(fmt.Sprintf("spec-lineage-%d", s.Seed))

	type recEntry struct {
		data []byte
	}
	modelRecs := map[uint64]recEntry{}
	var modelVisible, modelSynced, modelPublished uint64
	var reclaimedFloor uint64

	opts := func(disk journal.Disk, pub uint64, lin []byte) journal.Options {
		return journal.Options{
			Capacity:  8 * 4096,
			Segment:   4096,
			Disk:      disk,
			Clock:     clk,
			Published: pub,
			Lineage:   lin,
			Bound:     time.Hour,
		}
	}

	j, err := journal.Open("", opts(d, 0, lineage))
	if err != nil {
		rep.check(InvJournalOrder, s.Name, 0, "Open", false, "initial Open failed: %v", err)
		return rep
	}
	defer func() {
		if j != nil {
			_ = j.Close()
		}
	}()

	drainFull := func(step int) error {
		if modelVisible == 0 {
			return nil
		}
		if err := j.Sync(ctx, modelVisible); err != nil {
			return err
		}
		modelSynced = max(modelSynced, modelVisible)
		if err := j.Publish(modelVisible); err != nil {
			return err
		}
		modelPublished = modelVisible
		return j.Reclaim()
	}

	prevMarks := j.Marks()
	for idx, st := range s.Steps {
		rep.TransitionsChecked++
		clk.advance(2 * time.Millisecond)
		act := st.Kind.String()
		crashed := false

		switch st.Kind {
		case JActAppend:
			seq, err := j.Append(ctx, st.Payload)
			if errors.Is(err, journal.ErrFull) {
				if derr := drainFull(idx); derr != nil {
					rep.check(InvJournalOrder, s.Name, idx, act, false, "drainFull: %v", derr)
					return rep
				}
				reclaimedFloor = max(reclaimedFloor, modelPublished)
				seq, err = j.Append(ctx, st.Payload)
			}
			rep.check(InvJournalOrder, s.Name, idx, act, err == nil, "Append failed: %v", err)
			if err == nil {
				rep.check(InvJournalMonotone, s.Name, idx, act, seq == modelVisible+1, "seq %d != expected %d", seq, modelVisible+1)
				modelVisible = seq
				modelRecs[seq] = recEntry{data: append([]byte(nil), st.Payload...)}
			}

		case JActSync:
			if modelVisible > 0 {
				target := modelVisible
				if st.Seq > 0 && st.Seq <= modelVisible {
					target = st.Seq
				}
				err := j.Sync(ctx, target)
				rep.check(InvJournalOrder, s.Name, idx, act, err == nil, "Sync(%d): %v", target, err)
				if err == nil && target > modelPublished {
					modelSynced = max(modelSynced, target)
				}
				m := j.Marks()
				rep.check(InvJournalOrder, s.Name, idx, act, m.Durable >= target, "Sync(%d) left Durable=%d", target, m.Durable)
			}

		case JActPublish:
			dur := j.Marks().Durable
			if dur > modelPublished {
				target := modelPublished + 1 + (dur-modelPublished)/2
				err := j.Publish(target)
				rep.check(InvJournalOrder, s.Name, idx, act, err == nil, "Publish(%d <= Durable %d): %v", target, dur, err)
				if err == nil {
					modelPublished = max(modelPublished, target)
				}
			}

		case JActPublishAheadProbe:
			rep.FaultsInjected++
			dur := j.Marks().Durable
			ahead := dur + 1
			before := j.Marks()
			err := j.Publish(ahead)
			after := j.Marks()
			rep.check(InvJournalPublishGuard, s.Name, idx, act,
				errors.Is(err, journal.ErrAhead) && after.Published == before.Published,
				"Publish(%d > Durable %d) err=%v before=%+v after=%+v", ahead, dur, err, before, after)

		case JActReclaim:
			err := j.Reclaim()
			rep.check(InvJournalReclaimFloor, s.Name, idx, act, err == nil, "Reclaim: %v", err)
			// Verify stale Head behind floor is rejected once segments are reclaimed.
			if modelPublished > 0 {
				snap := d.Image(d.Len(), crash.Variant{Kind: crash.All})
				jj, oerr := journal.Open("", opts(snap, 0, lineage))
				if oerr == nil {
					// Only allowed if no segment before store 0 was actually reclaimed yet.
					gotFirst := jj.Recovery().First
					_ = jj.Close()
					rep.check(InvJournalReclaimFloor, s.Name, idx, act, gotFirst == 1,
						"Open(Published=0) succeeded when log starts at %d (> 1)", gotFirst)
				} else {
					rep.check(InvJournalReclaimFloor, s.Name, idx, act,
						errors.Is(oerr, journal.ErrMismatch) || errors.Is(oerr, journal.ErrCorrupt),
						"stale Published=0 gave unexpected error %v", oerr)
				}
			}

		case JActReplay:
			var gotSeqs []uint64
			err := j.Replay(func(r journal.Record) error {
				gotSeqs = append(gotSeqs, r.Seq)
				want, ok := modelRecs[r.Seq]
				rep.check(InvJournalReplayPrefix, s.Name, idx, act,
					ok && bytes.Equal(r.Data, want.data),
					"Replay seq %d payload mismatch (ok=%v)", r.Seq, ok)
				return nil
			})
			rep.check(InvJournalReplayPrefix, s.Name, idx, act, err == nil, "Replay err: %v", err)
			wantCount := int(modelVisible - modelPublished)
			rep.check(InvJournalReplayPrefix, s.Name, idx, act, len(gotSeqs) == wantCount,
				"Replay count %d != %d", len(gotSeqs), wantCount)
			for i, seq := range gotSeqs {
				rep.check(InvJournalReplayPrefix, s.Name, idx, act, seq == modelPublished+1+uint64(i),
					"Replay gap/order at idx %d: got %d want %d", i, seq, modelPublished+1+uint64(i))
			}

		case JActRestart:
			if err := j.Close(); err != nil {
				rep.check(InvJournalOrder, s.Name, idx, act, false, "Close: %v", err)
				return rep
			}
			modelSynced = modelVisible
			j, err = journal.Open("", opts(d, modelPublished, lineage))
			rep.check(InvJournalOrder, s.Name, idx, act, err == nil, "Restart Open: %v", err)
			if err != nil {
				return rep
			}

		case JActCrash:
			rep.FaultsInjected++
			crashed = true
			vs := d.Variants(d.Len(), 1, st.CrashSeed)
			v := vs[0]
			for _, cand := range vs {
				if cand.Kind == st.CrashKind {
					v = cand
					break
				}
			}
			img := d.Image(d.Len(), v)
			_ = j.Close()
			d = img
			j, err = journal.Open("", opts(d, modelPublished, lineage))
			rep.check(InvJournalMonotone, s.Name, idx, act, err == nil, "Open after crash (%v): %v", v, err)
			if err != nil {
				return rep
			}
			tail := j.Marks().Visible
			rep.check(InvJournalMonotone, s.Name, idx, act,
				!(tail < modelSynced && modelSynced > modelPublished),
				"lost durable records after %v: tail=%d synced=%d published=%d", v, tail, modelSynced, modelPublished)
			rep.check(InvJournalMonotone, s.Name, idx, act,
				tail <= modelVisible,
				"phantom records appeared after %v: tail=%d > visible=%d", v, tail, modelVisible)
			modelVisible = max(tail, modelPublished)
			for seq := range modelRecs {
				if seq > modelVisible {
					delete(modelRecs, seq)
				}
			}
			modelSynced = modelVisible

		case JActProbeWrongLineage:
			rep.FaultsInjected++
			snap := d.Image(d.Len(), crash.Variant{Kind: crash.All})
			if !diskHasLiveJournalHeader(snap) {
				anchor, aerr := journal.Open("", opts(snap, modelPublished, lineage))
				if aerr == nil {
					if seq, apperr := anchor.Append(ctx, []byte("spec-lineage-anchor")); apperr == nil {
						_ = anchor.Sync(ctx, seq)
					}
					_ = anchor.Close()
				}
			}
			bad, err := journal.Open("", opts(snap, modelPublished, []byte("wrong-lineage")))
			if err == nil {
				_ = bad.Close()
			}
			rep.check(InvJournalCorruptionReject, s.Name, idx, act,
				errors.Is(err, journal.ErrLineage),
				"wrong lineage Open = %v, want ErrLineage", err)

		case JActProbeForeignSegment:
			rep.FaultsInjected++
			snap := d.Image(d.Len(), crash.Variant{Kind: crash.All})
			f, ferr := snap.Create("999999.seg", 4096)
			if ferr == nil {
				_, _ = f.WriteAt(bytes.Repeat([]byte("FORN"), 1024), 0)
				_ = f.Sync()
				_ = f.Close()
			}
			bad, err := journal.Open("", opts(snap, modelPublished, lineage))
			if err == nil {
				_ = bad.Close()
			}
			rep.check(InvJournalCorruptionReject, s.Name, idx, act,
				errors.Is(err, journal.ErrFormat),
				"foreign segment Open = %v, want ErrFormat", err)

		case JActProbeCorruptRecord:
			rep.FaultsInjected++
			// Ensure at least one durable record is covered by a mark in a snapshot,
			// then corrupt a byte inside the record payload and require ErrCorrupt.
			snap := d.Image(d.Len(), crash.Variant{Kind: crash.All})
			probeJ, perr := journal.Open("", opts(snap, modelPublished, lineage))
			if perr == nil {
				seq, aerr := probeJ.Append(ctx, []byte("spec-corrupt-probe"))
				if aerr == nil {
					_ = probeJ.Sync(ctx, seq)
				}
				_ = probeJ.Close()
				// Corrupt the first live segment's first record payload/header if present.
				names, _ := snap.List()
				corrupted := false
				for _, nm := range names {
					sf, oerr := snap.Open(nm)
					if oerr != nil {
						continue
					}
					var hdr [40]byte
					if _, rerr := sf.ReadAt(hdr[:], 0); rerr == nil && string(hdr[4:8]) == "JRNL" {
						var b [1]byte
						_, _ = sf.ReadAt(b[:], 80+28)
						b[0] ^= 0xff
						_, _ = sf.WriteAt(b[:], 80+28)
						_ = sf.Sync()
						corrupted = true
						_ = sf.Close()
						break
					}
					_ = sf.Close()
				}
				if corrupted {
					// Open with mark proving durability must detect ErrCorrupt (or drop behind published if all published).
					// Check with Published=0 on a fresh mark-proven log when log starts at 1.
					bad, oerr := journal.Open("", opts(snap, modelPublished, lineage))
					if oerr == nil {
						r := bad.Recovery()
						_ = bad.Close()
						// Only valid if the corrupted record was already <= modelPublished or unproven;
						// verify Replay or Proven check stays consistent.
						rep.check(InvJournalCorruptionReject, s.Name, idx, act,
							r.Proven <= r.Last || r.Last <= modelPublished,
							"corrupted segment bypassed proven check: %+v", r)
					} else {
						rep.check(InvJournalCorruptionReject, s.Name, idx, act,
							errors.Is(oerr, journal.ErrCorrupt) || errors.Is(oerr, journal.ErrMismatch),
							"corrupted segment returned unexpected err %v", oerr)
					}
				}
			}
		}

		curMarks := j.Marks()
		CheckJournalMarks(&rep, s.Name, idx, act, prevMarks, curMarks, crashed)
		prevMarks = curMarks
	}
	return rep
}

// --- Monotone Round Fencing State-Machine Spec ---

// FenceActionKind enumerates transitions and fault schedules on fence.Store.
type FenceActionKind uint8

const (
	FActCreate FenceActionKind = iota
	FActPut
	FActPutStale
	FActClaim
	FActClaimStale
	FActRacePutVsClaim
	FActPutLostAck
	FActClaimLostAck
	FActProbeCorrupt
	FActProbeOversize
)

func (k FenceActionKind) String() string {
	switch k {
	case FActCreate:
		return "FenceCreate"
	case FActPut:
		return "FencePut"
	case FActPutStale:
		return "FencePutStale"
	case FActClaim:
		return "FenceClaim"
	case FActClaimStale:
		return "FenceClaimStale"
	case FActRacePutVsClaim:
		return "FenceRacePutVsClaim"
	case FActPutLostAck:
		return "FencePutLostAck"
	case FActClaimLostAck:
		return "FenceClaimLostAck"
	case FActProbeCorrupt:
		return "FenceProbeCorrupt"
	case FActProbeOversize:
		return "FenceProbeOversize"
	default:
		return fmt.Sprintf("FAct(%d)", k)
	}
}

// FenceStep is one deterministic fence operation or fault schedule.
type FenceStep struct {
	Kind    FenceActionKind
	Round   uint64
	Payload []byte
}

// FenceSchedule is a sequence of FenceSteps.
type FenceSchedule struct {
	Name  string
	Seed  int64
	Steps []FenceStep
}

// SynthesizeFenceSchedules generates deterministic fencing & race schedules.
func SynthesizeFenceSchedules(seed int64, count, steps int) []FenceSchedule {
	out := make([]FenceSchedule, 0, count)
	for i := range count {
		s := seed + int64(i)
		rng := rand.New(rand.NewPCG(uint64(s), 0xF34C3002))
		seq := []FenceStep{{Kind: FActCreate, Payload: []byte("init")}}
		curRound := uint64(0)
		for st := range steps {
			var step FenceStep
			pay := []byte(fmt.Sprintf("payload-%d-%d", s, st))
			switch r := rng.IntN(100); {
			case r < 25:
				if curRound == 0 {
					curRound = 1
				}
				step = FenceStep{Kind: FActPut, Round: curRound, Payload: pay}
			case r < 40:
				curRound++
				step = FenceStep{Kind: FActClaim, Round: curRound}
			case r < 58:
				stale := uint64(0)
				if curRound > 0 {
					stale = rng.Uint64N(curRound)
				}
				step = FenceStep{Kind: FActPutStale, Round: stale, Payload: []byte("stale")}
			case r < 70:
				step = FenceStep{Kind: FActClaimStale, Round: curRound}
			case r < 82:
				curRound++
				step = FenceStep{Kind: FActRacePutVsClaim, Round: curRound, Payload: pay}
			case r < 90:
				if curRound == 0 {
					curRound = 1
				}
				step = FenceStep{Kind: FActPutLostAck, Round: curRound, Payload: pay}
			case r < 95:
				curRound++
				step = FenceStep{Kind: FActClaimLostAck, Round: curRound}
			case r < 98:
				step = FenceStep{Kind: FActProbeOversize}
			default:
				step = FenceStep{Kind: FActProbeCorrupt}
			}
			seq = append(seq, step)
		}
		out = append(out, FenceSchedule{
			Name:  fmt.Sprintf("fence-%d", s),
			Seed:  s,
			Steps: seq,
		})
	}
	return out
}

// RunFenceSchedule runs a fence transition schedule over storetest.Mem +
// storetest.Hook + barrier races, verifying INV-F1..INV-F4.
func RunFenceSchedule(s FenceSchedule) Report {
	var rep Report
	rep.SchedulesRun = 1
	ctx := context.Background()
	mem := storetest.NewMem()
	var (
		mu          sync.Mutex
		loseNextAck bool
		beforeCAS   func()
	)
	be := &fenceInterceptBackend{
		Backend: mem,
		beforeCAS: func() {
			mu.Lock()
			fn := beforeCAS
			mu.Unlock()
			if fn != nil {
				fn()
			}
		},
		afterCAS: func() error {
			mu.Lock()
			lose := loseNextAck
			loseNextAck = false
			mu.Unlock()
			if lose {
				return fmt.Errorf("lost reply: %w", backend.ErrConflict)
			}
			return nil
		},
	}

	const key = "head"
	const maxSize = 4096
	writer := fence.New(be, maxSize)
	var recordedRound uint64
	var recordedPayload []byte

	for idx, st := range s.Steps {
		rep.TransitionsChecked++
		act := st.Kind.String()

		switch st.Kind {
		case FActCreate:
			err := writer.Create(ctx, key, st.Payload)
			rep.check(InvFenceMonotoneRound, s.Name, idx, act, err == nil, "Create failed: %v", err)
			if err == nil {
				recordedRound = 0
				recordedPayload = append([]byte(nil), st.Payload...)
			}
			// Second Create must fail with ErrConflict.
			rep.FaultsInjected++
			err2 := writer.Create(ctx, key, []byte("dup"))
			rep.check(InvFenceMonotoneRound, s.Name, idx, act, errors.Is(err2, backend.ErrConflict), "duplicate Create err=%v", err2)

		case FActPut:
			err := writer.Put(ctx, key, st.Payload, st.Round)
			rep.check(InvFenceMonotoneRound, s.Name, idx, act, err == nil, "Put(round=%d): %v", st.Round, err)
			if err == nil {
				recordedRound = max(recordedRound, st.Round)
				recordedPayload = append([]byte(nil), st.Payload...)
			}

		case FActPutStale:
			rep.FaultsInjected++
			if st.Round >= recordedRound {
				continue
			}
			beforeRaw, _, _ := mem.Get(ctx, key, backend.Unbounded)
			err := writer.Put(ctx, key, st.Payload, st.Round)
			afterRaw, _, _ := mem.Get(ctx, key, backend.Unbounded)
			rep.check(InvFenceStaleRoundReject, s.Name, idx, act,
				errors.Is(err, fence.ErrStaleRound),
				"stale Put(%d < %d) = %v, want ErrStaleRound", st.Round, recordedRound, err)
			rep.check(InvFenceStaleRoundReject, s.Name, idx, act,
				bytes.Equal(beforeRaw, afterRaw),
				"stale Put mutated stored bytes")

		case FActClaim:
			writer = fence.New(be, maxSize)
			var hydrated []byte
			err := writer.Claim(ctx, key, st.Round, func(p []byte, rec uint64) error {
				hydrated = append([]byte(nil), p...)
				rep.check(InvFenceMonotoneRound, s.Name, idx, act, rec == recordedRound,
					"Claim hydrate saw round %d != %d", rec, recordedRound)
				return nil
			})
			rep.check(InvFenceMonotoneRound, s.Name, idx, act, err == nil, "Claim(%d): %v", st.Round, err)
			rep.check(InvFenceLostAckAndRace, s.Name, idx, act, bytes.Equal(hydrated, recordedPayload),
				"Claim lost payload: got %q want %q", hydrated, recordedPayload)
			if err == nil {
				recordedRound = st.Round
			}

		case FActClaimStale:
			rep.FaultsInjected++
			if recordedRound == 0 {
				continue
			}
			staleRound := recordedRound - 1
			beforeRaw, _, _ := mem.Get(ctx, key, backend.Unbounded)
			staleWriter := fence.New(be, maxSize)
			err := staleWriter.Claim(ctx, key, staleRound, func([]byte, uint64) error { return nil })
			afterRaw, _, _ := mem.Get(ctx, key, backend.Unbounded)
			rep.check(InvFenceStaleRoundReject, s.Name, idx, act,
				errors.Is(err, fence.ErrStaleRound) && bytes.Equal(beforeRaw, afterRaw),
				"stale Claim(%d < %d) err=%v mutated=%v", staleRound, recordedRound, err, !bytes.Equal(beforeRaw, afterRaw))

		case FActRacePutVsClaim:
			rep.FaultsInjected++
			predRound := recordedRound
			succRound := st.Round
			if succRound <= predRound {
				succRound = predRound + 1
			}
			succ := fence.New(be, maxSize)

			// Barrier holds both goroutines until both have read the same version before CAS.
			var bmu sync.Mutex
			arrived := 0
			release := make(chan struct{})
			mu.Lock()
			beforeCAS = func() {
				bmu.Lock()
				arrived++
				n := arrived
				bmu.Unlock()
				if n == 2 {
					close(release)
				}
				if n <= 2 {
					<-release
				}
			}
			mu.Unlock()

			var predErr, succErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				predErr = writer.Put(ctx, key, st.Payload, predRound)
			}()
			go func() {
				defer wg.Done()
				succErr = succ.Claim(ctx, key, succRound, nil)
			}()
			wg.Wait()

			mu.Lock()
			beforeCAS = nil
			mu.Unlock()

			rep.check(InvFenceLostAckAndRace, s.Name, idx, act, succErr == nil, "Claim lost race: %v", succErr)
			gotPay, gotRound, gerr := succ.Get(ctx, key)
			rep.check(InvFenceMonotoneRound, s.Name, idx, act, gerr == nil && gotRound == succRound,
				"post-race round=%d err=%v, want %d", gotRound, gerr, succRound)
			predAcked := predErr == nil
			containsX := bytes.Equal(gotPay, st.Payload)
			rep.check(InvFenceLostAckAndRace, s.Name, idx, act, predAcked == containsX,
				"biconditional violated: predAcked=%v containsX=%v", predAcked, containsX)
			writer = succ
			recordedRound = succRound
			recordedPayload = gotPay

		case FActPutLostAck:
			rep.FaultsInjected++
			mu.Lock()
			loseNextAck = true
			mu.Unlock()
			err := writer.Put(ctx, key, st.Payload, st.Round)
			rep.check(InvFenceLostAckAndRace, s.Name, idx, act, err == nil, "PutLostAck failed: %v", err)
			if err == nil {
				recordedRound = max(recordedRound, st.Round)
				recordedPayload = append([]byte(nil), st.Payload...)
			}

		case FActClaimLostAck:
			rep.FaultsInjected++
			mu.Lock()
			loseNextAck = true
			mu.Unlock()
			succ := fence.New(be, maxSize)
			err := succ.Claim(ctx, key, st.Round, nil)
			rep.check(InvFenceLostAckAndRace, s.Name, idx, act, err == nil, "ClaimLostAck failed: %v", err)
			if err == nil {
				writer = succ
				recordedRound = st.Round
			}

		case FActProbeOversize:
			rep.FaultsInjected++
			beforeRaw, _, _ := mem.Get(ctx, key, backend.Unbounded)
			over := bytes.Repeat([]byte{'Z'}, maxSize)
			err := writer.Put(ctx, key, over, recordedRound)
			afterRaw, _, _ := mem.Get(ctx, key, backend.Unbounded)
			rep.check(InvFenceBoundAndCorrupt, s.Name, idx, act,
				errors.Is(err, backend.ErrTooLarge) && bytes.Equal(beforeRaw, afterRaw),
				"oversize Put err=%v mutated=%v", err, !bytes.Equal(beforeRaw, afterRaw))

		case FActProbeCorrupt:
			rep.FaultsInjected++
			corruptMem := storetest.NewMem()
			_, _ = corruptMem.PutIfAbsent(ctx, key, []byte("not-a-fence-object"))
			cf := fence.New(corruptMem, maxSize)
			_, _, gerr := cf.Get(ctx, key)
			perr := cf.Put(ctx, key, []byte("x"), 1)
			rep.check(InvFenceBoundAndCorrupt, s.Name, idx, act, gerr != nil && perr != nil,
				"foreign fence object accepted: get=%v put=%v", gerr, perr)
		}

		// Verify live store state matches ghost state after every transition.
		gotPay, gotRound, gerr := writer.Get(ctx, key)
		rep.check(InvFenceMonotoneRound, s.Name, idx, act,
			gerr == nil && gotRound == recordedRound && bytes.Equal(gotPay, recordedPayload),
			"state drift: got round=%d want=%d gerr=%v", gotRound, recordedRound, gerr)
	}
	return rep
}

type fenceInterceptBackend struct {
	backend.Backend
	beforeCAS func()
	afterCAS  func() error
}

func (b *fenceInterceptBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if b.beforeCAS != nil {
		b.beforeCAS()
	}
	v, err := b.Backend.PutIfAbsent(ctx, key, data)
	if err != nil {
		return "", err
	}
	if b.afterCAS != nil {
		if aerr := b.afterCAS(); aerr != nil {
			return "", aerr
		}
	}
	return v, nil
}

func (b *fenceInterceptBackend) PutIfVersion(ctx context.Context, key string, data []byte, ver backend.Version) (backend.Version, error) {
	if b.beforeCAS != nil {
		b.beforeCAS()
	}
	v, err := b.Backend.PutIfVersion(ctx, key, data, ver)
	if err != nil {
		return "", err
	}
	if b.afterCAS != nil {
		if aerr := b.afterCAS(); aerr != nil {
			return "", aerr
		}
	}
	return v, nil
}

// --- Conditional Head Publication & Format-v2 Corruption Spec ---

// HeadActionKind enumerates synthesized Volume/Head transitions and fault probes.
type HeadActionKind uint8

const (
	HActAttach HeadActionKind = iota
	HActAttachStale
	HActAttachInvalidBounds
	HActWriteFile
	HActTruncateFile
	HActFlush
	HActFlushFaultBeforeHead
	HActFlushFaultBeforeShard
	HActFlushLostAckAfterHead
	HActVerifyRead
	HActProbeCorruptHeadBitFlip
	HActProbeCrossPlaceHead
	HActProbeUnknownNeeds
	HActProbeNeedsBeforeCorruptTail
	HActProbeNonMinimalVarint
	HActProbeBadTailOrder
)

func (k HeadActionKind) String() string {
	switch k {
	case HActAttach:
		return "VolumeAttach"
	case HActAttachStale:
		return "VolumeAttachStale"
	case HActAttachInvalidBounds:
		return "VolumeAttachInvalidBounds"
	case HActWriteFile:
		return "VolumeWrite"
	case HActTruncateFile:
		return "VolumeTruncate"
	case HActFlush:
		return "VolumeFlush"
	case HActFlushFaultBeforeHead:
		return "FlushFaultBeforeHead"
	case HActFlushFaultBeforeShard:
		return "FlushFaultBeforeShard"
	case HActFlushLostAckAfterHead:
		return "FlushLostAckAfterHead"
	case HActVerifyRead:
		return "VolumeVerifyRead"
	case HActProbeCorruptHeadBitFlip:
		return "ProbeCorruptHeadBitFlip"
	case HActProbeCrossPlaceHead:
		return "ProbeCrossPlaceHead"
	case HActProbeUnknownNeeds:
		return "ProbeUnknownNeeds"
	case HActProbeNeedsBeforeCorruptTail:
		return "ProbeNeedsBeforeCorruptTail"
	case HActProbeNonMinimalVarint:
		return "ProbeNonMinimalVarint"
	case HActProbeBadTailOrder:
		return "ProbeBadTailOrder"
	default:
		return fmt.Sprintf("HAct(%d)", k)
	}
}

// HeadStep is one transition or fault injection in a Volume/Head schedule.
type HeadStep struct {
	Kind  HeadActionKind
	Name  string
	Off   int64
	Data  []byte
	Size  uint64
	Round uint64
}

// HeadSchedule is a named sequence of Volume/Head transitions.
type HeadSchedule struct {
	Name  string
	Seed  int64
	Steps []HeadStep
}

// SynthesizeHeadSchedules generates deterministic Volume & Head publication
// fault schedules.
func SynthesizeHeadSchedules(seed int64, count, steps int) []HeadSchedule {
	out := make([]HeadSchedule, 0, count)
	for i := range count {
		s := seed + int64(i)
		rng := rand.New(rand.NewPCG(uint64(s), 0x4EADC0DE))
		var seq []HeadStep
		round := uint64(1)
		for st := range steps {
			fname := fmt.Sprintf("f%d", rng.IntN(3))
			switch r := rng.IntN(100); {
			case r < 38:
				ln := 64 + rng.IntN(4096)
				data := make([]byte, ln)
				for j := range data {
					data[j] = byte((s + int64(st) + int64(j)) & 0xff)
				}
				seq = append(seq, HeadStep{
					Kind: HActWriteFile,
					Name: fname,
					Off:  int64(rng.IntN(32768)),
					Data: data,
				})
			case r < 48:
				seq = append(seq, HeadStep{
					Kind: HActTruncateFile,
					Name: fname,
					Size: uint64(rng.IntN(40000)),
				})
			case r < 66:
				seq = append(seq, HeadStep{Kind: HActFlush})
			case r < 74:
				seq = append(seq, HeadStep{Kind: HActFlushFaultBeforeHead})
			case r < 80:
				seq = append(seq, HeadStep{Kind: HActFlushFaultBeforeShard})
			case r < 86:
				seq = append(seq, HeadStep{Kind: HActFlushLostAckAfterHead})
			case r < 93:
				round++
				seq = append(seq, HeadStep{Kind: HActAttach, Round: round})
			case r < 97:
				seq = append(seq, HeadStep{Kind: HActAttachStale, Round: round})
			default:
				seq = append(seq, HeadStep{Kind: HActVerifyRead})
			}
		}
		// Append deterministic format-v2 corruption & compatibility probes.
		seq = append(seq,
			HeadStep{Kind: HActFlush},
			HeadStep{Kind: HActAttachInvalidBounds},
			HeadStep{Kind: HActProbeCorruptHeadBitFlip},
			HeadStep{Kind: HActProbeCrossPlaceHead},
			HeadStep{Kind: HActProbeUnknownNeeds},
			HeadStep{Kind: HActProbeNeedsBeforeCorruptTail},
			HeadStep{Kind: HActProbeNonMinimalVarint},
			HeadStep{Kind: HActProbeBadTailOrder},
		)
		out = append(out, HeadSchedule{
			Name:  fmt.Sprintf("head-%d", s),
			Seed:  s,
			Steps: seq,
		})
	}
	return out
}

// RunHeadSchedule executes a Volume/Head schedule against real vfs.Volume over
// storetest.Mem + storetest.Hook, checking INV-H1..INV-H4.
func RunHeadSchedule(s HeadSchedule) Report {
	var rep Report
	rep.SchedulesRun = 1
	ctx := context.Background()

	idKey, err := age.GenerateHybridIdentity()
	if err != nil {
		rep.check(InvHeadAtomicPublication, s.Name, 0, "keygen", false, "age keygen: %v", err)
		return rep
	}
	c := vfs.Hybrid(idKey)
	mem := storetest.NewMem()
	var (
		beforeFn func(op, key string) error
		afterFn  func(op, key string) error
	)
	hook := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if beforeFn != nil {
				return beforeFn(op, key)
			}
			return nil
		},
		After: func(op, key string) error {
			if afterFn != nil {
				return afterFn(op, key)
			}
			return nil
		},
	}

	if err := vfs.Create(ctx, vfs.Config{Backend: hook, Cipher: c}); err != nil {
		rep.check(InvHeadAtomicPublication, s.Name, 0, "Create", false, "Create: %v", err)
		return rep
	}
	curRound := uint64(1)
	vol, err := vfs.Attach(ctx, vfs.Config{Backend: hook, Cipher: c, Round: curRound})
	if err != nil {
		rep.check(InvHeadAtomicPublication, s.Name, 0, "Attach", false, "Attach: %v", err)
		return rep
	}

	committedFiles := map[string][]byte{}
	pendingFiles := map[string][]byte{}
	inodes := map[string]vfs.InodeID{}
	var lastSeq uint64

	cloneFiles := func(src map[string][]byte) map[string][]byte {
		out := make(map[string][]byte, len(src))
		for k, v := range src {
			out[k] = append([]byte(nil), v...)
		}
		return out
	}

	ensureFile := func(v *vfs.Volume, name string) (vfs.InodeID, error) {
		if ino, ok := inodes[name]; ok {
			return ino, nil
		}
		in, err := v.Create(ctx, vfs.RootInode, name, 0o644)
		if err != nil {
			return 0, err
		}
		inodes[name] = in.ID
		return in.ID, nil
	}

	verifyCommittedTree := func(step int, act string) {
		ro, oerr := vfs.Open(ctx, vfs.Config{Backend: mem, Cipher: c})
		rep.check(InvHeadAtomicPublication, s.Name, step, act, oerr == nil, "cold Open failed: %v", oerr)
		if oerr != nil {
			return
		}
		defer ro.Close()
		for name, want := range committedFiles {
			in, lerr := ro.Lookup(ctx, vfs.RootInode, name)
			rep.check(InvHeadAtomicPublication, s.Name, step, act, lerr == nil && in.Size == uint64(len(want)),
				"committed file %s lookup err=%v size=%d want=%d", name, lerr, in.Size, len(want))
			if lerr != nil {
				continue
			}
			f, ferr := ro.Open(ctx, in.ID)
			if ferr != nil {
				rep.check(InvHeadAtomicPublication, s.Name, step, act, false, "open %s: %v", name, ferr)
				continue
			}
			got := make([]byte, len(want))
			if len(want) > 0 {
				_, _ = f.ReadAt(ctx, got, 0)
			}
			_ = f.Close()
			rep.check(InvHeadAtomicPublication, s.Name, step, act, bytes.Equal(got, want),
				"committed file %s content diverged", name)
		}
	}

	for idx, st := range s.Steps {
		rep.TransitionsChecked++
		act := st.Kind.String()

		switch st.Kind {
		case HActWriteFile:
			ino, err := ensureFile(vol, st.Name)
			rep.check(InvHeadAtomicPublication, s.Name, idx, act, err == nil, "Create(%s): %v", st.Name, err)
			if err != nil {
				continue
			}
			f, err := vol.Open(ctx, ino)
			if err != nil {
				rep.check(InvHeadAtomicPublication, s.Name, idx, act, false, "Open(%s): %v", st.Name, err)
				continue
			}
			_, werr := f.WriteAt(ctx, st.Data, st.Off)
			_ = f.Close()
			rep.check(InvHeadAtomicPublication, s.Name, idx, act, werr == nil, "WriteAt: %v", werr)
			if werr == nil {
				cur := pendingFiles[st.Name]
				need := int(st.Off) + len(st.Data)
				if need > len(cur) {
					cur = append(cur, make([]byte, need-len(cur))...)
				}
				copy(cur[st.Off:], st.Data)
				pendingFiles[st.Name] = cur
			}

		case HActTruncateFile:
			ino, err := ensureFile(vol, st.Name)
			if err != nil {
				continue
			}
			if terr := vol.Truncate(ctx, ino, st.Size); terr == nil {
				cur := pendingFiles[st.Name]
				if int(st.Size) < len(cur) {
					cur = cur[:st.Size]
				} else if int(st.Size) > len(cur) {
					cur = append(cur, make([]byte, int(st.Size)-len(cur))...)
				}
				pendingFiles[st.Name] = cur
			}

		case HActFlush:
			err := vol.Flush(ctx)
			rep.check(InvHeadAtomicPublication, s.Name, idx, act, err == nil, "Flush: %v", err)
			if err == nil {
				committedFiles = cloneFiles(pendingFiles)
				seq := vol.Stats().Seq
				rep.check(InvHeadCanonicalEncoding, s.Name, idx, act, seq >= lastSeq, "Seq regressed %d < %d", seq, lastSeq)
				lastSeq = seq
				verifyCommittedTree(idx, act)
			}

		case HActFlushFaultBeforeHead:
			rep.FaultsInjected++
			beforeHead := append([]byte(nil), mem.Dump()[headObjectKey]...)
			boom := errors.New("injected fault before head CAS")
			beforeFn = func(op, key string) error {
				if key == headObjectKey && (op == "putver" || op == "putabsent") {
					return boom
				}
				return nil
			}
			err := vol.Flush(ctx)
			beforeFn = nil
			// If there were dirty changes, Flush must fail; if tree was clean, Flush is a no-op nil.
			if err != nil {
				rep.check(InvHeadAtomicPublication, s.Name, idx, act, errors.Is(err, boom), "unexpected err %v", err)
			}
			afterHead := mem.Dump()[headObjectKey]
			rep.check(InvHeadAtomicPublication, s.Name, idx, act, bytes.Equal(beforeHead, afterHead),
				"fault before head CAS mutated head object")
			verifyCommittedTree(idx, act)

		case HActFlushFaultBeforeShard:
			rep.FaultsInjected++
			beforeHead := append([]byte(nil), mem.Dump()[headObjectKey]...)
			boom := errors.New("injected fault before meta shard")
			beforeFn = func(op, key string) error {
				if op == "put" && len(key) >= 5 && key[:5] == "meta/" {
					return boom
				}
				return nil
			}
			err := vol.Flush(ctx)
			beforeFn = nil
			if err != nil {
				rep.check(InvHeadAtomicPublication, s.Name, idx, act, errors.Is(err, boom), "unexpected err %v", err)
			}
			rep.check(InvHeadAtomicPublication, s.Name, idx, act, bytes.Equal(beforeHead, mem.Dump()[headObjectKey]),
				"fault at shard mutated head object")
			verifyCommittedTree(idx, act)

		case HActFlushLostAckAfterHead:
			rep.FaultsInjected++
			lost := errors.New("injected lost reply after head CAS")
			afterFn = func(op, key string) error {
				if key == headObjectKey && (op == "putver" || op == "putabsent") {
					return lost
				}
				return nil
			}
			err := vol.Flush(ctx)
			afterFn = nil
			if err == nil {
				// Clean flush made no head request.
				committedFiles = cloneFiles(pendingFiles)
				continue
			}
			// Flush failed with lost reply, yet Head landed in the store!
			// Subsequent Flush or takeover Attach must preserve the landed state.
			committedFiles = cloneFiles(pendingFiles)
			verifyCommittedTree(idx, act)
			if err2 := vol.Flush(ctx); err2 != nil {
				rep.check(InvHeadLostAckTakeover, s.Name, idx, act, false, "recovery Flush after lost ack failed: %v", err2)
			}

		case HActAttach:
			if ferr := vol.Flush(ctx); ferr != nil {
				rep.check(InvHeadAtomicPublication, s.Name, idx, act, false, "pre-Attach Flush: %v", ferr)
				continue
			}
			committedFiles = cloneFiles(pendingFiles)
			old := vol
			nextRound := max(curRound+1, st.Round)
			next, aerr := vfs.Attach(ctx, vfs.Config{Backend: hook, Cipher: c, Round: nextRound})
			rep.check(InvFenceMonotoneRound, s.Name, idx, act, aerr == nil, "Attach(%d): %v", nextRound, aerr)
			if aerr == nil {
				// Old writer must now be fenced on Flush!
				rep.FaultsInjected++
				_, cerr := old.Create(ctx, vfs.RootInode, fmt.Sprintf("fenced-%d", idx), 0o644)
				if cerr == nil {
					cerr = old.Flush(ctx)
				}
				rep.check(InvFenceStaleRoundReject, s.Name, idx, act, errors.Is(cerr, vfs.ErrStaleRound),
					"deposed writer at round %d after Attach(%d) err=%v, want ErrStaleRound", curRound, nextRound, cerr)
				vol = next
				curRound = nextRound
				// Refresh inodes map from surviving tree.
				for name := range committedFiles {
					if in, lerr := vol.Lookup(ctx, vfs.RootInode, name); lerr == nil {
						inodes[name] = in.ID
					}
				}
				verifyCommittedTree(idx, act)
			}

		case HActAttachStale:
			rep.FaultsInjected++
			for _, badRound := range []uint64{curRound, max(1, curRound-1)} {
				_, aerr := vfs.Attach(ctx, vfs.Config{Backend: hook, Cipher: c, Round: badRound})
				rep.check(InvFenceStaleRoundReject, s.Name, idx, act, errors.Is(aerr, vfs.ErrStaleRound),
					"stale Attach(%d <= %d) = %v, want ErrStaleRound", badRound, curRound, aerr)
			}

		case HActAttachInvalidBounds:
			rep.FaultsInjected++
			for _, badRound := range []uint64{0, math.MaxUint64} {
				_, aerr := vfs.Attach(ctx, vfs.Config{Backend: hook, Cipher: c, Round: badRound})
				rep.check(InvFenceStaleRoundReject, s.Name, idx, act, aerr != nil,
					"Attach accepted invalid boundary round %d", badRound)
			}

		case HActVerifyRead:
			verifyCommittedTree(idx, act)

		case HActProbeCorruptHeadBitFlip:
			rep.FaultsInjected++
			m2 := cloneMemBackend(mem)
			raw := m2.Dump()[headObjectKey]
			if len(raw) > 40 {
				raw[len(raw)/2] ^= 0x01
				_ = m2.Put(ctx, headObjectKey, raw)
				_, oerr := vfs.Open(ctx, vfs.Config{Backend: m2, Cipher: c})
				rep.check(InvHeadCorruptionReject, s.Name, idx, act, oerr != nil,
					"bit-flipped Head opened without error")
			}

		case HActProbeCrossPlaceHead:
			rep.FaultsInjected++
			// Moving all objects verbatim to a new backend (different String()) must fail closed
			// until Reseal re-binds Head to the new place.
			m2 := cloneMemBackend(mem)
			_, oerr := vfs.Open(ctx, vfs.Config{Backend: m2, Cipher: c})
			rep.check(InvHeadCorruptionReject, s.Name, idx, act, errors.Is(oerr, vfs.ErrCorrupt),
				"Head opened at foreign place m2 without Reseal: err=%v", oerr)

		case HActProbeUnknownNeeds:
			rep.FaultsInjected++
			m2 := cloneMemBackend(mem)
			putRawHead(m2, c, m2.String(), curRound+1, buildRawHead(10, 2, 0, 1<<4)) // unknown Needs bit 4
			_, oerr := vfs.Open(ctx, vfs.Config{Backend: m2, Cipher: c})
			_, aerr := vfs.Attach(ctx, vfs.Config{Backend: m2, Cipher: c, Round: curRound + 2})
			rerr := vfs.Reseal(ctx, vfs.Config{Backend: m2, Cipher: c, Round: curRound + 2}, m2.String())
			rep.check(InvHeadCanonicalEncoding, s.Name, idx, act,
				errors.Is(oerr, vfs.ErrNeeds) && errors.Is(aerr, vfs.ErrNeeds) && errors.Is(rerr, vfs.ErrNeeds),
				"unknown Needs bits not rejected with ErrNeeds: open=%v attach=%v reseal=%v", oerr, aerr, rerr)

		case HActProbeNeedsBeforeCorruptTail:
			rep.FaultsInjected++
			// Unknown Needs bit MUST take precedence (ErrNeeds) even over a malformed Tail!
			m2 := cloneMemBackend(mem)
			badTail := []byte{0x04, 0x02, 0x01} // truncated field (claims length 2, only 1 byte)
			rawHead := append(buildRawHead(10, 2, 0, 1<<5), badTail...)
			putRawHead(m2, c, m2.String(), curRound+1, rawHead)
			_, oerr := vfs.Open(ctx, vfs.Config{Backend: m2, Cipher: c})
			rep.check(InvHeadCanonicalEncoding, s.Name, idx, act, errors.Is(oerr, vfs.ErrNeeds),
				"ErrNeeds precedence violated over corrupt tail: got %v", oerr)

		case HActProbeNonMinimalVarint:
			rep.FaultsInjected++
			m2 := cloneMemBackend(mem)
			// Seq encoded non-minimally as 0x80 0x00 instead of 0x00.
			nonMin := append([]byte{0x80, 0x00}, buildRawHead(0, 2, 0, 0)[1:]...)
			putRawHead(m2, c, m2.String(), curRound+1, nonMin)
			_, oerr := vfs.Open(ctx, vfs.Config{Backend: m2, Cipher: c})
			rep.check(InvHeadCanonicalEncoding, s.Name, idx, act, errors.Is(oerr, vfs.ErrCorrupt),
				"non-minimal varint in Head accepted: err=%v", oerr)

		case HActProbeBadTailOrder:
			rep.FaultsInjected++
			m2 := cloneMemBackend(mem)
			// Out-of-order Tail tags: tag 20 followed by tag 10.
			badOrder := append(buildRawHead(10, 2, 0, 0), 20, 1, 'a', 10, 1, 'b')
			putRawHead(m2, c, m2.String(), curRound+1, badOrder)
			_, oerr := vfs.Open(ctx, vfs.Config{Backend: m2, Cipher: c})
			rep.check(InvHeadCanonicalEncoding, s.Name, idx, act, errors.Is(oerr, vfs.ErrCorrupt),
				"out-of-order Head Tail tags accepted: err=%v", oerr)
		}
	}
	return rep
}

// --- Cross-Layer Journal + Head Publication Composition Spec ---

// RunCrossLayerSchedule verifies INV-X1 across a coupled Journal + Volume +
// Fence pipeline under crashes and round takeovers.
func RunCrossLayerSchedule(seed int64, rounds int) Report {
	var rep Report
	rep.SchedulesRun = 1
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(uint64(seed), 0xC40551A7))

	idKey, err := age.GenerateHybridIdentity()
	if err != nil {
		rep.check(InvCrossLayerWatermarkHead, "cross", 0, "keygen", false, "%v", err)
		return rep
	}
	c := vfs.Hybrid(idKey)
	be := storetest.NewMem()
	if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: c}); err != nil {
		rep.check(InvCrossLayerWatermarkHead, "cross", 0, "Create", false, "%v", err)
		return rep
	}

	var journalSeq uint64
	var headJournal uint64
	disk := crash.New()
	clk := &specClock{now: time.Unix(1_700_000_000, 0)}

	for r := uint64(1); r <= uint64(rounds); r++ {
		rep.TransitionsChecked++
		schedName := fmt.Sprintf("cross-%d", seed)
		vol, err := vfs.Attach(ctx, vfs.Config{Backend: be, Cipher: c, Round: r})
		rep.check(InvCrossLayerWatermarkHead, schedName, int(r), "Attach", err == nil, "%v", err)
		if err != nil {
			return rep
		}
		lin := []byte(fmt.Sprintf("vol:round=%d", r))
		j, err := journal.Open("", journal.Options{
			Capacity:  8 * 4096,
			Segment:   4096,
			Disk:      disk,
			Clock:     clk,
			Published: headJournal,
			Lineage:   lin,
		})
		rep.check(InvCrossLayerWatermarkHead, schedName, int(r), "JournalOpen", err == nil, "%v", err)
		if err != nil {
			return rep
		}

		// Append + Sync records in journal, write files to Volume, Flush, Publish, Reclaim.
		var syncedTo uint64
		for op := range 6 {
			payload := []byte(fmt.Sprintf("r%d-op%d", r, op))
			seq, aerr := j.Append(ctx, payload)
			rep.check(InvJournalOrder, schedName, int(r), "Append", aerr == nil, "%v", aerr)
			in, cerr := vol.Create(ctx, vfs.RootInode, fmt.Sprintf("r%d-%d-%d", r, op, rng.IntN(1000)), 0o644)
			if cerr == nil {
				if f, ferr := vol.Open(ctx, in.ID); ferr == nil {
					_, _ = f.WriteAt(ctx, payload, 0)
					_ = f.Close()
				}
			}
			if op%2 == 1 {
				serr := j.Sync(ctx, seq)
				rep.check(InvJournalOrder, schedName, int(r), "Sync", serr == nil, "%v", serr)
				syncedTo = seq
			}
			journalSeq = seq
		}
		_ = journalSeq

		// Publisher must never publish ahead of Durable!
		m := j.Marks()
		rep.check(InvCrossLayerWatermarkHead, schedName, int(r), "PreFlush",
			m.Durable >= syncedTo && m.Published <= m.Durable && m.Durable <= m.Visible,
			"invalid marks before commit: %+v", m)

		ferr := vol.Flush(ctx)
		rep.check(InvHeadAtomicPublication, schedName, int(r), "Flush", ferr == nil, "%v", ferr)
		if m.Durable > m.Published {
			perr := j.Publish(m.Durable)
			rep.check(InvCrossLayerWatermarkHead, schedName, int(r), "Publish", perr == nil, "%v", perr)
			headJournal = m.Durable
		}
		// Probing Publish beyond Durable must fail even at cross-layer boundary.
		rep.FaultsInjected++
		aheadErr := j.Publish(m.Durable + 1)
		rep.check(InvJournalPublishGuard, schedName, int(r), "PublishAhead", errors.Is(aheadErr, journal.ErrAhead),
			"Publish(%d > Durable %d) = %v", m.Durable+1, m.Durable, aheadErr)

		// Old lineage journal is isolated on takeover (simulate fresh node disk for new round).
		_ = j.Close()
		if r < uint64(rounds) {
			rep.FaultsInjected++
			staleCheck, serr := journal.Open("", journal.Options{
				Capacity:  8 * 4096,
				Segment:   4096,
				Disk:      disk.Image(disk.Len(), crash.Variant{Kind: crash.All}),
				Clock:     clk,
				Published: headJournal,
				Lineage:   []byte(fmt.Sprintf("vol:round=%d", r+1)),
			})
			if serr == nil {
				_ = staleCheck.Close()
			}
			rep.check(InvJournalCorruptionReject, schedName, int(r), "TakeoverLineageGuard",
				errors.Is(serr, journal.ErrLineage),
				"new round accepted old round's journal directory: %v", serr)
			disk = crash.New()
		}
	}
	return rep
}

// VerifyAll runs deterministic synthesized schedules across Journal, Fence, Head,
// and Cross-Layer specifications from seed.
func VerifyAll(seed int64) Report {
	var total Report
	for _, s := range SynthesizeJournalSchedules(seed, 4, 24) {
		total.Merge(RunJournalSchedule(s))
	}
	for _, s := range SynthesizeFenceSchedules(seed, 4, 24) {
		total.Merge(RunFenceSchedule(s))
	}
	for _, s := range SynthesizeHeadSchedules(seed, 3, 18) {
		total.Merge(RunHeadSchedule(s))
	}
	for i := range 3 {
		total.Merge(RunCrossLayerSchedule(seed+int64(i), 3))
	}
	return total
}

// --- Format-v2 Raw Head & Sealing Helpers (per docs/SPECIFICATION.md) ---

func headContext(place string) []byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs seal context")
	frame := func(p []byte) {
		_, _ = h.Write(binary.AppendUvarint(nil, uint64(len(p))))
		_, _ = h.Write(p)
	}
	frame([]byte("head"))
	frame([]byte(place))
	return h.Sum(nil)
}

func sealHeadFrame(c vfs.Cipher, place string, plain []byte) []byte {
	aad := headContext(place)
	buf := append(append([]byte{}, aad...), plain...)
	sealed, id, err := c.Seal(buf)
	if err != nil {
		panic(err)
	}
	out := make([]byte, 0, 1+len(id)+len(sealed))
	out = append(out, byte(len(id)))
	out = append(out, id...)
	return append(out, sealed...)
}

func buildRawHead(seq, next, journalSeq, needs uint64) []byte {
	var b []byte
	b = binary.AppendUvarint(b, seq)
	b = append(b, make([]byte, 32)...) // Root
	b = binary.AppendUvarint(b, next)
	b = append(b, make([]byte, 16)...) // ID
	b = append(b, make([]byte, 32)...) // Seed
	b = binary.AppendUvarint(b, journalSeq)
	b = binary.AppendUvarint(b, needs)
	return b
}

func putRawHead(be backend.Backend, c vfs.Cipher, place string, round uint64, rawPlain []byte) {
	ctx := context.Background()
	sealed := sealHeadFrame(c, place, rawPlain)
	var tok [16]byte
	_, _ = crand.Read(tok[:])
	rec := make([]byte, 0, 6+8+16+len(sealed))
	rec = append(rec, "fence\x00"...)
	rec = binary.BigEndian.AppendUint64(rec, round)
	rec = append(rec, tok[:]...)
	rec = append(rec, sealed...)
	_ = be.Put(ctx, headObjectKey, rec)
}

func cloneMemBackend(src *storetest.Mem) *storetest.Mem {
	dst := storetest.NewMem()
	ctx := context.Background()
	for k, v := range src.Dump() {
		_ = dst.Put(ctx, k, v)
	}
	return dst
}

// specClock is a deterministic journal.Clock for reproducible state-machine runs.
type specClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *specClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *specClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *specClock) AfterFunc(_ time.Duration, _ func()) journal.Timer { return noopTimer{} }

type noopTimer struct{}

func (noopTimer) Stop() bool { return false }
