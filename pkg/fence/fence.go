// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package fence turns the unconditional Put of an object store, whose contract
// is literally "overwriting is allowed", into a round-fenced write, so a deposed
// or partitioned writer can no longer clobber a newer owner's object. A Volume's
// Head and a replicated SQLite snapshot are both fenced here: this is the one
// fencing implementation.
//
// It is the STORAGE half of single-writer safety: coordination decides who
// writes and at what monotone round; Store makes the object store REJECT a write
// stamped with a superseded round, no matter what a stale local view believes.
//
// # The admission rule
//
// One object per key carries a small round header in front of the payload. The
// store, never this process, enforces atomically:
//
//	admit a Put at `round` iff round >= the round recorded in the object;
//	reject with ErrStaleRound when round < recorded.
//
// `>=` (not the one-shot fence's strict `>`) is deliberate: the single owner
// re-ships the SAME object many times at its STABLE round, so same-round
// overwrites by the current owner MUST be admitted; only a LOWER round, the
// signature of a writer whose lease was taken over and whose successor already
// advanced the recorded round, is refused. A new owner seals the object to its
// higher round exactly once (CarryForward); from that instant every lower-round
// ship by the old owner is fenced forever.
//
// # No admit/write window
//
// The round header and the payload live in ONE object and advance in ONE
// conditional write. There is therefore no interval between "round admitted" and
// "data written" for a second writer to slip into. Because a takeover's claim is
// a compare-and-set conditioned on the version just read, an owner that slips a
// write in between the successor's read and its claim only makes the claim retry
// (re-reading the newer payload), so the successor always carries forward the
// latest durably-shipped bytes before it seals: no acknowledged data is lost.
package fence

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/pkg/backend"
)

var (
	// ErrStaleRound is returned when a Put's round is below the round already
	// recorded for the object: the deposed or partitioned writer rejection. The
	// object is left byte-for-byte untouched. Once a successor advances the
	// recorded round, the predecessor can never write again.
	ErrStaleRound = errors.New("fence: write round is below the recorded round (writer fenced)")

	// errCorrupt is returned when a stored object does not decode as a record this
	// package wrote: fail closed rather than invent a round from arbitrary bytes.
	errCorrupt = errors.New("fence: stored object is not a valid fenced record")
)

// maxCASAttempts bounds the read-check-CAS retry loops so a pathologically hot
// object fails loudly rather than spinning forever. One retry suffices for any
// single genuine race; further attempts only absorb repeated contention.
const maxCASAttempts = 8

// Backend is the part of backend.Backend the fence needs: a versioned read and
// the two atomic conditional writes. PutIfAbsent and PutIfVersion MUST evaluate
// their precondition atomically, server-side; a client-side "Get, compare in Go,
// then Put" is NOT a valid implementation, since that gap is the exact race this
// fence exists to close.
type Backend interface {
	Get(ctx context.Context, key string) ([]byte, backend.Version, error)
	PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error)
	PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error)
}

// Store is a round-fenced writer over a Backend: one object per key, a monotone
// round admitted only when at least the recorded one.
type Store struct{ be Backend }

// New builds a Store over a conditional-write backend.
func New(be Backend) *Store { return &Store{be: be} }

// magic prefixes every record so a foreign object at the same key is detected
// (errCorrupt) rather than silently misparsed as an absurd round.
const magic = "rfnc1\x00"

func encode(round uint64, payload []byte) []byte {
	buf := make([]byte, 0, len(magic)+8+len(payload))
	buf = append(buf, magic...)
	var r [8]byte
	binary.BigEndian.PutUint64(r[:], round)
	buf = append(buf, r[:]...)
	return append(buf, payload...)
}

func decode(b []byte) (round uint64, payload []byte, err error) {
	m := len(magic)
	if len(b) < m+8 || string(b[:m]) != magic {
		return 0, nil, errCorrupt
	}
	return binary.BigEndian.Uint64(b[m : m+8]), b[m+8:], nil
}

// write conditions a write on the version just read: create-only when the key was
// absent (version ""), else compare-and-set on that version.
func (s *Store) write(ctx context.Context, key string, data []byte, v backend.Version) error {
	var err error
	if v == "" {
		_, err = s.be.PutIfAbsent(ctx, key, data)
	} else {
		_, err = s.be.PutIfVersion(ctx, key, data, v)
	}
	return err
}

// Create writes payload at key at round 0, only if key does not exist. It is how
// an object is born: Put would admit round 0 over an existing round-0 object.
// If key exists the error wraps backend.ErrConflict.
func (s *Store) Create(ctx context.Context, key string, payload []byte) error {
	if _, err := s.be.PutIfAbsent(ctx, key, encode(0, payload)); err != nil {
		return fmt.Errorf("fence: create %s: %w", key, err)
	}
	return nil
}

// Put admits and (atomically with admission) writes payload for key at round. It
// rejects with ErrStaleRound when round is below the recorded round, the fence,
// before any write is attempted, then advances the header and replaces the
// payload in a single conditional write on the version read THIS attempt (never a
// cached one). If a concurrent writer's write lands first, our CAS fails and we
// retry: re-read the now-authoritative round, re-run the check (a lower-round
// racer now loses as ErrStaleRound; an equal-or-higher one retries the CAS
// against the new version). The store is the sole arbiter of which round owns the
// key. An absent key is recorded round 0, so any round admits a first write.
func (s *Store) Put(ctx context.Context, key string, payload []byte, round uint64) error {
	if s == nil || s.be == nil {
		return fmt.Errorf("fence: nil store")
	}
	var lastErr error
	for range maxCASAttempts {
		recorded, version, err := s.current(ctx, key)
		if err != nil {
			return err
		}
		if round < recorded {
			return fmt.Errorf("%w: key=%s round=%d recorded=%d", ErrStaleRound, key, round, recorded)
		}
		if err := s.write(ctx, key, encode(round, payload), version); err != nil {
			if errors.Is(err, backend.ErrConflict) {
				lastErr = err
				continue // re-read the winner's state and retry.
			}
			return fmt.Errorf("fence: put %s: %w", key, err)
		}
		return nil
	}
	return fmt.Errorf("%w: exhausted %d attempts fencing %s at round %d", lastErr, maxCASAttempts, key, round)
}

// CarryForward is the safe TAKEOVER seal: it reads the latest durably-stored
// payload for key and re-seals it UNCHANGED at round, invoking hydrate on that
// payload first. It exists because a successor must never seal a payload it read
// BEFORE a predecessor's last landed write, or that acknowledged write would be
// clobbered. The read and the seal are one compare-and-set on the same version:
// if a predecessor write lands in between, the CAS conflicts and CarryForward
// retries, re-reading the newer payload, re-hydrating, and re-sealing IT. After
// CarryForward returns, every predecessor write at a round below `round` is fenced.
//
// hydrate receives the payload and the round recorded with it, and may refuse the
// takeover by returning an error (CarryForward returns it and writes nothing). It
// may run more than once across retries, so it must be idempotent. An absent key is
// recorded round 0 with a nil payload.
func (s *Store) CarryForward(ctx context.Context, key string, round uint64, hydrate func(payload []byte, recorded uint64) error) error {
	if s == nil || s.be == nil {
		return fmt.Errorf("fence: nil store")
	}
	var lastErr error
	for range maxCASAttempts {
		data, version, err := s.be.Get(ctx, key)
		var recorded uint64
		var payload []byte
		switch {
		case errors.Is(err, backend.ErrNotFound):
			version = ""
		case err != nil:
			return fmt.Errorf("fence: carry-forward read %s: %w", key, err)
		default:
			if recorded, payload, err = decode(data); err != nil {
				return err
			}
		}
		if round < recorded {
			return fmt.Errorf("%w: key=%s round=%d recorded=%d", ErrStaleRound, key, round, recorded)
		}
		if hydrate != nil {
			if err := hydrate(payload, recorded); err != nil {
				return fmt.Errorf("fence: carry-forward hydrate %s: %w", key, err)
			}
		}
		if err := s.write(ctx, key, encode(round, payload), version); err != nil {
			if errors.Is(err, backend.ErrConflict) {
				lastErr = err
				continue // a predecessor's write landed; re-read it and carry it forward.
			}
			return fmt.Errorf("fence: carry-forward seal %s: %w", key, err)
		}
		return nil
	}
	return fmt.Errorf("%w: exhausted %d attempts carrying forward %s at round %d", lastErr, maxCASAttempts, key, round)
}

// Get returns the fenced payload and the round it was written at, or an error
// wrapping backend.ErrNotFound when key is absent.
func (s *Store) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	data, _, err := s.be.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	round, payload, err := decode(data)
	if err != nil {
		return nil, 0, err
	}
	return payload, round, nil
}

// Round returns the round currently recorded for key, or 0 if the object is
// absent: the value a new owner exceeds to claim the writer role.
func (s *Store) Round(ctx context.Context, key string) (uint64, error) {
	round, _, err := s.current(ctx, key)
	return round, err
}

// current returns the recorded round (0 if absent) and the version token to
// condition the next write on ("" if absent).
func (s *Store) current(ctx context.Context, key string) (round uint64, version backend.Version, err error) {
	data, version, err := s.be.Get(ctx, key)
	switch {
	case errors.Is(err, backend.ErrNotFound):
		return 0, "", nil
	case err != nil:
		return 0, "", fmt.Errorf("fence: read %s: %w", key, err)
	}
	round, _, err = decode(data)
	if err != nil {
		return 0, "", err
	}
	return round, version, nil
}
