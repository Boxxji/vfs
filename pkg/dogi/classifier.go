// Copyright 2026 Hanzo AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package dogi

import (
	"path"
	"strings"
	"sync"
)

// DirStatsSnapshot exposes a read-only view of online per-directory lifetime statistics.
type DirStatsSnapshot struct {
	Dir                string  `json:"dir"`
	Writes             uint64  `json:"writes"`
	Deletes            uint64  `json:"deletes"`
	Overwrites         uint64  `json:"overwrites"`
	Survivals          uint64  `json:"survivals"`
	EWMAExpectedLife   float64 `json:"ewma_expected_life"`
	EWMAShortDeathRate float64 `json:"ewma_short_death_rate"`
	RecentBurstWrites  uint64  `json:"recent_burst_writes"`
}

type dirStats struct {
	writes             uint64
	deletes            uint64
	overwrites         uint64
	survivals          uint64
	ewmaLife           float64
	ewmaShortDeathRate float64
	hasLifeSample      bool
	burstEpoch         uint64
	burstWrites        uint64
	ewmaSize           float64
}

// OnlineDirLifetimeTracker learns per-directory invalidation behavior online
// using exponentially weighted moving averages (EWMA) of observed lifetimes,
// death rates, overwrite frequencies, and write bursts. It never peeks at
// future deletion events.
type OnlineDirLifetimeTracker struct {
	mu   sync.RWMutex
	dirs map[string]*dirStats
}

// NewOnlineDirLifetimeTracker creates an empty thread-safe online tracker.
func NewOnlineDirLifetimeTracker() *OnlineDirLifetimeTracker {
	return &OnlineDirLifetimeTracker{
		dirs: make(map[string]*dirStats),
	}
}

func (t *OnlineDirLifetimeTracker) getOrCreateLocked(dir string) *dirStats {
	st, ok := t.dirs[dir]
	if !ok {
		st = &dirStats{
			ewmaLife:           4.0,
			ewmaShortDeathRate: 0.5,
		}
		t.dirs[dir] = st
	}
	return st
}

// RecordWrite updates write-count, size EWMA, and burst counters for dir.
func (t *OnlineDirLifetimeTracker) RecordWrite(dir string, size int, epoch uint64, isOverwrite bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.getOrCreateLocked(dir)
	st.writes++
	if isOverwrite {
		st.overwrites++
	}
	if st.writes == 1 {
		st.ewmaSize = float64(size)
	} else {
		st.ewmaSize = 0.8*st.ewmaSize + 0.2*float64(size)
	}
	if st.burstEpoch == epoch {
		st.burstWrites++
	} else {
		st.burstEpoch = epoch
		st.burstWrites = 1
	}
}

// RecordDelete records an observed invalidation (delete or overwrite) for a chunk
// created at createdEpoch and invalidated at deleteEpoch.
func (t *OnlineDirLifetimeTracker) RecordDelete(dir string, createdEpoch, deleteEpoch uint64, isOverwrite bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.getOrCreateLocked(dir)
	st.deletes++
	if isOverwrite {
		st.overwrites++
	}
	life := float64(0)
	if deleteEpoch >= createdEpoch {
		life = float64(deleteEpoch - createdEpoch)
	}
	const alphaLife = 0.30
	if !st.hasLifeSample {
		st.ewmaLife = life
		st.hasLifeSample = true
	} else {
		st.ewmaLife = (1-alphaLife)*st.ewmaLife + alphaLife*life
	}

	const alphaRate = 0.25
	var shortIndicator float64
	switch {
	case life <= 2.0:
		shortIndicator = 1.0
	case life <= 5.0:
		shortIndicator = 0.45
	default:
		shortIndicator = 0.0
	}
	if st.deletes == 1 && st.survivals == 0 {
		st.ewmaShortDeathRate = shortIndicator
	} else {
		st.ewmaShortDeathRate = (1-alphaRate)*st.ewmaShortDeathRate + alphaRate*shortIndicator
	}
}

// RecordEpochSurvival records that count live chunks in dir have survived at
// least ageEpochs without deletion. This allows the tracker to promote long-lived
// directories (even those under misleading names such as tmp/golden/) out of
// short-lived streams once survival evidence accumulates.
func (t *OnlineDirLifetimeTracker) RecordEpochSurvival(dir string, count int, ageEpochs uint64) {
	if count <= 0 || ageEpochs < 2 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.getOrCreateLocked(dir)
	st.survivals += uint64(count)

	// Cap per-epoch EWMA step so a single large directory does not oscillate,
	// while still converging smoothly within 2-3 epochs.
	steps := count
	if steps > 12 {
		steps = 12
	}
	const alphaSurv = 0.18
	impliedLife := float64(ageEpochs) + 3.0
	for i := 0; i < steps; i++ {
		st.ewmaShortDeathRate = (1 - alphaSurv) * st.ewmaShortDeathRate
		if !st.hasLifeSample {
			st.ewmaLife = impliedLife
			st.hasLifeSample = true
		} else if impliedLife > st.ewmaLife {
			st.ewmaLife = (1-alphaSurv)*st.ewmaLife + alphaSurv*impliedLife
		}
	}
}

// Snapshot returns a copy of tracked stats for dir, if any exist.
func (t *OnlineDirLifetimeTracker) Snapshot(dir string) (DirStatsSnapshot, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.dirs[dir]
	if !ok {
		return DirStatsSnapshot{}, false
	}
	return DirStatsSnapshot{
		Dir:                dir,
		Writes:             st.writes,
		Deletes:            st.deletes,
		Overwrites:         st.overwrites,
		Survivals:          st.survivals,
		EWMAExpectedLife:   st.ewmaLife,
		EWMAShortDeathRate: st.ewmaShortDeathRate,
		RecentBurstWrites:  st.burstWrites,
	}, true
}

// Classifier assigns incoming writes and GC-relocated chunks to lifetime streams
// according to the configured Policy.
type Classifier struct {
	policy  Policy
	tracker *OnlineDirLifetimeTracker
}

// NewClassifier creates a Classifier for policy backed by an online directory tracker.
func NewClassifier(policy Policy) *Classifier {
	return &Classifier{
		policy:  policy,
		tracker: NewOnlineDirLifetimeTracker(),
	}
}

// Tracker returns the underlying online per-directory lifetime tracker.
func (c *Classifier) Tracker() *OnlineDirLifetimeTracker {
	return c.tracker
}

// normalizePath returns clean POSIX path, parent directory, and lowercase extension.
func normalizePath(raw string) (cleanPath, dir, ext string) {
	p := strings.ReplaceAll(raw, "\\", "/")
	p = strings.TrimLeft(p, "/")
	cleanPath = path.Clean(p)
	if cleanPath == "." {
		cleanPath = ""
	}
	dir = path.Dir(cleanPath)
	if dir == "." {
		dir = ""
	}
	ext = strings.ToLower(path.Ext(cleanPath))
	return cleanPath, dir, ext
}

// staticPathPrior classifies a path solely from observable path, directory, and
// extension conventions. Returns (class, confidence) where confidence is in (0, 1].
func staticPathPrior(cleanPath, dir, ext string) (LifetimeClass, float64) {
	lower := strings.ToLower(cleanPath)
	padded := "/" + lower + "/"

	// 1. Pinned / immutable artifacts (.git/objects, model weights, snapshots).
	if strings.Contains(padded, "/.git/objects/") ||
		strings.Contains(padded, "/.git/packs/") ||
		strings.Contains(padded, "/snapshots/") ||
		strings.Contains(padded, "/releases/") {
		return ClassPinnedImmutable, 0.90
	}
	switch ext {
	case ".safetensors", ".gguf", ".onnx", ".pack", ".idx":
		return ClassPinnedImmutable, 0.90
	}
	if strings.Contains(padded, "/models/") && (ext == ".bin" || ext == ".pt" || ext == ".pth") {
		return ClassPinnedImmutable, 0.85
	}

	// 2. Short-lived build artifacts, caches, and scratch outputs.
	shortDirs := []string{
		"/target/",
		"/node_modules/.cache/",
		"/.next/",
		"/.cache/",
		"/tmp/",
		"/__pycache__/",
		"/.pytest_cache/",
		"/build/tmp/",
		"/dist/.tmp/",
		"/.turbo/",
		"/.gradle/",
	}
	for _, pat := range shortDirs {
		if strings.Contains(padded, pat) {
			return ClassShortLived, 0.85
		}
	}
	switch ext {
	case ".o", ".obj", ".tmp", ".temp", ".pyc", ".pyo", ".d", ".rmeta", ".swp", ".part":
		return ClassShortLived, 0.85
	}
	if strings.HasSuffix(lower, ".lock.tmp") || strings.HasSuffix(lower, ".log.tmp") {
		return ClassShortLived, 0.85
	}

	// 3. Long-lived repository dependencies, lockfiles, docs, and stable packages.
	longDirs := []string{
		"/vendor/",
		"/third_party/",
		"/docs/",
		"/assets/",
		"/testdata/",
		"/pkg/",
		"/internal/",
		"/cmd/",
		"/src/",
	}
	for _, pat := range longDirs {
		if strings.Contains(padded, pat) {
			return ClassLongLived, 0.78
		}
	}
	base := path.Base(lower)
	switch base {
	case "go.sum", "go.mod", "package-lock.json", "cargo.lock", "yarn.lock", "pnpm-lock.yaml", "license", "readme.md":
		return ClassLongLived, 0.85
	}
	switch ext {
	case ".md", ".txt", ".sum", ".mod", ".toml", ".yaml", ".yml", ".json":
		return ClassLongLived, 0.65
	}

	// 4. Unknown / opaque path: low-confidence medium fallback so online EWMA adapts fast.
	return ClassMediumLived, 0.30
}

// oracleClassFromRemaining maps exact hindsight remaining epochs to a LifetimeClass.
// Called ONLY when Policy == PolicyOracleUpperBound.
func oracleClassFromRemaining(currentEpoch, deathEpoch uint64) LifetimeClass {
	if deathEpoch == 0 {
		return ClassPinnedImmutable
	}
	if deathEpoch <= currentEpoch {
		return ClassShortLived
	}
	rem := deathEpoch - currentEpoch
	switch {
	case rem <= 2:
		return ClassShortLived
	case rem <= 7:
		return ClassMediumLived
	case rem <= 16:
		return ClassLongLived
	default:
		return ClassPinnedImmutable
	}
}

// ClassifyWrite assigns an incoming user-written chunk to a LifetimeClass.
// For all non-oracle policies, meta.OracleDeathEpoch is strictly ignored.
func (c *Classifier) ClassifyWrite(meta ChunkMeta, size int) LifetimeClass {
	switch c.policy {
	case PolicyFIFOMixed:
		return ClassShortLived // single stream 0
	case PolicyOracleUpperBound:
		return oracleClassFromRemaining(meta.Epoch, meta.OracleDeathEpoch)
	case PolicyPathGrouped:
		cleanPath, dir, ext := normalizePath(meta.Path)
		cls, _ := staticPathPrior(cleanPath, dir, ext)
		return cls
	case PolicyDOGILifetime:
		return c.classifyDOGIWrite(meta.Path, size, meta.Epoch)
	default:
		return ClassShortLived
	}
}

func (c *Classifier) classifyDOGIWrite(rawPath string, size int, epoch uint64) LifetimeClass {
	cleanPath, dir, ext := normalizePath(rawPath)
	priorClass, priorConf := staticPathPrior(cleanPath, dir, ext)

	snap, hasStats := c.tracker.Snapshot(dir)
	// Also check parent directory if exact dir has too few observations.
	if (!hasStats || (snap.Deletes+snap.Survivals) < 4) && strings.Contains(dir, "/") {
		parent := path.Dir(dir)
		if ps, ok := c.tracker.Snapshot(parent); ok && (ps.Deletes+ps.Survivals) > (snap.Deletes+snap.Survivals) {
			snap = ps
			hasStats = true
		}
	}

	priorScore := float64(priorClass)
	score := priorScore

	obs := float64(snap.Deletes + snap.Survivals)
	if hasStats && obs >= 4 {
		onlineScore := onlineLifetimeScore(snap)
		// Unknown/opaque paths have low priorWeight (fast 1-2 epoch learning).
		// Strong static priors have higher priorWeight (models realistic adaptation
		// lag on adversarial inverted priors before online evidence dominates).
		priorWeight := 4.0
		if priorConf > 0.50 {
			priorWeight = 18.0
		}
		wOnline := obs / (obs + priorWeight)
		score = (1.0-wOnline)*priorScore + wOnline*onlineScore
	} else if priorConf <= 0.40 {
		// Cold-start size and write-burst heuristic for unknown paths.
		if size >= 128<<10 && (!hasStats || snap.RecentBurstWrites < 8) {
			score += 0.45
		} else if hasStats && snap.RecentBurstWrites >= 16 && size <= 32<<10 {
			score -= 0.30
		}
	}

	return scoreToClass(score)
}

func onlineLifetimeScore(snap DirStatsSnapshot) float64 {
	switch {
	case snap.EWMAShortDeathRate >= 0.65 || (snap.Deletes >= 4 && snap.EWMAExpectedLife <= 2.2):
		return 0.0 // ClassShortLived
	case snap.EWMAShortDeathRate >= 0.32 || (snap.Deletes >= 4 && snap.EWMAExpectedLife <= 6.5):
		return 1.0 // ClassMediumLived
	case snap.Survivals >= 16 && snap.Deletes == 0 && snap.EWMAExpectedLife >= 8.0:
		return 3.0 // ClassPinnedImmutable
	default:
		return 2.0 // ClassLongLived
	}
}

func scoreToClass(score float64) LifetimeClass {
	switch {
	case score < 0.55:
		return ClassShortLived
	case score < 1.50:
		return ClassMediumLived
	case score < 2.55:
		return ClassLongLived
	default:
		return ClassPinnedImmutable
	}
}

// ClassifyRelocation re-predicts the destination LifetimeClass for a surviving
// live chunk being copied out of a victim pack during GC compaction (implementing
// DOGI's Insight #2: GC-written blocks must be re-predicted rather than blindly
// dumped into one cold bucket or left in a hot churn stream).
func (c *Classifier) ClassifyRelocation(rawPath string, createdEpoch, currentEpoch uint64, gcGen int, oracleDeathEpoch uint64) LifetimeClass {
	switch c.policy {
	case PolicyFIFOMixed:
		return ClassShortLived // single stream 0
	case PolicyOracleUpperBound:
		return oracleClassFromRemaining(currentEpoch, oracleDeathEpoch)
	case PolicyPathGrouped:
		cleanPath, dir, ext := normalizePath(rawPath)
		cls, _ := staticPathPrior(cleanPath, dir, ext)
		return cls
	case PolicyDOGILifetime:
		cleanPath, dir, ext := normalizePath(rawPath)
		priorClass, _ := staticPathPrior(cleanPath, dir, ext)
		age := uint64(0)
		if currentEpoch >= createdEpoch {
			age = currentEpoch - createdEpoch
		}

		snap, hasStats := c.tracker.Snapshot(dir)
		if hasStats && (snap.Deletes+snap.Survivals) >= 4 {
			// If directory is still demonstrably high-churn and chunk is young,
			// hold it in MediumLived so it does not pollute LongLived packs.
			if snap.EWMAShortDeathRate >= 0.65 && age < 3 && gcGen <= 1 {
				return ClassMediumLived
			}
			// If chunk has survived multiple GC generations and its dir is cold,
			// promote it to PinnedImmutable.
			if age >= 6 && gcGen >= 2 && snap.EWMAShortDeathRate < 0.20 {
				return ClassPinnedImmutable
			}
			if age >= 2 && snap.EWMAShortDeathRate < 0.35 {
				return ClassLongLived
			}
			onlineCls := scoreToClass(onlineLifetimeScore(snap))
			if onlineCls == ClassShortLived && age >= 2 {
				return ClassMediumLived
			}
			return onlineCls
		}

		// Without enough dir stats yet, use age + static prior.
		if age >= 6 && gcGen >= 2 {
			return ClassPinnedImmutable
		}
		if age >= 2 || priorClass >= ClassLongLived {
			if priorClass == ClassPinnedImmutable {
				return ClassPinnedImmutable
			}
			return ClassLongLived
		}
		return ClassMediumLived
	default:
		return ClassShortLived
	}
}
