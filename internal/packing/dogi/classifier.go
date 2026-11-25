// Package dogi implements a clean-room, lifetime-aware initial object packing
// engine inspired by DOGI (Kim et al., USENIX FAST '26,
// https://www.usenix.org/conference/fast26/presentation/kim-jeeyun).
//
// Scope: this package governs initial co-packing placement decisions by
// predicted object lifetime (segregating short-lived build artifacts from
// long-lived workspace chunks before slabs are sealed), distinct from
// asynchronous garbage collection scheduling.
package dogi

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
)

// LifetimeClass partitions workspace objects into coarse initial-packing
// lifetime tiers. Keeping the tier count small (3 classes) avoids slab
// underfill and bounds open-slab RAM overhead.
type LifetimeClass uint8

const (
	// ClassEphemeral covers short-lived build intermediates, caches, and
	// scratch outputs expected to die within a few build/clean cycles.
	ClassEphemeral LifetimeClass = iota
	// ClassMedium covers actively edited source files, manifests, and
	// rolling logs with intermediate retention.
	ClassMedium
	// ClassLongLived covers pinned source trees, immutable datasets,
	// model weights, assets, and committed snapshots.
	ClassLongLived
)

// NumLifetimeClasses is the number of coarse lifetime tiers.
const NumLifetimeClasses = 3

// String returns a stable human-readable name for a LifetimeClass.
func (c LifetimeClass) String() string {
	switch c {
	case ClassEphemeral:
		return "ephemeral"
	case ClassMedium:
		return "medium"
	case ClassLongLived:
		return "long_lived"
	default:
		return fmt.Sprintf("class_%d", uint8(c))
	}
}

// ObjectHint carries strictly causal metadata available at initial write/pack
// time. It intentionally excludes any future deletion or expiration timestamps.
type ObjectHint struct {
	TenantID string
	Path     string
	Size     int64
	Epoch    int64
}

// ErrNonMonotonicEpoch is returned when an online classifier receives an
// observation with an epoch earlier than the tenant's previously observed
// causal frontier.
var ErrNonMonotonicEpoch = errors.New("dogi: non-monotonic observation epoch")

// StaticPathClassifier classifies objects using deterministic path-prefix and
// file-extension heuristics alone (no online adaptation).
type StaticPathClassifier struct{}

// Classify returns a LifetimeClass derived purely from the normalized path and
// file extension. Unknown or opaque paths fall back to ClassMedium.
func (StaticPathClassifier) Classify(h ObjectHint) LifetimeClass {
	p := normalizePath(h.Path)
	if isEphemeralPath(p) {
		return ClassEphemeral
	}
	if isLongLivedPath(p) {
		return ClassLongLived
	}
	return ClassMedium
}

// OnlineClassifierConfig tunes the causal per-directory survival-rate feedback
// used by OnlineSurvivalClassifier.
type OnlineClassifierConfig struct {
	// ShortHorizonEpochs defines the age threshold (deleteEpoch - writeEpoch)
	// at or below which a deleted object counts as a short-lived death.
	ShortHorizonEpochs int64
	// EWMAAlpha is the exponential moving average smoothing factor in (0, 1].
	EWMAAlpha float64
	// MinObservations is the minimum number of causal outcome observations for
	// a directory prefix before online feedback can override the static prior.
	MinObservations int
	// EphemeralDeathThreshold is the posterior short-death probability at or
	// above which a directory is classified as ClassEphemeral.
	EphemeralDeathThreshold float64
	// LongLivedDeathThreshold is the posterior short-death probability at or
	// below which a mature directory is classified as ClassLongLived.
	LongLivedDeathThreshold float64
	// PriorWeight controls Bayesian shrinkage toward the static path prior
	// during cold start.
	PriorWeight float64
}

// DefaultOnlineClassifierConfig returns balanced defaults suitable for
// iterative compile/test/clean workspace workloads.
func DefaultOnlineClassifierConfig() OnlineClassifierConfig {
	return OnlineClassifierConfig{
		ShortHorizonEpochs:      4,
		EWMAAlpha:               0.25,
		MinObservations:         6,
		EphemeralDeathThreshold: 0.60,
		LongLivedDeathThreshold: 0.15,
		PriorWeight:             3.0,
	}
}

type dirStats struct {
	ewmaShortDeath float64
	observations   int
	shortDeaths    int
	longSurvivals  int
}

type liveTrack struct {
	dirKey     string
	writeEpoch int64
	matured    bool
}

type tenantClassifierState struct {
	lastEpoch int64
	hasEpoch  bool
	dirs      map[string]*dirStats
	live      map[uint64]*liveTrack
}

// OnlineSurvivalClassifier implements a strictly causal hybrid lifetime
// classifier: it initializes every directory prefix from deterministic path
// priors and updates per-(tenant, dirPrefix) survival estimates online as
// objects are deleted or survive past ShortHorizonEpochs.
//
// Multi-tenant safety: every tenant maintains completely separate directory
// statistics and live object tracking, preventing cross-tenant information
// leakage or adversarial poisoning.
type OnlineSurvivalClassifier struct {
	cfg OnlineClassifierConfig
	mu  sync.RWMutex
	ts  map[string]*tenantClassifierState
}

// NewOnlineSurvivalClassifier constructs a causal hybrid lifetime classifier.
func NewOnlineSurvivalClassifier(cfg OnlineClassifierConfig) *OnlineSurvivalClassifier {
	def := DefaultOnlineClassifierConfig()
	if cfg.ShortHorizonEpochs <= 0 {
		cfg.ShortHorizonEpochs = def.ShortHorizonEpochs
	}
	if cfg.EWMAAlpha <= 0 || cfg.EWMAAlpha > 1 {
		cfg.EWMAAlpha = def.EWMAAlpha
	}
	if cfg.MinObservations <= 0 {
		cfg.MinObservations = def.MinObservations
	}
	if cfg.EphemeralDeathThreshold <= 0 || cfg.EphemeralDeathThreshold >= 1 {
		cfg.EphemeralDeathThreshold = def.EphemeralDeathThreshold
	}
	if cfg.LongLivedDeathThreshold <= 0 || cfg.LongLivedDeathThreshold >= cfg.EphemeralDeathThreshold {
		cfg.LongLivedDeathThreshold = def.LongLivedDeathThreshold
	}
	if cfg.PriorWeight <= 0 {
		cfg.PriorWeight = def.PriorWeight
	}
	return &OnlineSurvivalClassifier{
		cfg: cfg,
		ts:  make(map[string]*tenantClassifierState),
	}
}

func (c *OnlineSurvivalClassifier) tenantStateLocked(tenantID string) *tenantClassifierState {
	st, ok := c.ts[tenantID]
	if !ok {
		st = &tenantClassifierState{
			dirs: make(map[string]*dirStats),
			live: make(map[uint64]*liveTrack),
		}
		c.ts[tenantID] = st
	}
	return st
}

func (c *OnlineSurvivalClassifier) checkEpochLocked(st *tenantClassifierState, epoch int64) error {
	if st.hasEpoch && epoch < st.lastEpoch {
		return fmt.Errorf("%w: got %d < last %d", ErrNonMonotonicEpoch, epoch, st.lastEpoch)
	}
	st.lastEpoch = epoch
	st.hasEpoch = true
	return nil
}

// Classify predicts a LifetimeClass using only path priors and causal
// observations recorded at or before h.Epoch for h.TenantID.
func (c *OnlineSurvivalClassifier) Classify(h ObjectHint) LifetimeClass {
	p := normalizePath(h.Path)
	priorClass, priorDeathProb := pathPrior(p)
	dirKey := extractDirKey(p)

	c.mu.RLock()
	st := c.ts[h.TenantID]
	var (
		obs  int
		ewma float64
	)
	if st != nil {
		if ds, ok := st.dirs[dirKey]; ok {
			obs = ds.observations
			ewma = ds.ewmaShortDeath
		}
	}
	c.mu.RUnlock()

	if obs < c.cfg.MinObservations {
		return priorClass
	}

	// Shrink EWMA toward the path prior using PriorWeight so noisy early
	// observations do not flap classifications prematurely.
	n := float64(obs)
	posteriorDeath := (c.cfg.PriorWeight*priorDeathProb + n*ewma) / (c.cfg.PriorWeight + n)

	if posteriorDeath >= c.cfg.EphemeralDeathThreshold {
		return ClassEphemeral
	}
	if posteriorDeath <= c.cfg.LongLivedDeathThreshold {
		return ClassLongLived
	}
	return ClassMedium
}

// ObserveWrite records an object creation at writeEpoch so subsequent deletes
// or horizon expirations can update the directory's causal survival model.
func (c *OnlineSurvivalClassifier) ObserveWrite(tenantID string, objID uint64, objPath string, writeEpoch int64) error {
	dirKey := extractDirKey(normalizePath(objPath))
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.tenantStateLocked(tenantID)
	if err := c.checkEpochLocked(st, writeEpoch); err != nil {
		return err
	}
	st.live[objID] = &liveTrack{
		dirKey:     dirKey,
		writeEpoch: writeEpoch,
	}
	return nil
}

// ObserveDelete records an object deletion at deleteEpoch. If the object died
// within ShortHorizonEpochs of creation, it updates the directory's EWMA
// short-death probability toward 1.0; otherwise toward 0.0 (if not already
// matured via AdvanceEpoch).
func (c *OnlineSurvivalClassifier) ObserveDelete(tenantID string, objID uint64, deleteEpoch int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.tenantStateLocked(tenantID)
	if err := c.checkEpochLocked(st, deleteEpoch); err != nil {
		return err
	}
	tr, ok := st.live[objID]
	if !ok {
		return nil
	}
	delete(st.live, objID)

	age := deleteEpoch - tr.writeEpoch
	if age <= c.cfg.ShortHorizonEpochs {
		c.recordOutcomeLocked(st, tr.dirKey, true)
	} else if !tr.matured {
		c.recordOutcomeLocked(st, tr.dirKey, false)
	}
	return nil
}

// AdvanceEpoch advances the tenant's causal clock to epoch and credits any
// surviving live objects whose age now exceeds ShortHorizonEpochs as survival
// observations (strictly causal: only past writes that survived to `epoch`).
func (c *OnlineSurvivalClassifier) AdvanceEpoch(tenantID string, epoch int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.tenantStateLocked(tenantID)
	if err := c.checkEpochLocked(st, epoch); err != nil {
		return err
	}
	for _, tr := range st.live {
		if !tr.matured && epoch-tr.writeEpoch > c.cfg.ShortHorizonEpochs {
			tr.matured = true
			c.recordOutcomeLocked(st, tr.dirKey, false)
		}
	}
	return nil
}

func (c *OnlineSurvivalClassifier) recordOutcomeLocked(st *tenantClassifierState, dirKey string, shortDeath bool) {
	ds, ok := st.dirs[dirKey]
	if !ok {
		ds = &dirStats{}
		st.dirs[dirKey] = ds
	}
	sample := 0.0
	if shortDeath {
		sample = 1.0
		ds.shortDeaths++
	} else {
		ds.longSurvivals++
	}
	if ds.observations == 0 {
		ds.ewmaShortDeath = sample
	} else {
		ds.ewmaShortDeath = c.cfg.EWMAAlpha*sample + (1.0-c.cfg.EWMAAlpha)*ds.ewmaShortDeath
	}
	ds.observations++
}

// DirObservationCount returns how many causal outcomes have been recorded for
// a tenant's directory prefix (useful for convergence inspection and tests).
func (c *OnlineSurvivalClassifier) DirObservationCount(tenantID, objPath string) int {
	dirKey := extractDirKey(normalizePath(objPath))
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := c.ts[tenantID]
	if st == nil {
		return 0
	}
	ds := st.dirs[dirKey]
	if ds == nil {
		return 0
	}
	return ds.observations
}

// normalizePath cleans and lowercases a virtual workspace path for matching.
func normalizePath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return ""
	}
	p = path.Clean(p)
	return strings.ToLower(p)
}

// extractDirKey returns a stable bounded directory key (up to the first two
// directory components) so related files under a build/cache subtree share
// online survival statistics without unbounded key cardinality.
func extractDirKey(normPath string) string {
	dir := path.Dir(normPath)
	if dir == "." || dir == "/" || dir == "" {
		return "<root>"
	}
	parts := strings.Split(dir, "/")
	if len(parts) > 2 {
		return parts[0] + "/" + parts[1]
	}
	return dir
}

func pathPrior(normPath string) (LifetimeClass, float64) {
	if isEphemeralPath(normPath) {
		return ClassEphemeral, 0.90
	}
	if isLongLivedPath(normPath) {
		return ClassLongLived, 0.08
	}
	return ClassMedium, 0.40
}

func isEphemeralPath(p string) bool {
	if p == "" {
		return false
	}
	// Known ephemeral directory segments in agent/CI build workspaces.
	ephemeralDirs := []string{
		"target/", "/target/",
		"node_modules/.cache/", "/node_modules/.cache/",
		".cache/", "/.cache/",
		"__pycache__/", "/__pycache__/",
		".pytest_cache/", "/.pytest_cache/",
		".next/cache/", "/.next/cache/",
		"dist/", "/dist/",
		"build/", "/build/",
		"tmp/", "/tmp/",
		"temp/", "/temp/",
		".turbo/", "/.turbo/",
		"coverage/", "/coverage/",
	}
	for _, seg := range ephemeralDirs {
		if strings.HasPrefix(p, seg) || strings.Contains(p, seg) {
			return true
		}
	}

	// Known ephemeral file extensions.
	ext := path.Ext(p)
	switch ext {
	case ".o", ".obj", ".d", ".tmp", ".temp", ".pyc", ".pyo", ".class",
		".swp", ".swo", ".bak", ".part", ".partial", ".a", ".lo", ".la":
		return true
	}
	base := path.Base(p)
	if strings.HasPrefix(base, ".#") || strings.HasSuffix(base, "~") || strings.Contains(base, ".tmp.") {
		return true
	}
	return false
}

func isLongLivedPath(p string) bool {
	if p == "" {
		return false
	}
	longLivedDirs := []string{
		"datasets/", "/datasets/",
		"weights/", "/weights/",
		"models/", "/models/",
		"snapshots/", "/snapshots/",
		"assets/", "/assets/",
		"vendor/", "/vendor/",
		"third_party/", "/third_party/",
		".git/objects/pack/", "/.git/objects/pack/",
		"checkpoints/", "/checkpoints/",
	}
	for _, seg := range longLivedDirs {
		if strings.HasPrefix(p, seg) || strings.Contains(p, seg) {
			return true
		}
	}

	ext := path.Ext(p)
	switch ext {
	case ".safetensors", ".gguf", ".onnx", ".pt", ".pth", ".parquet",
		".arrow", ".woff2", ".png", ".jpg", ".jpeg", ".webp", ".pdf",
		".zip", ".gz", ".zst", ".tar", ".pack", ".idx", ".bin":
		return true
	}
	return false
}
