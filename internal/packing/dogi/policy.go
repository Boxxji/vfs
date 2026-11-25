package dogi

import (
	"hash/fnv"
	"path"
)

// PolicyKind enumerates the 5 matched initial-packing policies evaluated
// against identical workloads, slab geometry, and reclaim consumers.
type PolicyKind string

const (
	// PolicyMixedFIFO packs all objects for a tenant sequentially into a
	// single open slab regardless of lifetime (mirrors single-open-pack baseline).
	PolicyMixedFIFO PolicyKind = "mixed_fifo_packing_baseline"

	// PolicySizeBucket segregates objects strictly by payload size into 3
	// buckets without considering lifetime semantics.
	PolicySizeBucket PolicyKind = "size_bucket_packing"

	// PolicyStaticPath segregates objects into 3 lifetime buckets using static
	// path/extension heuristics only (no online adaptation).
	PolicyStaticPath PolicyKind = "static_path_heuristic_packing"

	// PolicyDOGIAdaptive is the DOGI-inspired hybrid policy: static path priors
	// combined with strictly causal online EWMA per-directory survival feedback,
	// routing into 3 segregated lifetime slabs per tenant under a bounded
	// open-slab memory cap.
	PolicyDOGIAdaptive PolicyKind = "dogi_adaptive_lifetime_packing"

	// PolicyOverFragmented is an intentional negative control that partitions
	// objects across 64 ultra-fine buckets, causing slab underfill, elevated
	// tail-flush overhead, and high open-slab memory pressure.
	PolicyOverFragmented PolicyKind = "over_fragmented_class_negative"
)

// AllPolicies lists all 5 matched policies in canonical evaluation order.
var AllPolicies = []PolicyKind{
	PolicyMixedFIFO,
	PolicySizeBucket,
	PolicyStaticPath,
	PolicyDOGIAdaptive,
	PolicyOverFragmented,
}

// PackingPolicy decides which tenant-local slab bucket receives an object at
// initial pack time. Every policy receives identical causal hooks.
type PackingPolicy interface {
	Kind() PolicyKind
	// AssignBucket returns a deterministic non-negative bucket ID within the
	// object's tenant namespace.
	AssignBucket(h ObjectHint) int
	// ObserveWrite notifies adaptive policies that objID was written at epoch.
	ObserveWrite(tenantID string, objID uint64, objPath string, epoch int64) error
	// ObserveDelete notifies adaptive policies that objID was deleted at epoch.
	ObserveDelete(tenantID string, objID uint64, epoch int64) error
	// AdvanceEpoch advances causal time for tenantID so surviving objects can
	// contribute survival feedback once past ShortHorizonEpochs.
	AdvanceEpoch(tenantID string, epoch int64) error
}

// NewPolicy constructs one of the 5 matched policies.
func NewPolicy(kind PolicyKind) PackingPolicy {
	switch kind {
	case PolicyMixedFIFO:
		return &mixedFIFOPolicy{}
	case PolicySizeBucket:
		return &sizeBucketPolicy{}
	case PolicyStaticPath:
		return &staticPathPolicy{}
	case PolicyDOGIAdaptive:
		return &dogiAdaptivePolicy{
			clf: NewOnlineSurvivalClassifier(DefaultOnlineClassifierConfig()),
		}
	case PolicyOverFragmented:
		return &overFragmentedPolicy{numBuckets: 64}
	default:
		return &mixedFIFOPolicy{}
	}
}

type noopObserver struct{}

func (noopObserver) ObserveWrite(string, uint64, string, int64) error { return nil }
func (noopObserver) ObserveDelete(string, uint64, int64) error        { return nil }
func (noopObserver) AdvanceEpoch(string, int64) error                 { return nil }

type mixedFIFOPolicy struct{ noopObserver }

func (*mixedFIFOPolicy) Kind() PolicyKind            { return PolicyMixedFIFO }
func (*mixedFIFOPolicy) AssignBucket(ObjectHint) int { return 0 }

type sizeBucketPolicy struct{ noopObserver }

func (*sizeBucketPolicy) Kind() PolicyKind { return PolicySizeBucket }
func (*sizeBucketPolicy) AssignBucket(h ObjectHint) int {
	switch {
	case h.Size < 8<<10:
		return 0
	case h.Size < 32<<10:
		return 1
	default:
		return 2
	}
}

type staticPathPolicy struct {
	noopObserver
	clf StaticPathClassifier
}

func (*staticPathPolicy) Kind() PolicyKind { return PolicyStaticPath }
func (p *staticPathPolicy) AssignBucket(h ObjectHint) int {
	return int(p.clf.Classify(h))
}

type dogiAdaptivePolicy struct {
	clf *OnlineSurvivalClassifier
}

func (*dogiAdaptivePolicy) Kind() PolicyKind { return PolicyDOGIAdaptive }
func (p *dogiAdaptivePolicy) AssignBucket(h ObjectHint) int {
	return int(p.clf.Classify(h))
}
func (p *dogiAdaptivePolicy) ObserveWrite(tenantID string, objID uint64, objPath string, epoch int64) error {
	return p.clf.ObserveWrite(tenantID, objID, objPath, epoch)
}
func (p *dogiAdaptivePolicy) ObserveDelete(tenantID string, objID uint64, epoch int64) error {
	return p.clf.ObserveDelete(tenantID, objID, epoch)
}
func (p *dogiAdaptivePolicy) AdvanceEpoch(tenantID string, epoch int64) error {
	return p.clf.AdvanceEpoch(tenantID, epoch)
}

// Classifier exposes the underlying OnlineSurvivalClassifier for diagnostics
// and convergence testing.
func (p *dogiAdaptivePolicy) Classifier() *OnlineSurvivalClassifier {
	return p.clf
}

type overFragmentedPolicy struct {
	noopObserver
	numBuckets int
}

func (*overFragmentedPolicy) Kind() PolicyKind { return PolicyOverFragmented }
func (p *overFragmentedPolicy) AssignBucket(h ObjectHint) int {
	// Spreads writes across 64 ultra-fine buckets keyed by (dir, ext, sizeBand,
	// filename hash) to demonstrate slab underfill and open-slab overhead.
	norm := normalizePath(h.Path)
	h64 := fnv.New64a()
	_, _ = h64.Write([]byte(norm))
	_, _ = h64.Write([]byte{0})
	_, _ = h64.Write([]byte(path.Ext(norm)))
	sizeBand := byte(0)
	switch {
	case h.Size >= 32<<10:
		sizeBand = 2
	case h.Size >= 8<<10:
		sizeBand = 1
	}
	_, _ = h64.Write([]byte{sizeBand})
	return int(h64.Sum64() % uint64(p.numBuckets))
}
