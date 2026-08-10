// Package costmodel implements DESIGN.md §12's cost estimation and
// §12.3's budget admission control. The arithmetic itself is trivial —
// bytes times a per-GB rate, requests times a per-1k rate — the value
// this package adds is doing that arithmetic consistently, off real
// caller-supplied prices, so a placement decision has a cost dimension
// at all (§12.3: "Placement decisions with no cost dimension are not
// decisions").
//
// Prices are never hardcoded here. They come from store.Caps, which
// each Backend fills in from whatever it was actually configured with
// (DESIGN.md §24.1). This is why a Cloudflare R2 backend, whose
// EgressUSDPerGB is legitimately 0, produces a $0 egress estimate
// instead of a wrong one — see §12.1.
//
// Units, fixed once here rather than left ambiguous per call site:
// bytes are always base-2 (a "4 MiB chunk" is 4*1024*1024 bytes), but
// GB in a cost figure is always the decimal billion bytes that cloud
// providers bill in ($/GB, $/1k requests). This matches DESIGN.md
// §12.2's own worked numbers — see costmodel_test.go, which reproduces
// them exactly.
package costmodel

import (
	"fmt"

	"github.com/aburan28/atlasfs/pkg/store"
)

// gbDecimal is bytes per billed GB. Cloud egress/storage pricing is
// decimal (10^9), not binary — mixing this up is exactly the kind of
// silent factor-of-1.074 error a cost model exists to prevent.
const gbDecimal = 1e9

// Estimate is the result of a cost projection: what `atlas placement
// estimate` (DESIGN.md §12.3) prints, and what Policy.Admit checks
// against a budget.
//
// Deliberately does not carry an ExceedsBudget bool: whether a given
// estimate exceeds budget depends on a Policy, which an Estimate has no
// reference to (the same Estimate might be evaluated against several
// subtrees' policies, or none). That check lives in Policy.Admit.
type Estimate struct {
	EgressUSD  float64 // egress transfer cost
	EgressGB   float64 // bytes transferred, in decimal GB — what MaxEgressGBPerDay is checked against
	RequestUSD float64 // GET/PUT request cost
	RequestOps int64   // request count the RequestUSD was computed from
	TotalUSD   float64 // EgressUSD + RequestUSD
	Detail     string  // human-readable breakdown, for CLI output
}

// EstimateReplication projects the one-time cost of pulling objectCount
// objects totaling totalBytes out of a backend — the source-side cost
// of a replication leg (DESIGN.md §13 step 3, §12.1's "replicating W3's
// 50TB corpus from S3 to GCS" example). It prices the source backend's
// egress and GET charges; it does not price the destination's PUTs,
// because the destination is a different Backend with its own Caps —
// call EstimateReplication a second time against the destination with
// PutUSDPer1k in mind if that leg's cost is also needed.
func EstimateReplication(backend store.Caps, objectCount int64, totalBytes int64) Estimate {
	egressGB := float64(totalBytes) / gbDecimal
	egressUSD := egressGB * backend.EgressUSDPerGB
	requestUSD := float64(objectCount) / 1000 * backend.GetUSDPer1k

	return Estimate{
		EgressUSD:  egressUSD,
		EgressGB:   egressGB,
		RequestUSD: requestUSD,
		RequestOps: objectCount,
		TotalUSD:   egressUSD + requestUSD,
		Detail: fmt.Sprintf(
			"replicate %d objects / %.2f GB: egress $%.2f (%.4f $/GB) + %d GETs $%.2f (%.4f $/1k) = $%.2f",
			objectCount, egressGB, egressUSD, backend.EgressUSDPerGB, objectCount, requestUSD, backend.GetUSDPer1k, egressUSD+requestUSD,
		),
	}
}

// RequestRate computes the steady-state GET rate a fleet of nodes
// produces against the origin object store: nodes reading at
// throughputBytesPerSecPerNode, split into chunkSizeBytes chunks, with
// no cache hits. This is DESIGN.md §12.2's "1,000 nodes reading at 5
// GB/s" table — feed the result into EstimateSteadyState with a
// hitRate to get the cached-fleet number.
func RequestRate(nodes int, throughputBytesPerSecPerNode float64, chunkSizeBytes int64) float64 {
	return float64(nodes) * throughputBytesPerSecPerNode / float64(chunkSizeBytes)
}

// EstimateSteadyState projects the hourly request cost of a fleet
// issuing getsPerSec GETs against the origin store at steady-state
// cache hitRate (DESIGN.md §12.2). Only the (1-hitRate) fraction misses
// the node cache and reaches the backend, which is why "cache hit rate
// is the business case, not a performance metric" (§12.2): it is a
// direct multiplier on the bill, not just on latency.
//
// EgressUSD is always 0 here: a steady-state cache miss is served by
// the origin object store in the same region as the reading node
// (§11's read-path hierarchy bottoms out at "origin object store", and
// §12.2's own table prices only GETs) — cross-region reads are a
// replication/placement decision priced by EstimateReplication, not a
// steady-state read cost.
//
// The returned RequestUSD/TotalUSD are $/hour, not a one-time total —
// Detail says so explicitly since Estimate's field names don't carry
// units.
func EstimateSteadyState(backend store.Caps, getsPerSec float64, hitRate float64) Estimate {
	missRate := 1 - hitRate
	originGetsPerSec := getsPerSec * missRate
	requestsPerHour := originGetsPerSec * 3600
	requestUSD := requestsPerHour / 1000 * backend.GetUSDPer1k

	return Estimate{
		RequestUSD: requestUSD,
		RequestOps: int64(requestsPerHour),
		TotalUSD:   requestUSD,
		Detail: fmt.Sprintf(
			"%.0f GET/s demand, %.0f%% hit rate -> %.0f GET/s to origin, %.0f GET/hr, $%.4f/hr (%.4f $/1k)",
			getsPerSec, hitRate*100, originGetsPerSec, requestsPerHour, requestUSD, backend.GetUSDPer1k,
		),
	}
}

// Policy is DESIGN.md §12.3's `placement.budget` block: the cost
// dimension of a placement decision. Field names track the YAML keys
// (egress_usd_per_month, max_egress_gb_per_day, request_usd_per_month,
// on_exceed) so a config loader can map them 1:1.
type Policy struct {
	Subtree string

	// BudgetUSDPerMonth is placement.budget.egress_usd_per_month. 0 means
	// unbounded (no egress budget check).
	BudgetUSDPerMonth float64

	// MaxEgressGBPerDay is placement.budget.max_egress_gb_per_day. 0
	// means unbounded. Checked against Estimate.EgressGB directly — the
	// caller is responsible for producing an Estimate that actually
	// represents one day's worth of transfer if this check is meant to
	// mean something; Admit has no independent notion of time.
	MaxEgressGBPerDay float64

	// RequestBudgetUSDPerMonth is placement.budget.request_usd_per_month.
	// 0 means unbounded.
	RequestBudgetUSDPerMonth float64

	// OnExceed is placement.budget.on_exceed: "block" | "degrade" |
	// "alert" (DESIGN.md §12.3). Empty or any value other than "alert"
	// is treated as blocking, per the design's own examples — an
	// unrecognized on_exceed should fail closed, not silently let a
	// breach through.
	OnExceed string
}

// Admit is the admission-control check DESIGN.md §12.3 requires before
// the replication controller executes work: "estimates cost before
// executing and refuses (or defers, per on_exceed) work that would
// breach budget."
//
// allow is false for both "block" and "degrade": in both cases the
// requested (bulk) work is not admitted as-is. The distinction between
// them is what the caller does next — "degrade falls back to on-demand
// fetch instead of bulk replication" (§12.3) — which is outside Admit's
// scope; it only gates the work actually described by est. "alert"
// admits the work but reason explains why it's over budget, so the
// caller can still surface it.
func (p Policy) Admit(est Estimate) (allow bool, reason string) {
	var breaches []string

	if p.BudgetUSDPerMonth > 0 && est.EgressUSD > p.BudgetUSDPerMonth {
		breaches = append(breaches, fmt.Sprintf(
			"egress $%.2f exceeds budget $%.2f/month", est.EgressUSD, p.BudgetUSDPerMonth))
	}
	if p.MaxEgressGBPerDay > 0 && est.EgressGB > p.MaxEgressGBPerDay {
		breaches = append(breaches, fmt.Sprintf(
			"egress %.2f GB exceeds %.2f GB/day", est.EgressGB, p.MaxEgressGBPerDay))
	}
	if p.RequestBudgetUSDPerMonth > 0 && est.RequestUSD > p.RequestBudgetUSDPerMonth {
		breaches = append(breaches, fmt.Sprintf(
			"requests $%.2f exceeds budget $%.2f/month", est.RequestUSD, p.RequestBudgetUSDPerMonth))
	}

	if len(breaches) == 0 {
		return true, fmt.Sprintf("within budget: $%.2f total", est.TotalUSD)
	}

	summary := breaches[0]
	for _, b := range breaches[1:] {
		summary += "; " + b
	}

	switch p.OnExceed {
	case "alert":
		return true, "alert: " + summary
	case "degrade":
		return false, "degrade: " + summary
	default: // "block" and any unrecognized value fail closed
		return false, "blocked: " + summary
	}
}
