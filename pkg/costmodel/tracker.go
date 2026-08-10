package costmodel

import "sync"

// EgressKey attributes an egress cost observation, matching the label
// set of DESIGN.md §23's atlas_egress_usd_total{subtree,src_region,dst_region}.
type EgressKey struct {
	Subtree   string
	SrcRegion string
	DstRegion string
}

// RequestKey attributes a request cost observation, matching the label
// set of DESIGN.md §23's atlas_requests_total{subtree,region,op}. Op is
// caller-defined ("get", "put", ...), mirroring the Backend methods it
// bills for.
type RequestKey struct {
	Subtree string
	Region  string
	Op      string
}

// RequestTotal is the accumulated observation for one RequestKey: both
// the raw count (what atlas_requests_total counts) and its dollar cost
// (needed for RequestBudgetUSDPerMonth enforcement) — recorded together
// since a caller observing a request already knows what it cost.
type RequestTotal struct {
	Count int64
	USD   float64
}

// CostTracker is DESIGN.md §12.3's attribution requirement made
// concrete: "Without per-subtree attribution, budgets are unenforceable
// and the invoice is unactionable." It is an in-memory stand-in for the
// atlas_egress_usd_total / atlas_requests_total Prometheus counters
// named in §23 — a real Prometheus registry is not this package's
// concern (that's a wiring detail of whatever exports these), so a
// plain mutex-guarded map is the right scope: enough to attribute cost
// per subtree for admission checks and for `atlas` to print, without
// pulling in a metrics client library this package doesn't otherwise
// need.
//
// Safe for concurrent use.
type CostTracker struct {
	mu       sync.Mutex
	egress   map[EgressKey]float64
	requests map[RequestKey]RequestTotal
}

// NewCostTracker returns an empty tracker.
func NewCostTracker() *CostTracker {
	return &CostTracker{
		egress:   make(map[EgressKey]float64),
		requests: make(map[RequestKey]RequestTotal),
	}
}

// AddEgress accumulates an observed egress cost under key.
func (t *CostTracker) AddEgress(key EgressKey, usd float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.egress[key] += usd
}

// AddRequests accumulates count requests costing usd under key.
func (t *CostTracker) AddRequests(key RequestKey, count int64, usd float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tot := t.requests[key]
	tot.Count += count
	tot.USD += usd
	t.requests[key] = tot
}

// EgressTotals returns a snapshot of accumulated egress cost per
// EgressKey. The returned map is a copy; mutating it does not affect
// the tracker.
func (t *CostTracker) EgressTotals() map[EgressKey]float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[EgressKey]float64, len(t.egress))
	for k, v := range t.egress {
		out[k] = v
	}
	return out
}

// RequestTotals returns a snapshot of accumulated request count/cost
// per RequestKey. The returned map is a copy; mutating it does not
// affect the tracker.
func (t *CostTracker) RequestTotals() map[RequestKey]RequestTotal {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[RequestKey]RequestTotal, len(t.requests))
	for k, v := range t.requests {
		out[k] = v
	}
	return out
}

// SubtreeTotalUSD sums egress and request cost across all regions/ops
// for one subtree — the number a BudgetUSDPerMonth check ultimately
// wants, and what a `atlas cost <subtree>` CLI command would print.
func (t *CostTracker) SubtreeTotalUSD(subtree string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total float64
	for k, v := range t.egress {
		if k.Subtree == subtree {
			total += v
		}
	}
	for k, v := range t.requests {
		if k.Subtree == subtree {
			total += v.USD
		}
	}
	return total
}
