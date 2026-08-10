package costmodel

import (
	"math"
	"testing"

	"github.com/aburan28/atlasfs/pkg/store"
)

func approxEqual(t *testing.T, got, want, tolFrac float64, what string) {
	t.Helper()
	tol := math.Abs(want) * tolFrac
	if math.Abs(got-want) > tol {
		t.Errorf("%s: got %v, want %v (+/- %.1f%%)", what, got, want, tolFrac*100)
	}
}

// TestEstimateReplication_50TB reproduces DESIGN.md §12.1: "Replicating
// W3's 50 TB corpus from S3 to GCS is $2,500-$4,500, one-time, per
// replica" at AWS's stated egress range of $0.05-$0.09/GB. At the top
// of that range (0.09/GB) the number is exactly $4,500 given 50 TB in
// decimal (50 * 1000 GB), which is how cloud providers bill.
func TestEstimateReplication_50TB(t *testing.T) {
	const fiftyTB = 50 * 1_000_000_000_000 // 50 TB, decimal

	caps := store.Caps{EgressUSDPerGB: 0.09}
	est := EstimateReplication(caps, 0, fiftyTB)

	if est.EgressGB != 50_000 {
		t.Fatalf("EgressGB = %v, want 50000", est.EgressGB)
	}
	if got, want := est.EgressUSD, 4500.0; got != want {
		t.Fatalf("EgressUSD = %v, want %v (exact: 50000 GB * $0.09/GB)", got, want)
	}

	// Low end of the $0.05-$0.09/GB range: DESIGN.md's $2,500 figure.
	caps.EgressUSDPerGB = 0.05
	est = EstimateReplication(caps, 0, fiftyTB)
	if got, want := est.EgressUSD, 2500.0; got != want {
		t.Fatalf("EgressUSD at $0.05/GB = %v, want %v", got, want)
	}
}

// TestEstimateReplication_RequestCost checks the GET side of a
// replication estimate independently of egress, using S3's own
// $0.0004/1k GET price from §12.2.
func TestEstimateReplication_RequestCost(t *testing.T) {
	caps := store.Caps{GetUSDPer1k: 0.0004}
	est := EstimateReplication(caps, 1_000_000, 0)
	if got, want := est.RequestUSD, 0.4; got != want {
		t.Fatalf("RequestUSD for 1M GETs = %v, want %v", got, want)
	}
	if est.RequestOps != 1_000_000 {
		t.Fatalf("RequestOps = %d, want 1000000", est.RequestOps)
	}
}

// TestRequestRate_1000NodesAt5GBps reproduces the per-node and
// aggregate GET/s figures from DESIGN.md §12.2's table: "1,000 nodes
// reading at 5 GB/s" with 4 MiB chunks, all cold, gives 1,192 GET/s per
// node and 1.19M GET/s total.
func TestRequestRate_1000NodesAt5GBps(t *testing.T) {
	const (
		nodes         = 1000
		throughputBps = 5e9 // 5 GB/s decimal, matches cloud throughput billing
		chunk4MiB     = 4 * 1024 * 1024
	)

	perNode := RequestRate(1, throughputBps, chunk4MiB)
	approxEqual(t, perNode, 1192, 0.01, "per-node GET/s at 4 MiB")

	total := RequestRate(nodes, throughputBps, chunk4MiB)
	approxEqual(t, total, 1_192_000, 0.01, "total GET/s at 4 MiB, all cold")

	// §12.2's second row: 16 MiB chunks -> 298 GET/s/node, 298k total.
	const chunk16MiB = 16 * 1024 * 1024
	perNode16 := RequestRate(1, throughputBps, chunk16MiB)
	approxEqual(t, perNode16, 298, 0.01, "per-node GET/s at 16 MiB")
	total16 := RequestRate(nodes, throughputBps, chunk16MiB)
	approxEqual(t, total16, 298_000, 0.01, "total GET/s at 16 MiB, all cold")
}

// TestEstimateSteadyState_DesignDocTable reproduces every row of
// DESIGN.md §12.2's cost table for 1,000 nodes at 5 GB/s against S3's
// $0.0004/1k GET price:
//
//	4 MiB, all cold        -> 1.19M GET/s  -> $1,717/hr
//	16 MiB, all cold        ->  298k GET/s  ->   $429/hr
//	4 MiB, 95% cache hit    ->   60k GET/s  ->    $86/hr
//
// If this ever stops matching, either this package's arithmetic is
// wrong or DESIGN.md's own numbers are — see the test failure message
// for which.
func TestEstimateSteadyState_DesignDocTable(t *testing.T) {
	const (
		nodes         = 1000
		throughputBps = 5e9
		s3GetPrice    = 0.0004 // $/1k GETs
	)
	caps := store.Caps{GetUSDPer1k: s3GetPrice}

	cases := []struct {
		name         string
		chunkBytes   int64
		hitRate      float64
		wantGetsPerS float64
		wantUSDPerHr float64
	}{
		{"4MiB cold", 4 * 1024 * 1024, 0.0, 1_192_000, 1717},
		{"16MiB cold", 16 * 1024 * 1024, 0.0, 298_000, 429},
		{"4MiB 95% hit", 4 * 1024 * 1024, 0.95, 60_000, 86},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rate := RequestRate(nodes, throughputBps, c.chunkBytes)
			est := EstimateSteadyState(caps, rate, c.hitRate)

			approxEqual(t, rate*(1-c.hitRate), c.wantGetsPerS, 0.02, "origin GET/s")
			approxEqual(t, est.RequestUSD, c.wantUSDPerHr, 0.02, "$/hr")
			if est.TotalUSD != est.RequestUSD {
				t.Errorf("TotalUSD = %v, want equal to RequestUSD %v (no egress modeled for steady-state)", est.TotalUSD, est.RequestUSD)
			}
			if est.EgressUSD != 0 {
				t.Errorf("EgressUSD = %v, want 0 for steady-state cache reads", est.EgressUSD)
			}
		})
	}
}

// TestEstimateSteadyState_ThrottleWarning documents the other half of
// §12.2's point: 1.19M GET/s is not just expensive, it exceeds S3's
// ~5,500 GET/s per-prefix throttle limit by more than two orders of
// magnitude ("it does not work"). This package doesn't model
// throttling directly (Caps has no rate limit field), but the request
// count it produces is exactly the number an operator needs to compare
// against that limit.
func TestEstimateSteadyState_ThrottleWarning(t *testing.T) {
	const s3PrefixLimit = 5500 // GET/s, DESIGN.md §11.1
	rate := RequestRate(1000, 5e9, 4*1024*1024)
	if rate <= s3PrefixLimit*100 {
		t.Fatalf("expected cold 4MiB rate to dwarf the per-prefix throttle limit, got %v vs limit %v", rate, s3PrefixLimit)
	}
}

func TestPolicyAdmit_Block(t *testing.T) {
	p := Policy{Subtree: "/datasets/imagenet", BudgetUSDPerMonth: 1000, OnExceed: "block"}
	est := Estimate{EgressUSD: 4500}

	allow, reason := p.Admit(est)
	if allow {
		t.Fatalf("expected block to refuse admission, reason=%q", reason)
	}
	if reason == "" {
		t.Fatal("expected a non-empty reason")
	}
}

func TestPolicyAdmit_Degrade(t *testing.T) {
	p := Policy{BudgetUSDPerMonth: 1000, OnExceed: "degrade"}
	est := Estimate{EgressUSD: 4500}

	allow, _ := p.Admit(est)
	if allow {
		t.Fatal("expected degrade to also refuse the bulk request as-is")
	}
}

func TestPolicyAdmit_Alert(t *testing.T) {
	p := Policy{BudgetUSDPerMonth: 1000, OnExceed: "alert"}
	est := Estimate{EgressUSD: 4500}

	allow, reason := p.Admit(est)
	if !allow {
		t.Fatal("expected alert to still admit the work")
	}
	if reason == "" {
		t.Fatal("expected alert reason to explain the breach")
	}
}

func TestPolicyAdmit_WithinBudget(t *testing.T) {
	p := Policy{BudgetUSDPerMonth: 5000, OnExceed: "block"}
	est := Estimate{EgressUSD: 4500, TotalUSD: 4500}

	allow, reason := p.Admit(est)
	if !allow {
		t.Fatalf("expected admission within budget, reason=%q", reason)
	}
}

func TestPolicyAdmit_Unbounded(t *testing.T) {
	// Zero fields mean "no budget check" (matches YAML omission).
	p := Policy{}
	est := Estimate{EgressUSD: 1e9, RequestUSD: 1e9, EgressGB: 1e9}

	allow, reason := p.Admit(est)
	if !allow {
		t.Fatalf("expected unbounded policy to admit everything, reason=%q", reason)
	}
}

func TestPolicyAdmit_MaxEgressGBPerDay(t *testing.T) {
	p := Policy{MaxEgressGBPerDay: 2000, OnExceed: "block"}
	est := Estimate{EgressGB: 2001}

	allow, reason := p.Admit(est)
	if allow {
		t.Fatalf("expected daily egress cap breach to block, reason=%q", reason)
	}
}

func TestPolicyAdmit_RequestBudget(t *testing.T) {
	p := Policy{RequestBudgetUSDPerMonth: 500, OnExceed: "block"}
	est := Estimate{RequestUSD: 501}

	allow, _ := p.Admit(est)
	if allow {
		t.Fatal("expected request budget breach to block")
	}
}

func TestPolicyAdmit_UnrecognizedOnExceedFailsClosed(t *testing.T) {
	p := Policy{BudgetUSDPerMonth: 1, OnExceed: "yolo"}
	est := Estimate{EgressUSD: 2}

	allow, _ := p.Admit(est)
	if allow {
		t.Fatal("expected an unrecognized on_exceed value to fail closed (block)")
	}
}

// TestR2ZeroEgress reproduces §12.1's Cloudflare R2 point directly: a
// backend whose EgressUSDPerGB is legitimately 0 must produce a $0
// estimate, not a divide-by-zero or a silent nonzero cost from some
// hardcoded assumption.
func TestR2ZeroEgress(t *testing.T) {
	caps := store.Caps{EgressUSDPerGB: 0, GetUSDPer1k: 0.00036}
	est := EstimateReplication(caps, 1000, 50_000_000_000_000)
	if est.EgressUSD != 0 {
		t.Fatalf("EgressUSD = %v, want 0 for a zero-egress backend", est.EgressUSD)
	}
	if est.TotalUSD != est.RequestUSD {
		t.Fatalf("TotalUSD = %v, want to equal RequestUSD %v when egress is free", est.TotalUSD, est.RequestUSD)
	}
}
