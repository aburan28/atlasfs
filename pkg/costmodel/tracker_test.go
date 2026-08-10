package costmodel

import (
	"sync"
	"testing"
)

func TestCostTracker_AccumulatesPerKey(t *testing.T) {
	tr := NewCostTracker()

	tr.AddEgress(EgressKey{Subtree: "/datasets/imagenet", SrcRegion: "us-west-2", DstRegion: "europe-west4"}, 100)
	tr.AddEgress(EgressKey{Subtree: "/datasets/imagenet", SrcRegion: "us-west-2", DstRegion: "europe-west4"}, 50)
	tr.AddEgress(EgressKey{Subtree: "/datasets/w3", SrcRegion: "us-west-2", DstRegion: "europe-west4"}, 9)

	totals := tr.EgressTotals()
	got := totals[EgressKey{Subtree: "/datasets/imagenet", SrcRegion: "us-west-2", DstRegion: "europe-west4"}]
	if got != 150 {
		t.Fatalf("accumulated egress = %v, want 150", got)
	}
	if len(totals) != 2 {
		t.Fatalf("expected 2 distinct egress keys, got %d", len(totals))
	}

	tr.AddRequests(RequestKey{Subtree: "/datasets/imagenet", Region: "us-west-2", Op: "get"}, 1000, 0.4)
	tr.AddRequests(RequestKey{Subtree: "/datasets/imagenet", Region: "us-west-2", Op: "get"}, 500, 0.2)
	tr.AddRequests(RequestKey{Subtree: "/datasets/imagenet", Region: "us-west-2", Op: "put"}, 10, 0.01)

	reqTotals := tr.RequestTotals()
	getTot := reqTotals[RequestKey{Subtree: "/datasets/imagenet", Region: "us-west-2", Op: "get"}]
	if getTot.Count != 1500 {
		t.Fatalf("get count = %d, want 1500", getTot.Count)
	}
	approxEqual(t, getTot.USD, 0.6, 1e-9, "get USD")

	if len(reqTotals) != 2 {
		t.Fatalf("expected 2 distinct request keys (get, put), got %d", len(reqTotals))
	}
}

func TestCostTracker_SubtreeTotalUSD(t *testing.T) {
	tr := NewCostTracker()
	tr.AddEgress(EgressKey{Subtree: "/a", SrcRegion: "x", DstRegion: "y"}, 100)
	tr.AddRequests(RequestKey{Subtree: "/a", Region: "x", Op: "get"}, 100, 25)
	tr.AddEgress(EgressKey{Subtree: "/b", SrcRegion: "x", DstRegion: "y"}, 5000)

	if got, want := tr.SubtreeTotalUSD("/a"), 125.0; got != want {
		t.Fatalf("SubtreeTotalUSD(/a) = %v, want %v", got, want)
	}
	if got, want := tr.SubtreeTotalUSD("/b"), 5000.0; got != want {
		t.Fatalf("SubtreeTotalUSD(/b) = %v, want %v", got, want)
	}
	if got := tr.SubtreeTotalUSD("/nonexistent"); got != 0 {
		t.Fatalf("SubtreeTotalUSD(/nonexistent) = %v, want 0", got)
	}
}

// TestCostTracker_SnapshotIsolation checks EgressTotals/RequestTotals
// return copies: mutating the returned map must not corrupt the
// tracker's internal state for a later read.
func TestCostTracker_SnapshotIsolation(t *testing.T) {
	tr := NewCostTracker()
	key := EgressKey{Subtree: "/a", SrcRegion: "x", DstRegion: "y"}
	tr.AddEgress(key, 10)

	snap := tr.EgressTotals()
	snap[key] = 99999

	if got := tr.EgressTotals()[key]; got != 10 {
		t.Fatalf("tracker state mutated via returned snapshot: got %v, want 10", got)
	}
}

// TestCostTracker_ConcurrentUse exercises the documented thread-safety
// contract under -race.
func TestCostTracker_ConcurrentUse(t *testing.T) {
	tr := NewCostTracker()
	key := EgressKey{Subtree: "/a", SrcRegion: "x", DstRegion: "y"}
	rkey := RequestKey{Subtree: "/a", Region: "x", Op: "get"}

	var wg sync.WaitGroup
	const n = 100
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			tr.AddEgress(key, 1)
		}()
		go func() {
			defer wg.Done()
			tr.AddRequests(rkey, 1, 1)
		}()
	}
	wg.Wait()

	if got := tr.EgressTotals()[key]; got != n {
		t.Fatalf("egress total after concurrent adds = %v, want %d", got, n)
	}
	if got := tr.RequestTotals()[rkey].Count; got != n {
		t.Fatalf("request count after concurrent adds = %v, want %d", got, n)
	}
}
