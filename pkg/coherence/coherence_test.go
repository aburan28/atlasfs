package coherence

import (
	"testing"
	"time"
)

// fakeClock gives tests exact control over lease expiry, the same role
// spec/coherence.qnt's Tick action plays for the model.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestManager() (*Manager, *fakeClock) {
	clk := &fakeClock{now: time.Unix(0, 0)}
	return New(clk, 5*time.Second), clk
}

// --- mirrors invLeaseNeverTrustedPastExpiry ------------------------------

func TestLeaseTrustedWithinWindowThenExpires(t *testing.T) {
	m, clk := newTestManager()

	if _, ok := m.TrustedVersion("h1", "inode:1"); ok {
		t.Fatal("no lease granted yet, should not be trusted")
	}

	v := m.Grant("h1", "inode:1")
	got, ok := m.TrustedVersion("h1", "inode:1")
	if !ok || got != v {
		t.Fatalf("expected trusted version %d, got %d ok=%v", v, got, ok)
	}

	clk.advance(4 * time.Second)
	if got, ok := m.TrustedVersion("h1", "inode:1"); !ok || got != v {
		t.Fatalf("lease should still be valid at 4s of a 5s window: got=%d ok=%v", got, ok)
	}

	clk.advance(2 * time.Second) // total 6s, past the 5s window
	if _, ok := m.TrustedVersion("h1", "inode:1"); ok {
		t.Fatal("lease must not be trusted past its expiry")
	}
}

func TestLeasesArePerHolder(t *testing.T) {
	m, _ := newTestManager()
	m.Grant("h1", "inode:1")
	if _, ok := m.TrustedVersion("h2", "inode:1"); ok {
		t.Fatal("a lease granted to h1 must not be trusted by h2")
	}
}

// --- mirrors invRecallSafety's spirit: a bump is authoritative and ------
// --- push, when subscribed, invalidates immediately ----------------------

func TestBumpInvalidatesSubscribedHolderImmediately(t *testing.T) {
	m, _ := newTestManager()
	m.Grant("h1", "inode:1")

	invalidated := false
	if !m.Subscribe("h1", "inode:1", func() { invalidated = true }) {
		t.Fatal("subscribe should succeed under the registry cap")
	}

	m.Bump("inode:1")
	if !invalidated {
		t.Fatal("expected the subscribed callback to fire on Bump")
	}
}

// This is the load-bearing case: DESIGN.md §10.2 says correctness must
// not depend on push landing. A holder that never subscribed (modeling
// a lost invalidation message) keeps serving its stale-but-unexpired
// lease — that is bounded staleness working as designed, not a bug.
func TestUnsubscribedHolderStaysBoundedStaleUntilExpiry(t *testing.T) {
	m, clk := newTestManager()
	v0 := m.Grant("h1", "inode:1") // no Subscribe call at all

	m.Bump("inode:1") // authoritative version now v0+1; h1 never told

	got, ok := m.TrustedVersion("h1", "inode:1")
	if !ok {
		t.Fatal("an unexpired lease must still be trusted even with no push")
	}
	if got != v0 {
		t.Fatalf("holder should still observe its stale version %d, got %d", v0, got)
	}

	clk.advance(6 * time.Second) // past the 5s window
	if _, ok := m.TrustedVersion("h1", "inode:1"); ok {
		t.Fatal("staleness must still be bounded by lease expiry even without push")
	}
}

func TestRegistryCapRefusesSubscriptionsPastCap(t *testing.T) {
	m, _ := newTestManager()
	for i := 0; i < registryCap; i++ {
		if !m.Subscribe(holderName(i), "inode:1", func() {}) {
			t.Fatalf("subscription %d should succeed, under the cap of %d", i, registryCap)
		}
	}
	if m.Subscribe("one-too-many", "inode:1", func() {}) {
		t.Fatal("subscription past registryCap should be refused")
	}
	// A refused subscriber is not thereby incorrect — it still gets
	// bounded staleness via lease expiry, same as any unsubscribed
	// holder (checked above).
}

func holderName(i int) string {
	return "h" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

// --- mirrors invNegativeCacheSound ---------------------------------------

func TestNegativeCacheInvalidatedByDirBumpAcrossAllHolders(t *testing.T) {
	m, _ := newTestManager()
	m.GrantNegative("h1", "dir:1", "missing.txt")
	m.GrantNegative("h2", "dir:1", "missing.txt")

	if !m.NegativeTrusted("h1", "dir:1", "missing.txt") {
		t.Fatal("fresh negative entry should be trusted")
	}
	if !m.NegativeTrusted("h2", "dir:1", "missing.txt") {
		t.Fatal("fresh negative entry should be trusted for h2 too")
	}

	m.BumpDir("dir:1") // e.g. someone created missing.txt

	if m.NegativeTrusted("h1", "dir:1", "missing.txt") {
		t.Fatal("a dirver bump must invalidate h1's negative entry without a per-entry message")
	}
	if m.NegativeTrusted("h2", "dir:1", "missing.txt") {
		t.Fatal("a dirver bump must invalidate h2's negative entry too — that's the whole point of gating on dirver instead of per-entry push")
	}
}

func TestNegativeCacheUnrelatedDirUnaffected(t *testing.T) {
	m, _ := newTestManager()
	m.GrantNegative("h1", "dir:1", "missing.txt")
	m.BumpDir("dir:2") // a different directory
	if !m.NegativeTrusted("h1", "dir:1", "missing.txt") {
		t.Fatal("bumping an unrelated directory must not invalidate this one's negative entries")
	}
}

func TestNegativeCacheExpiresEvenWithoutBump(t *testing.T) {
	m, clk := newTestManager()
	m.GrantNegative("h1", "dir:1", "missing.txt")
	clk.advance(6 * time.Second)
	if m.NegativeTrusted("h1", "dir:1", "missing.txt") {
		t.Fatal("a negative entry must still expire on its own lease window")
	}
}

// --- a lightweight monotonic-view check (mirrors invMonotonicClientView) -

func TestGrantAfterBumpNeverGoesBackward(t *testing.T) {
	m, _ := newTestManager()
	v0 := m.Grant("h1", "inode:1")
	m.Bump("inode:1")
	v1 := m.Grant("h1", "inode:1")
	if v1 < v0 {
		t.Fatalf("version must never regress: v0=%d v1=%d", v0, v1)
	}
	if v1 == v0 {
		t.Fatalf("a Grant after a Bump should observe the bumped version: v0=%d v1=%d", v0, v1)
	}
}
