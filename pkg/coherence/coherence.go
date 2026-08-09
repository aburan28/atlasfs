// Package coherence is the Go implementation of DESIGN.md §10, the
// metadata cache coherence protocol modeled in spec/coherence.qnt.
// Same state shape as the model (per-holder leases, directory versions,
// negative entries), same central guarantee: correctness derives from
// lease expiry alone, on the holder's own clock (§10.7) — push
// invalidation only shortens the staleness window, and losing every
// invalidation message violates no guarantee (§10.2).
//
// Scope cut, stated plainly: this package implements the `relaxed`/
// `session` mechanics (leases, negative caching, best-effort push). It
// does not implement §10.6's blocking recall for the `posix` class —
// that needs a live RPC round trip to a remote holder, and this build
// has no remote holders yet (metadb is a single-process embedded store,
// so every consumer of a Manager in this codebase is in-process). The
// registry/push machinery below is written and tested generically
// against multiple holder IDs anyway, the same way spec/coherence.qnt
// checks it against symbolic clients "c1"/"c2" — so it's ready for a
// real second holder (a second process against a shared metadb) without
// changing this package, even though this build doesn't have one yet.
package coherence

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can control lease expiry deterministically
// — the Go analog of spec/coherence.qnt advancing a logical clock via
// Tick instead of depending on wall time.
type Clock interface{ Now() time.Time }

// RealClock is the production Clock, backed by time.Now (which on every
// supported Go platform is monotonic-safe for the subtractions this
// package does — see the time package's monotonic-reading docs).
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// registryCap bounds the best-effort push-invalidation registry per
// object (DESIGN.md §10.2: "a capped LRU per object ... overflow marks
// the object BROADCAST"). Past the cap, new subscriptions are refused —
// Subscribe returns false — and that holder relies on lease expiry
// alone, which remains fully correct, just less responsive.
const registryCap = 64

type leaseEntry struct {
	expiry  time.Time
	version uint64
}

type negEntry struct {
	dirverSeen uint64
	expiry     time.Time
}

// Manager tracks lease and negative-cache state for one consistency
// class (one lease duration). A repo with multiple classes uses one
// Manager per class.
type Manager struct {
	mu    sync.Mutex
	clock Clock
	d     time.Duration

	// leases[obj][holder] — an object's per-holder trust windows.
	leases map[string]map[string]leaseEntry
	// subscribers[obj][holder] — best-effort invalidation callbacks,
	// capped at registryCap entries per object.
	subscribers map[string]map[string]func()
	// versions[obj] — the authoritative version, advanced only by Bump.
	versions map[string]uint64

	dirver map[string]uint64
	// neg[dir][holder+"\x00"+name] — negative entries, invalidated in
	// bulk by a dirver bump rather than individually.
	neg map[string]map[string]negEntry
}

// New returns a Manager granting leases of duration d, using clock for
// all expiry decisions.
func New(clock Clock, d time.Duration) *Manager {
	return &Manager{
		clock:       clock,
		d:           d,
		leases:      map[string]map[string]leaseEntry{},
		subscribers: map[string]map[string]func(){},
		dirver:      map[string]uint64{},
		neg:         map[string]map[string]negEntry{},
	}
}

// TrustedVersion reports the version holder last observed for obj, and
// whether that observation is still inside its lease window. A true
// result may still be carrying a version that is behind the current
// authoritative one — bounded staleness is the point, not an error.
func (m *Manager) TrustedVersion(holder, obj string) (version uint64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, found := m.leases[obj][holder]
	if !found {
		return 0, false
	}
	if m.clock.Now().After(e.expiry) {
		return 0, false
	}
	return e.version, true
}

// Grant records that holder just observed obj at its current version,
// valid for this Manager's lease duration measured from now — DESIGN.md
// §10.2's "D seconds after the instant it sent the request",
// approximated here by "now" since there is no network hop to account
// for in-process. Returns the granted version. The Manager is the sole
// authority for an object's version counter (advanced only by Bump) —
// callers never supply one, the same way nothing external hands BumpDir
// a directory version either.
func (m *Manager) Grant(holder, obj string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.versionLocked(obj)
	if m.leases[obj] == nil {
		m.leases[obj] = map[string]leaseEntry{}
	}
	m.leases[obj][holder] = leaseEntry{expiry: m.clock.Now().Add(m.d), version: v}
	return v
}

// Subscribe registers a best-effort invalidation callback for holder on
// obj. Returns false, refusing the subscription, once obj's registry is
// at registryCap — the holder is still correct, just relies on lease
// expiry alone rather than a proactive push (DESIGN.md §10.2).
func (m *Manager) Subscribe(holder, obj string, onInvalidate func()) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	subs := m.subscribers[obj]
	if subs == nil {
		subs = map[string]func(){}
		m.subscribers[obj] = subs
	}
	if _, exists := subs[holder]; !exists && len(subs) >= registryCap {
		return false
	}
	subs[holder] = onInvalidate
	return true
}

// Bump advances obj's version and fires every registered subscriber's
// callback — best-effort, synchronous, in-process delivery here, which
// is as reliable as push invalidation ever gets to be; a remote holder
// over a real network could still miss it, and correctness must not
// (and does not) depend on that firing at all. Returns the new version.
func (m *Manager) Bump(obj string) uint64 {
	m.mu.Lock()
	subs := m.subscribers[obj]
	callbacks := make([]func(), 0, len(subs))
	for _, cb := range subs {
		callbacks = append(callbacks, cb)
	}
	m.mu.Unlock()

	for _, cb := range callbacks {
		cb()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.versionLocked(obj) + 1
	m.setVersionLocked(obj, v)
	return v
}

// versionLocked/setVersionLocked track the authoritative version as the
// max Grant'd or Bump'd value seen for obj — Manager doesn't otherwise
// need a separate "authoritative store"; the caller (repo) is the
// authority and calls Bump exactly when it mutates.
func (m *Manager) versionLocked(obj string) uint64 {
	return m.versions[obj]
}
func (m *Manager) setVersionLocked(obj string, v uint64) {
	if m.versions == nil {
		m.versions = map[string]uint64{}
	}
	m.versions[obj] = v
}

// --- negative caching (DESIGN.md §10.4) ------------------------------

// DirVersion returns dir's current version.
func (m *Manager) DirVersion(dir string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirver[dir]
}

// BumpDir advances dir's version, invalidating every negative entry
// recorded against it in one step — no per-entry messages needed,
// which is what makes negative caching affordable at all (§10.4).
func (m *Manager) BumpDir(dir string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirver[dir]++
	return m.dirver[dir]
}

func negKey(holder, name string) string { return holder + "\x00" + name }

// NegativeTrusted reports whether holder's cached "name does not exist
// in dir" is still valid: the dirver it was granted against must still
// match the live one, and the entry must not have separately expired.
func (m *Manager) NegativeTrusted(holder, dir, name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, found := m.neg[dir][negKey(holder, name)]
	if !found {
		return false
	}
	if e.dirverSeen != m.dirver[dir] {
		return false
	}
	return !m.clock.Now().After(e.expiry)
}

// GrantNegative records that holder looked up name in dir and found
// nothing, valid until the next dirver bump or lease expiry, whichever
// comes first.
func (m *Manager) GrantNegative(holder, dir, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.neg[dir] == nil {
		m.neg[dir] = map[string]negEntry{}
	}
	m.neg[dir][negKey(holder, name)] = negEntry{dirverSeen: m.dirver[dir], expiry: m.clock.Now().Add(m.d)}
}
