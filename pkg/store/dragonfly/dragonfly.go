// Package dragonfly routes AtlasFS container reads through a Dragonfly
// (d7y.io) peer, turning DESIGN.md §11.1's "P2P chunk exchange" from a
// design bullet into a deployment option without AtlasFS implementing a
// peer protocol of its own.
//
// # Why P2P is a cost feature here, not a speed feature
//
// DESIGN.md §12.2 is blunt that request charges, not bandwidth, are the
// binding constraint: the worked example there puts a cold read fleet at
// 1.19M GET/s and $1,717/hour in S3 request charges alone. Every one of
// those GETs is for an immutable, content-addressed container object,
// and in a fleet of N nodes training on the same dataset, N-1 of them
// are asking the origin for bytes a neighbour already has.
//
// Dragonfly's model fits that exactly: the first peer to want a
// container fetches it back-to-source, and every other peer gets it from
// the P2P mesh. Origin GETs collapse from once-per-node to once-per-
// cluster, and cross-AZ/region egress collapses with them. Nothing about
// correctness changes, because a container is immutable and named by
// content — the property that makes a cache safe with no invalidation
// protocol at all.
//
// # How it is wired
//
// dfdaemon exposes an HTTP proxy (default :4001). Rather than teach
// AtlasFS a new backend protocol, this package hands back an
// *http.Client whose transport proxies through dfdaemon and stamps the
// headers Dragonfly uses to make routing decisions. That client is then
// given to the S3 backend (or any HTTP-based backend), so container GETs
// flow through the mesh while the backend code is untouched.
//
// Two headers carry the policy:
//
//   - X-Dragonfly-Use-P2P: opts a request into P2P regardless of whether
//     it matches a proxy rule, so AtlasFS does not depend on cluster-side
//     regex configuration matching its bucket layout.
//   - X-Dragonfly-Prefetch: on a range request, tells Dragonfly to fetch
//     the whole task rather than just the range. That is the right
//     default here and it is worth being explicit about why: AtlasFS
//     reads a container by many small ranges (one per chunk), so
//     prefetching the container once turns what would be dozens of
//     independent P2P tasks into one, and every subsequent chunk read
//     hits the local peer's cache.
//
// # What this package does not do
//
// It does not talk to the Manager or Scheduler, does not register
// AtlasFS as a Seed Peer, and does not implement dfget. Those are
// cluster-side concerns; from AtlasFS's side a Dragonfly deployment is
// "an HTTP proxy that makes GETs cheaper", which is the whole point of
// integrating with a mature P2P system instead of writing one.
package dragonfly

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Header names Dragonfly's proxy understands.
const (
	HeaderUseP2P    = "X-Dragonfly-Use-P2P"
	HeaderPrefetch  = "X-Dragonfly-Prefetch"
	HeaderTag       = "X-Dragonfly-Tag"
	HeaderRegistry  = "X-Dragonfly-Registry"
	DefaultProxyURL = "http://127.0.0.1:4001"
)

// Config selects a dfdaemon and the policy applied to requests through
// it.
type Config struct {
	// ProxyURL is the dfdaemon proxy address. Empty means
	// DefaultProxyURL.
	ProxyURL string

	// Prefetch sets X-Dragonfly-Prefetch. Default true — see the package
	// doc on why whole-container prefetch is the right shape for
	// AtlasFS's many-small-ranges access pattern.
	Prefetch *bool

	// Tag sets X-Dragonfly-Tag, which separates otherwise-identical URLs
	// into distinct P2P tasks. Useful when one bucket serves several
	// AtlasFS regions and you do not want them sharing a task.
	Tag string

	// Timeout bounds a single request through the proxy. Zero means no
	// client-side timeout, deferring entirely to the caller's context.
	Timeout time.Duration
}

func (c Config) proxyURL() (*url.URL, error) {
	raw := c.ProxyURL
	if raw == "" {
		raw = DefaultProxyURL
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("dragonfly: bad proxy URL %q: %w", raw, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("dragonfly: proxy URL %q has no host", raw)
	}
	return u, nil
}

func (c Config) prefetch() bool {
	if c.Prefetch == nil {
		return true
	}
	return *c.Prefetch
}

// transport proxies through dfdaemon and stamps Dragonfly's headers on
// every request.
type transport struct {
	base   http.RoundTripper
	cfg    Config
	header http.Header
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone before mutating: RoundTrippers are explicitly forbidden from
	// modifying the request they are handed, and the AWS SDK reuses
	// requests across retries.
	r := req.Clone(req.Context())
	for k, vs := range t.header {
		for _, v := range vs {
			r.Header.Set(k, v)
		}
	}
	return t.base.RoundTrip(r)
}

// NewTransport returns an http.RoundTripper that sends requests through
// the configured dfdaemon proxy with Dragonfly's headers applied. base
// may be nil, in which case a clone of http.DefaultTransport is used.
func NewTransport(cfg Config, base *http.Transport) (http.RoundTripper, error) {
	proxy, err := cfg.proxyURL()
	if err != nil {
		return nil, err
	}
	if base == nil {
		base = http.DefaultTransport.(*http.Transport).Clone()
	} else {
		base = base.Clone()
	}
	base.Proxy = http.ProxyURL(proxy)

	h := http.Header{}
	h.Set(HeaderUseP2P, "true")
	if cfg.prefetch() {
		h.Set(HeaderPrefetch, "true")
	}
	if cfg.Tag != "" {
		h.Set(HeaderTag, cfg.Tag)
	}
	return &transport{base: base, cfg: cfg, header: h}, nil
}

// NewHTTPClient returns an http.Client routing through dfdaemon. Pass it
// to a backend that accepts one — pkg/store/s3's Config does — and its
// object GETs become P2P-accelerated with no other change.
func NewHTTPClient(cfg Config) (*http.Client, error) {
	rt, err := NewTransport(cfg, nil)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: rt, Timeout: cfg.Timeout}, nil
}
