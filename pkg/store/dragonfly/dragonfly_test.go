package dragonfly

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

var timeZero = time.Unix(0, 0)

// fakeDfdaemon is a real forwarding HTTP proxy standing in for
// dfdaemon. It is not a mock: it accepts the absolute-URI requests an
// HTTP client sends to a proxy, forwards them to the origin, and returns
// the response — which is what makes it able to prove that AtlasFS's
// traffic genuinely goes through the proxy rather than around it, and to
// capture the headers Dragonfly would route on.
type fakeDfdaemon struct {
	mu       sync.Mutex
	requests []recordedRequest
	// originHits counts requests actually forwarded upstream, standing in
	// for Dragonfly's back-to-source path. In a real cluster this is the
	// number that P2P drives toward one-per-cluster instead of
	// one-per-node.
	originHits int
}

type recordedRequest struct {
	Method    string
	URL       string
	UseP2P    string
	Prefetch  string
	Tag       string
	RangeHdr  string
	AbsoluteN bool
}

func (d *fakeDfdaemon) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		d.requests = append(d.requests, recordedRequest{
			Method:   r.Method,
			URL:      r.URL.String(),
			UseP2P:   r.Header.Get(HeaderUseP2P),
			Prefetch: r.Header.Get(HeaderPrefetch),
			Tag:      r.Header.Get(HeaderTag),
			RangeHdr: r.Header.Get("Range"),
			// A client talking to a proxy sends the full absolute URI on
			// the request line; a client talking directly sends a path.
			// This is the difference that proves the proxy is in the path.
			AbsoluteN: r.URL.IsAbs(),
		})
		d.originHits++
		d.mu.Unlock()

		// Forward upstream, exactly as dfdaemon does on a cache miss.
		outReq, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		for k, vs := range r.Header {
			if strings.HasPrefix(k, "X-Dragonfly-") {
				continue // consumed by the proxy, not sent upstream
			}
			for _, v := range vs {
				outReq.Header.Add(k, v)
			}
		}
		resp, err := http.DefaultTransport.RoundTrip(outReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (d *fakeDfdaemon) snapshot() ([]recordedRequest, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]recordedRequest, len(d.requests))
	copy(out, d.requests)
	return out, d.originHits
}

// TestRequestsActuallyTraverseTheProxy is the load-bearing test: without
// it, a broken proxy configuration would look identical to a working one
// because the content still arrives (directly). The absolute-URI check
// is what distinguishes "went through dfdaemon" from "went straight to
// the origin".
func TestRequestsActuallyTraverseTheProxy(t *testing.T) {
	content := strings.Repeat("atlasfs-container-bytes;", 500)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "obj", timeZero, strings.NewReader(content))
	}))
	defer origin.Close()

	df := &fakeDfdaemon{}
	proxy := df.start(t)

	client, err := NewHTTPClient(Config{ProxyURL: proxy.URL, Tag: "atlas-region-local"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(origin.URL + "/atlas/c/local/ab/container1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch through the proxy: got %d bytes, want %d", len(got), len(content))
	}

	reqs, _ := df.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("proxy saw %d requests, want 1 — traffic bypassed dfdaemon", len(reqs))
	}
	r := reqs[0]
	if !r.AbsoluteN {
		t.Fatal("request did not arrive as an absolute URI; the client was not using the proxy")
	}
	if r.UseP2P != "true" {
		t.Fatalf("%s = %q, want \"true\"", HeaderUseP2P, r.UseP2P)
	}
	if r.Prefetch != "true" {
		t.Fatalf("%s = %q, want \"true\" by default", HeaderPrefetch, r.Prefetch)
	}
	if r.Tag != "atlas-region-local" {
		t.Fatalf("%s = %q, want the configured tag", HeaderTag, r.Tag)
	}
}

// TestRangeRequestsSurviveTheProxy: AtlasFS reads a container by ranges
// (one per chunk), so a proxy that dropped or mangled Range would return
// whole containers where chunks were asked for — correct-looking bytes,
// catastrophically wrong offsets.
func TestRangeRequestsSurviveTheProxy(t *testing.T) {
	content := "0123456789abcdefghijklmnopqrstuvwxyz"
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "obj", timeZero, strings.NewReader(content))
	}))
	defer origin.Close()

	df := &fakeDfdaemon{}
	proxy := df.start(t)
	client, err := NewHTTPClient(Config{ProxyURL: proxy.URL})
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodGet, origin.URL+"/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=10-19")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206 Partial Content", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := content[10:20]; string(got) != want {
		t.Fatalf("range read got %q, want %q", got, want)
	}

	reqs, _ := df.snapshot()
	if len(reqs) != 1 || reqs[0].RangeHdr != "bytes=10-19" {
		t.Fatalf("proxy did not see the Range header intact: %+v", reqs)
	}
}

// TestPrefetchCanBeDisabled: whole-task prefetch is right for AtlasFS's
// many-ranges-per-container pattern, but a deployment reading a few
// scattered bytes out of very large containers would be pulling far more
// than it needs, so the default has to be overridable.
func TestPrefetchCanBeDisabled(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("x"))
	}))
	defer origin.Close()

	df := &fakeDfdaemon{}
	proxy := df.start(t)
	off := false
	client, err := NewHTTPClient(Config{ProxyURL: proxy.URL, Prefetch: &off})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(origin.URL + "/obj")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	reqs, _ := df.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("want 1 request, got %d", len(reqs))
	}
	if reqs[0].Prefetch != "" {
		t.Fatalf("%s = %q, want it unset when Prefetch is false", HeaderPrefetch, reqs[0].Prefetch)
	}
	if reqs[0].UseP2P != "true" {
		t.Fatal("disabling prefetch must not disable P2P")
	}
}

func TestBadProxyURLIsRejected(t *testing.T) {
	for _, bad := range []string{"://nope", "not-a-url"} {
		if _, err := NewHTTPClient(Config{ProxyURL: bad}); err == nil {
			t.Fatalf("expected %q to be rejected", bad)
		}
	}
}

func TestDefaultProxyURLIsUsedWhenUnset(t *testing.T) {
	// Not a network test: just that an empty config resolves to the
	// documented dfdaemon default rather than erroring.
	if _, err := NewHTTPClient(Config{}); err != nil {
		t.Fatalf("empty config should default to %s, got %v", DefaultProxyURL, err)
	}
}

// TestTransportDoesNotMutateTheCallersRequest guards a subtle rule the
// AWS SDK depends on: RoundTrippers must not modify the request they are
// given, because the SDK reuses it across retries and signs it. Stamping
// headers onto the original would corrupt a retried, already-signed
// request.
func TestTransportDoesNotMutateTheCallersRequest(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer origin.Close()
	df := &fakeDfdaemon{}
	proxy := df.start(t)

	rt, err := NewTransport(Config{ProxyURL: proxy.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, origin.URL+"/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if got := req.Header.Get(HeaderUseP2P); got != "" {
		t.Fatalf("RoundTrip mutated the caller's request (%s=%q)", HeaderUseP2P, got)
	}
}
