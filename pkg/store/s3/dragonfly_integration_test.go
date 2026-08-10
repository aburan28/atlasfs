package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aburan28/atlasfs/pkg/store"
	"github.com/aburan28/atlasfs/pkg/store/dragonfly"
)

// The end-to-end proof that Dragonfly integration is real: the S3
// backend, unmodified, driven through a forwarding proxy standing in for
// dfdaemon, against the same fake S3 server every other test in this
// package uses. If the wiring were wrong the bytes would still arrive
// (straight from S3), so the assertions deliberately check the *path*,
// not just the payload.

type countingProxy struct {
	requests atomic.Int64
	p2p      atomic.Int64
}

func (c *countingProxy) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.requests.Add(1)
		if r.Header.Get(dragonfly.HeaderUseP2P) == "true" {
			c.p2p.Add(1)
		}
		if !r.URL.IsAbs() {
			http.Error(w, "not a proxied request", http.StatusBadRequest)
			return
		}
		out, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		for k, vs := range r.Header {
			if strings.HasPrefix(k, "X-Dragonfly-") {
				continue
			}
			for _, v := range vs {
				out.Header.Add(k, v)
			}
		}
		// Preserve the declared length: rebuilding the request from an
		// io.ReadCloser leaves ContentLength at -1, which forces chunked
		// encoding and breaks the SDK's signed PUTs.
		out.ContentLength = r.ContentLength
		resp, err := http.DefaultTransport.RoundTrip(out)
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

func TestS3BackendThroughDragonflyProxy(t *testing.T) {
	ctx := context.Background()
	s3srv := newFakeS3("bkt")
	defer s3srv.Close()

	proxy := &countingProxy{}
	proxySrv := proxy.start(t)

	dfClient, err := dragonfly.NewHTTPClient(dragonfly.Config{
		ProxyURL: proxySrv.URL,
		Tag:      "atlas-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// The only change to how the backend is built: hand the AWS config an
	// HTTP client that proxies through dfdaemon. Nothing in pkg/store/s3
	// knows Dragonfly exists.
	cfg := testConfig()
	cfg.HTTPClient = dfClient
	b, err := New(ctx, cfg, Config{Bucket: "bkt"}, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(s3srv.URL)
	})
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte(strings.Repeat("container-payload;", 1000))
	key := "atlas/c/local/ab/cont1"
	if _, err := b.Put(ctx, key, bytes.NewReader(payload), int64(len(payload)), store.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	// A ranged read, the shape AtlasFS actually uses to fetch a chunk out
	// of a container.
	const off, length = 100, 250
	rc, err := b.Get(ctx, key, off, length)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if want := payload[off : off+length]; !bytes.Equal(got, want) {
		t.Fatalf("ranged read through Dragonfly returned wrong bytes (%d of %d)", len(got), len(want))
	}

	if n := proxy.requests.Load(); n == 0 {
		t.Fatal("no S3 traffic went through the Dragonfly proxy — the client was not wired in")
	}
	if proxy.p2p.Load() != proxy.requests.Load() {
		t.Fatalf("only %d of %d proxied requests carried %s",
			proxy.p2p.Load(), proxy.requests.Load(), dragonfly.HeaderUseP2P)
	}
	t.Logf("%d S3 requests traversed dfdaemon, all tagged for P2P", proxy.requests.Load())
}

// TestDragonflyProxyDoesNotBreakCapabilityProbe: the backend probes
// conditional-PUT support at construction (DESIGN.md §24.2). That probe
// is real traffic and must survive being proxied, or a Dragonfly-enabled
// deployment would silently mis-detect its own backend's capabilities.
func TestDragonflyProxyDoesNotBreakCapabilityProbe(t *testing.T) {
	ctx := context.Background()
	s3srv := newFakeS3("bkt")
	defer s3srv.Close()

	proxy := &countingProxy{}
	proxySrv := proxy.start(t)
	dfClient, err := dragonfly.NewHTTPClient(dragonfly.Config{ProxyURL: proxySrv.URL})
	if err != nil {
		t.Fatal(err)
	}

	direct, err := New(ctx, testConfig(), Config{Bucket: "bkt"}, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(s3srv.URL)
	})
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.HTTPClient = dfClient
	proxied, err := New(ctx, cfg, Config{Bucket: "bkt"}, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(s3srv.URL)
	})
	if err != nil {
		t.Fatal(err)
	}

	if direct.Caps().ConditionalPut != proxied.Caps().ConditionalPut {
		t.Fatalf("capability probe disagreed through the proxy: direct=%v proxied=%v",
			direct.Caps().ConditionalPut, proxied.Caps().ConditionalPut)
	}
}
