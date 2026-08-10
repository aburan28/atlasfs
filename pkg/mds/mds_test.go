package mds

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aburan28/atlasfs/pkg/coherence"
	"github.com/aburan28/atlasfs/pkg/metadb"
)

// These tests run a real gRPC server over a real TCP loopback socket
// with two independent clients. That matters: the properties being
// checked here (a push crossing a process boundary, a mutation blocking
// on a remote acknowledgement) are exactly the ones that cannot be
// observed when everything shares one address space, which is why they
// went unimplemented until this package existed.

type testEnv struct {
	srv    *Server
	db     *metadb.DB
	addr   string
	dialed []*grpc.ClientConn
	t      *testing.T
}

func startServer(t *testing.T, cfg Config) *testEnv {
	t.Helper()
	db, err := metadb.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	cfg.DB = db

	srv := NewServer(cfg)
	g := grpc.NewServer()
	srv.Register(g)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	return &testEnv{srv: srv, db: db, addr: lis.Addr().String(), t: t}
}

func (e *testEnv) client(name string, clock coherence.Clock) *Client {
	e.t.Helper()
	cc, err := grpc.NewClient(e.addr, grpc.WithTransportCredentials(insecure.NewCredentials()), DialOption())
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { cc.Close() })
	c := NewClient(cc, name, clock)
	e.t.Cleanup(c.Close)
	return c
}

func fileRec(size uint64) metadb.InodeRecord {
	return metadb.InodeRecord{Mode: 0o644, Size: size, NLink: 1, MTime: time.Unix(1_700_000_000, 0)}
}

// TestTwoClientsRealRPC is the baseline: the split works at all, and one
// client sees what the other committed.
func TestTwoClientsRealRPC(t *testing.T) {
	env := startServer(t, Config{LeaseDuration: 30 * time.Second})
	ctx := context.Background()

	writer := env.client("writer", nil)
	reader := env.client("reader", nil)

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(11)); err != nil {
		t.Fatal(err)
	}
	resp, err := reader.Lookup(ctx, metadb.RootInode, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Found || resp.Record.Size != 11 {
		t.Fatalf("reader did not observe the writer's commit: %+v", resp)
	}
	if resp.Lease.TTL != 30*time.Second {
		t.Fatalf("expected a 30s lease, got %s", resp.Lease.TTL)
	}
}

// TestPushInvalidationCrossesTheProcessBoundary: the reader caches under
// a lease, the writer mutates, and the reader's cache is dropped by a
// push that actually travelled over the wire — DESIGN.md §10.2.
func TestPushInvalidationCrossesTheProcessBoundary(t *testing.T) {
	env := startServer(t, Config{LeaseDuration: 30 * time.Second})
	ctx := context.Background()

	writer := env.client("writer", nil)
	reader := env.client("reader", nil)
	if err := reader.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(1)); err != nil {
		t.Fatal(err)
	}
	resp, err := reader.Lookup(ctx, metadb.RootInode, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reader.CachedInode(resp.Inode); !ok {
		t.Fatal("reader should have cached the record under its lease")
	}

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(2)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := reader.CachedInode(resp.Inode); !ok {
			return // invalidated, as required
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("reader's cache was never invalidated by the writer's commit")
}

// TestStaleReadIsBoundedByLeaseWhenPushIsLost is the property §10.2
// insists on and that a single-process build could never demonstrate:
// with the push stream never opened, the reader still serves a stale
// entry — but only until its own lease expires, never past it.
func TestStaleReadIsBoundedByLeaseWhenPushIsLost(t *testing.T) {
	env := startServer(t, Config{LeaseDuration: 30 * time.Second})
	ctx := context.Background()

	writer := env.client("writer", nil)
	// Deliberately no Subscribe: this models every push being lost.
	clk := &fakeClock{now: time.Unix(0, 0)}
	reader := env.client("reader", clk)

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(1)); err != nil {
		t.Fatal(err)
	}
	resp, err := reader.Lookup(ctx, metadb.RootInode, "f.txt")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(999)); err != nil {
		t.Fatal(err)
	}

	// Inside the lease window, serving the old size is correct: that is
	// what bounded staleness means, and it is not a bug.
	rec, ok := reader.CachedInode(resp.Inode)
	if !ok || rec.Size != 1 {
		t.Fatalf("expected the stale-but-leased record, got %+v ok=%v", rec, ok)
	}

	// Past the window, the client must refuse its own cache — with no
	// message from anyone, on its own clock alone (§10.7).
	clk.advance(31 * time.Second)
	if _, ok := reader.CachedInode(resp.Inode); ok {
		t.Fatal("cache was trusted past lease expiry — G1's bound is what makes push droppable")
	}
	fresh, err := reader.GetInode(ctx, resp.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Size != 999 {
		t.Fatalf("after expiry the client should see the new value, got size=%d", fresh.Size)
	}
}

// TestPosixCommitBlocksUntilRemoteHolderAcks is the headline: DESIGN.md
// §10.6's blocking recall, end to end, between two processes. The
// writer's Commit must not return until the reader — a different client,
// over a real socket — has dropped its cached copy and acknowledged.
func TestPosixCommitBlocksUntilRemoteHolderAcks(t *testing.T) {
	env := startServer(t, Config{
		LeaseDuration:  time.Hour, // posix leases are revocation-bounded, not time-bounded
		Posix:          true,
		RecallDeadline: 10 * time.Second,
	})
	ctx := context.Background()

	writer := env.client("writer", nil)
	reader := env.client("reader", nil)
	if err := reader.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(1)); err != nil {
		t.Fatal(err)
	}
	resp, err := reader.Lookup(ctx, metadb.RootInode, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reader.CachedInode(resp.Inode); !ok {
		t.Fatal("reader should hold a cached copy before the recall")
	}

	// Hold the ack back until we have confirmed the commit is blocked.
	release := make(chan struct{})
	recalled := make(chan struct{})
	reader.SetRecallHook(func(string) {
		close(recalled)
		<-release
	})

	committed := make(chan error, 1)
	go func() {
		_, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(2))
		committed <- err
	}()

	select {
	case <-recalled:
	case <-time.After(5 * time.Second):
		t.Fatal("the recall never reached the remote holder")
	}
	select {
	case err := <-committed:
		t.Fatalf("posix commit returned before the holder acked (err=%v) — §10.6 requires it to block", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-committed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("posix commit never completed after the holder acked")
	}
}

// TestPosixCommitProceedsWhenHolderNeverAcks is the liveness half at the
// RPC layer: a wedged client must not be able to block every writer
// forever. The commit completes on the DRECALL deadline and says
// explicitly that it did so without confirmation.
func TestPosixCommitProceedsWhenHolderNeverAcks(t *testing.T) {
	env := startServer(t, Config{
		LeaseDuration:  time.Hour,
		Posix:          true,
		RecallDeadline: 300 * time.Millisecond,
	})
	ctx := context.Background()

	writer := env.client("writer", nil)
	reader := env.client("reader", nil)
	if err := reader.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Lookup(ctx, metadb.RootInode, "f.txt"); err != nil {
		t.Fatal(err)
	}

	wedged := make(chan struct{})
	reader.SetRecallHook(func(string) { <-wedged }) // never returns, so never acks
	defer close(wedged)

	start := time.Now()
	resp, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(2))
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("commit returned in %s, before DRECALL elapsed", elapsed)
	}
	if len(resp.TimedOutHolders) != 1 || resp.TimedOutHolders[0] != "reader" {
		t.Fatalf("expected the wedged holder reported as timed out, got %+v", resp)
	}
	if len(resp.RecalledHolders) != 0 {
		t.Fatalf("expected no acks, got %+v", resp.RecalledHolders)
	}
}

// TestPosixRecallRevokesLeaseEvenWithoutAck: the safety guarantee that
// makes the timeout above acceptable. Even though the reader never
// acknowledged, its lease is gone, so it cannot keep answering reads
// from the copy it was told to drop.
func TestPosixRecallRevokesLeaseEvenWithoutAck(t *testing.T) {
	env := startServer(t, Config{
		LeaseDuration:  time.Hour,
		Posix:          true,
		RecallDeadline: 200 * time.Millisecond,
	})
	ctx := context.Background()

	writer := env.client("writer", nil)
	reader := env.client("reader", nil)
	if err := reader.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(1)); err != nil {
		t.Fatal(err)
	}
	resp, err := reader.Lookup(ctx, metadb.RootInode, "f.txt")
	if err != nil {
		t.Fatal(err)
	}

	wedged := make(chan struct{})
	reader.SetRecallHook(func(string) { <-wedged })
	defer close(wedged)

	if _, err := writer.Commit(ctx, metadb.RootInode, "f.txt", fileRec(2)); err != nil {
		t.Fatal(err)
	}

	// Server-side, the reader must no longer hold a valid lease, even
	// though it never answered.
	if _, ok := env.srv.coh.TrustedVersion("reader", inodeObj(resp.Inode)); ok {
		t.Fatal("un-acked holder still holds a server-side lease after recall")
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }
