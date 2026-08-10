package mdsfuse

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// The end-to-end tests: one authority, two independent FUSE mounts, and
// the §10 coherence protocol running between them over a real socket.
//
// This is the shape the whole design is about and the one nothing could
// exercise until now — a write on one mountpoint becoming an
// invalidation, or a blocking recall, on another.

type cluster struct {
	addr    string
	db      *metadb.DB
	backend *local.Backend
	stop    func()
}

// startCluster brings up an authority over a shared repo directory.
func startCluster(t *testing.T, cfg mds.Config) *cluster {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}
	repoDir := t.TempDir()

	// A repo has to exist first so metadb has a root inode.
	r, err := repo.OpenWithClass(repoDir, mustLocal(t, filepath.Join(repoDir, "objects")), repo.DefaultRegion, repo.ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()

	db, err := metadb.Open(filepath.Join(repoDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	cfg.DB = db
	srv := mds.NewServer(cfg)
	g := grpc.NewServer()
	srv.Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	return &cluster{
		addr:    lis.Addr().String(),
		db:      db,
		backend: mustLocal(t, filepath.Join(repoDir, "objects")),
		stop:    g.Stop,
	}
}

// unmountAndWait tears a test mount down without being able to hang the
// suite. Unmount() then <-errCh deadlocks permanently if the unmount
// fails, because Mount() only returns once the server is down — and a
// mountpoint the kernel still holds busy is exactly when that happens.
func unmountAndWait(t *testing.T, server *fuse.Server, errCh <-chan error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := server.Unmount()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("could not unmount the test mount: %v (leaving it mounted rather than blocking the suite)", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case <-errCh:
	case <-time.After(15 * time.Second):
		t.Errorf("Mount did not return after a successful unmount")
	}
}

func mustLocal(t *testing.T, dir string) *local.Backend {
	t.Helper()
	b, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// mountAt attaches an independent mount — its own gRPC connection, its
// own holder identity, its own cache — to the cluster. Two of these are
// two genuinely separate lease holders, which is the precondition for
// any of the coherence assertions below to mean anything.
func (c *cluster) mountAt(t *testing.T, holder string, ttl time.Duration) string {
	t.Helper()
	cc, err := grpc.NewClient(c.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), mds.DialOption())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	client := mds.NewClient(cc, holder, nil)
	if err := client.Subscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	mountpoint := t.TempDir()
	mounted := make(chan *fuse.Server, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Mount(context.Background(), Config{
			Client:         client,
			Backend:        c.backend,
			ChunkSize:      4096,
			KernelCacheTTL: ttl,
		}, mountpoint, func(s *fuse.Server) { mounted <- s })
	}()
	select {
	case server := <-mounted:
		t.Cleanup(func() { unmountAndWait(t, server, errCh) })
	case err := <-errCh:
		t.Fatalf("mount %s failed: %v", holder, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out mounting %s", holder)
	}
	return mountpoint
}

// TestWriteThroughAuthorityRoundTrips is the basic write path: content
// is chunked and uploaded to object storage by the client, its locators
// registered with the authority, and the inode committed — then read
// back through the kernel.
func TestWriteThroughAuthorityRoundTrips(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	small := []byte("written through the authority")
	if err := os.WriteFile(filepath.Join(mnt, "a.txt"), small, 0o644); err != nil {
		t.Fatalf("write through the mds-backed mount: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mnt, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, small) {
		t.Fatalf("read back %q, want %q", got, small)
	}

	// Multi-chunk, so the manifest path and several locator registrations
	// are exercised rather than just the inline case.
	big := bytes.Repeat([]byte("multi-chunk-payload;"), 3000)
	if err := os.WriteFile(filepath.Join(mnt, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	gotBig, err := os.ReadFile(filepath.Join(mnt, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBig, big) {
		t.Fatalf("multi-chunk round trip mismatch: %d vs %d bytes", len(gotBig), len(big))
	}

	// Directories and removal.
	if err := os.Mkdir(filepath.Join(mnt, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "d", "inner.txt"), []byte("inner"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mnt, "d")); err == nil {
		t.Fatal("rmdir of a non-empty directory should fail")
	}
	if err := os.Remove(filepath.Join(mnt, "d", "inner.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mnt, "d")); err != nil {
		t.Fatalf("rmdir of an emptied directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(mnt, "d")); !os.IsNotExist(err) {
		t.Fatalf("directory should be gone, got %v", err)
	}
}

// TestWriteOnOneMountBecomesVisibleOnAnother is the coherence payoff:
// two mounts, one authority, a write on the first becoming visible on
// the second through the push invalidation that drops the second
// holder's cached record.
//
// It asserts on the file's *size* via Stat, not on its content via
// ReadFile, and that distinction is the whole test. A ReadFile goes
// through LOOKUP, and Client.Lookup always round-trips to the authority
// and refreshes the cache — so a content-based assertion passes whether
// or not invalidation works, which was exactly the flaw in this test's
// first draft (caught by disabling push and watching it still pass).
// Stat goes through GETATTR to Client.GetInode, which *is* served from
// the lease cache, so it can only observe the new value if that cache
// was invalidated.
//
// Both mounts run with a zero kernel TTL so Linux's own attr cache
// cannot answer; what is under test is AtlasFS's protocol.
func TestWriteOnOneMountBecomesVisibleOnAnother(t *testing.T) {
	// A long lease is deliberate: if push did not work, B would be
	// entitled to serve the stale size for a full minute, so the 5s
	// deadline below can only be met by an invalidation.
	c := startCluster(t, mds.Config{LeaseDuration: time.Minute})
	a := c.mountAt(t, "mount-a", 0)
	b := c.mountAt(t, "mount-b", 0)

	v1 := []byte("v1")
	if err := os.WriteFile(filepath.Join(a, "shared.txt"), v1, 0o644); err != nil {
		t.Fatal(err)
	}

	// B stats it, taking and caching a lease on the inode.
	st, err := os.Stat(filepath.Join(b, "shared.txt"))
	if err != nil {
		t.Fatalf("mount B could not see mount A's create: %v", err)
	}
	if st.Size() != int64(len(v1)) {
		t.Fatalf("mount B sees size %d, want %d", st.Size(), len(v1))
	}

	// A overwrites with a clearly different length. B holds a cached
	// record for the old one.
	v2 := []byte("v2-substantially-longer-content")
	if err := os.WriteFile(filepath.Join(a, "shared.txt"), v2, 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := os.Stat(filepath.Join(b, "shared.txt"))
		if err == nil && st.Size() == int64(len(v2)) {
			// And the content agrees, so the invalidation was not merely
			// an attr refresh over stale bytes.
			got, err := os.ReadFile(filepath.Join(b, "shared.txt"))
			if err != nil || !bytes.Equal(got, v2) {
				t.Fatalf("size updated but content did not: %q %v", got, err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mount B never observed mount A's overwrite; still sees size %d, want %d "+
				"(the push invalidation did not reach it)", st.Size(), len(v2))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestUnlinkOnOneMountBecomesVisibleOnAnother: the negative direction,
// which exercises §10.4's directory-version invalidation rather than the
// per-inode push.
func TestUnlinkOnOneMountBecomesVisibleOnAnother(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	a := c.mountAt(t, "mount-a", 0)
	b := c.mountAt(t, "mount-b", 0)

	if err := os.WriteFile(filepath.Join(a, "doomed.txt"), []byte("here"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(b, "doomed.txt")); err != nil {
		t.Fatalf("mount B should see the file first: %v", err)
	}
	if err := os.Remove(filepath.Join(a, "doomed.txt")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(b, "doomed.txt")); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("mount B still sees a file unlinked on mount A")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPosixWriteRecallsTheOtherMount is the full §10.6 loop through real
// mountpoints: mount B holds a cached copy, mount A writes, and the
// authority must recall B before A's write commits.
//
// The assertion is on the *authority's* report rather than on timing
// alone: a commit that recalled nobody would return an empty
// RecalledHolders, which is exactly the failure this is written to
// catch.
func TestPosixWriteRecallsTheOtherMount(t *testing.T) {
	c := startCluster(t, mds.Config{
		LeaseDuration:  time.Hour, // posix leases are revocation-bounded
		Posix:          true,
		RecallDeadline: 5 * time.Second,
	})
	a := c.mountAt(t, "mount-a", 0)
	b := c.mountAt(t, "mount-b", 0)

	if err := os.WriteFile(filepath.Join(a, "p.txt"), []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	// B reads, taking a lease the authority will have to recall.
	if got, err := os.ReadFile(filepath.Join(b, "p.txt")); err != nil || string(got) != "v1" {
		t.Fatalf("mount B initial read: %q %v", got, err)
	}

	start := time.Now()
	if err := os.WriteFile(filepath.Join(a, "p.txt"), []byte("v2"), 0o644); err != nil {
		t.Fatalf("posix write through mount A: %v", err)
	}
	elapsed := time.Since(start)

	// If the recall had timed out rather than being acknowledged, the
	// write would have taken the full DRECALL.
	if elapsed >= 5*time.Second {
		t.Fatalf("posix write took %s — the recall timed out instead of being acked", elapsed)
	}

	// And B must now see the new content.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := os.ReadFile(filepath.Join(b, "p.txt"))
		if err == nil && string(got) == "v2" {
			t.Logf("posix write committed in %s after recalling the other mount", elapsed)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mount B never saw the posix write (last %q, err %v)", got, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestShellWorkflowThroughTheMount drives the mount with real shell
// commands rather than Go's os package. Go's file APIs produce a tidier
// syscall sequence than a shell does; every FUSE bug this project has
// hit so far showed up only under the messier one.
func TestShellWorkflowThroughTheMount(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "shell", 0)

	script := fmt.Sprintf(`
set -e
cd %q
echo "first" > f.txt
cat f.txt
echo "second" > f.txt          # overwrite, exercising truncate-then-write
cat f.txt
mkdir sub
echo "nested" > sub/g.txt
cat sub/g.txt
ls sub
rm sub/g.txt
rmdir sub
`, mnt)
	out, err := exec.Command("bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("shell workflow failed: %v\n%s", err, out)
	}
	got := string(out)
	for _, want := range []string{"first", "second", "nested", "g.txt"} {
		if !bytes.Contains([]byte(got), []byte(want)) {
			t.Fatalf("expected %q in shell output:\n%s", want, got)
		}
	}
	if _, err := os.Stat(filepath.Join(mnt, "sub")); !os.IsNotExist(err) {
		t.Fatalf("sub should have been removed, got %v", err)
	}
	// The overwrite must have actually replaced the content, not appended
	// to it — the failure mode a one-shot commit latch produces.
	final, err := os.ReadFile(filepath.Join(mnt, "f.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(final) != "second\n" {
		t.Fatalf("after overwrite f.txt is %q, want \"second\\n\"", final)
	}
}

// TestMutatingHolderSeesItsOwnChangesImmediately is a regression test for
// a bug the client-side dentry cache introduced and CI caught: after this
// holder's own rmdir, a stat still resolved the name.
//
// The cause is structural, not incidental. A holder is deliberately
// excluded from its own recall (§10.6: the mutator does not recall
// itself), and the invalidation push is asynchronous anyway — so a
// client that only dropped cache entries on push would keep answering
// from a view its own mutation had just invalidated. Read-your-own-writes
// has to be maintained locally by the mutator.
//
// Each assertion here is immediately after the mutation, with no polling,
// because "eventually correct" is exactly what must not be true for the
// holder that made the change.
func TestMutatingHolderSeesItsOwnChangesImmediately(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: time.Minute})
	mnt := c.mountAt(t, "solo", 0)

	// create -> immediately visible
	p := filepath.Join(mnt, "own.txt")
	if err := os.WriteFile(p, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Split rather than combined: dereferencing st on the error path is a
	// nil panic, which hides the real failure behind a crash.
	st, err := os.Stat(p)
	if err != nil {
		t.Fatalf("own create not immediately visible: %v", err)
	}
	if st.Size() != 2 {
		t.Fatalf("own create visible with size %d, want 2", st.Size())
	}

	// overwrite -> new size immediately, not the cached one
	if err := os.WriteFile(p, []byte("much longer content"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err = os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(len("much longer content")) {
		t.Fatalf("own overwrite not immediately visible: size %d", st.Size())
	}

	// unlink -> immediately gone
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("own unlink not immediately visible: %v", err)
	}

	// mkdir -> immediately present; rmdir -> immediately gone
	d := filepath.Join(mnt, "ownd")
	if err := os.Mkdir(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(d); err != nil || !st.IsDir() {
		t.Fatalf("own mkdir not immediately visible: %v", err)
	}
	if err := os.Remove(d); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(d); !os.IsNotExist(err) {
		t.Fatalf("own rmdir not immediately visible: %v", err)
	}
}
