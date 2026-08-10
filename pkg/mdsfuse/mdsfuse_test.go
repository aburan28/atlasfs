package mdsfuse

import (
	"bytes"
	"context"
	"net"
	"os"
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

// The whole point of this package, tested end to end: a kernel FUSE
// mount whose metadata crosses a real gRPC connection to a separate
// metadata authority, and whose bytes come from object storage without
// the authority ever seeing them.
//
// Publishing still goes through pkg/repo (the write path is not part of
// this mount), so the fixture is: publish locally, then serve that same
// metadb through an authority, then mount against the authority and
// read.

type env struct {
	mountpoint string
	repoDir    string
	client     *mds.Client
}

func setup(t *testing.T, build func(t *testing.T, r *repo.Repo)) env {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}
	ctx := context.Background()
	repoDir := t.TempDir()

	// 1. Build content with the ordinary local repo.
	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	build(t, r)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Serve that metadata through a real authority on a real socket.
	db, err := metadb.Open(filepath.Join(repoDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv := mds.NewServer(mds.Config{DB: db, LeaseDuration: 30 * time.Second})
	g := grpc.NewServer()
	srv.Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), mds.DialOption())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	client := mds.NewClient(cc, "mount-holder", nil)
	t.Cleanup(client.Close)

	// 3. Bytes come from the backend directly, never through the
	//    authority — the split this package exists to preserve.
	backend, err := local.New(filepath.Join(repoDir, "objects"))
	if err != nil {
		t.Fatal(err)
	}

	mountpoint := t.TempDir()
	mounted := make(chan *fuse.Server, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Mount(context.Background(), Config{
			Client:         client,
			Backend:        backend,
			KernelCacheTTL: repo.ClassRelaxed.KernelCacheTTL(),
		}, mountpoint, func(s *fuse.Server) { mounted <- s })
	}()

	var server *fuse.Server
	select {
	case server = <-mounted:
	case err := <-errCh:
		t.Fatalf("mount failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the mds-backed mount")
	}
	t.Cleanup(func() {
		_ = server.Unmount()
		<-errCh
	})
	_ = ctx
	return env{mountpoint: mountpoint, repoDir: repoDir, client: client}
}

func writeSrc(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestMountBackedByAuthorityReadsThroughKernel(t *testing.T) {
	small := []byte("served through a remote metadata authority")
	big := bytes.Repeat([]byte("chunked-content-"), 4000) // multi-chunk

	e := setup(t, func(t *testing.T, r *repo.Repo) {
		src := t.TempDir()
		writeSrc(t, src, "hello.txt", small)
		writeSrc(t, src, "data/big.bin", big)
		if err := os.Symlink("hello.txt", filepath.Join(src, "link.txt")); err != nil {
			t.Fatal(err)
		}
		r.ChunkSize = 4096
		if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
			t.Fatal(err)
		}
	})

	got, err := os.ReadFile(filepath.Join(e.mountpoint, "hello.txt"))
	if err != nil {
		t.Fatalf("read through the mds-backed mount: %v", err)
	}
	if !bytes.Equal(got, small) {
		t.Fatalf("small file mismatch: got %q", got)
	}

	gotBig, err := os.ReadFile(filepath.Join(e.mountpoint, "data", "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBig, big) {
		t.Fatalf("multi-chunk file mismatch: %d bytes vs %d", len(gotBig), len(big))
	}

	entries, err := os.ReadDir(e.mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		names := []string{}
		for _, en := range entries {
			names = append(names, en.Name())
		}
		t.Fatalf("expected 3 top-level entries, got %v", names)
	}

	target, err := os.Readlink(filepath.Join(e.mountpoint, "link.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if target != "hello.txt" {
		t.Fatalf("readlink got %q", target)
	}
	viaLink, err := os.ReadFile(filepath.Join(e.mountpoint, "link.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(viaLink, small) {
		t.Fatal("reading through the symlink returned the wrong bytes")
	}
}

// TestMountIsReadOnly: this mount deliberately serves no writes, and it
// must say so with EROFS rather than failing somewhere deeper with a
// confusing error.
func TestMountIsReadOnly(t *testing.T) {
	e := setup(t, func(t *testing.T, r *repo.Repo) {
		src := t.TempDir()
		writeSrc(t, src, "f.txt", []byte("read only"))
		if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
			t.Fatal(err)
		}
	})

	if err := os.WriteFile(filepath.Join(e.mountpoint, "new.txt"), []byte("x"), 0o644); err == nil {
		t.Fatal("expected a write to the read-only mds-backed mount to fail")
	}
	if err := os.Remove(filepath.Join(e.mountpoint, "f.txt")); err == nil {
		t.Fatal("expected unlink on the read-only mount to fail")
	}
}

// TestReadsGoThroughTheAuthority is the load-bearing check. Everything
// above would pass just as happily if the mount had quietly opened the
// local metadb itself, so this proves the RPC boundary is real: stop the
// authority, drop the client's cached leases, and reads must fail. If
// they still succeed, the mount was not talking to the authority at all.
func TestReadsGoThroughTheAuthority(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}
	ctx := context.Background()
	repoDir := t.TempDir()

	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	writeSrc(t, src, "f.txt", []byte("authority-served"))
	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	r.Close()

	db, err := metadb.Open(filepath.Join(repoDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A zero lease duration means every read revalidates, so stopping the
	// authority is immediately visible rather than masked for 30s.
	srv := mds.NewServer(mds.Config{DB: db})
	g := grpc.NewServer()
	srv.Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)

	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), mds.DialOption())
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	client := mds.NewClient(cc, "holder", nil)
	defer client.Close()

	backend, err := local.New(filepath.Join(repoDir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted := make(chan *fuse.Server, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Mount(context.Background(), Config{Client: client, Backend: backend}, mountpoint,
			func(s *fuse.Server) { mounted <- s })
	}()
	var server *fuse.Server
	select {
	case server = <-mounted:
	case err := <-errCh:
		t.Fatalf("mount failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for mount")
	}
	defer func() {
		_ = server.Unmount()
		<-errCh
	}()

	if _, err := os.ReadFile(filepath.Join(mountpoint, "f.txt")); err != nil {
		t.Fatalf("precondition: read should work while the authority is up: %v", err)
	}

	g.Stop()

	// With the authority gone and no lease to fall back on, a lookup must
	// fail. A mount reading the local metadb directly would not notice.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(filepath.Join(mountpoint, "f.txt"))
		if err != nil {
			return // correct: the mount depends on the authority
		}
		if time.Now().After(deadline) {
			t.Fatal("reads still succeeded after the authority was stopped — the mount is not actually going through it")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
