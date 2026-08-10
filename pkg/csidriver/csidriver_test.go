package csidriver

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// startTestServer runs a real grpc.Server over an in-memory bufconn
// listener and returns real csi.IdentityClient/csi.NodeClient dialed
// against it — genuine gRPC wire traffic (marshaling, method dispatch,
// status codes), just without a real Unix socket or a real kubelet on
// the other end.
func startTestServer(t *testing.T) (csi.IdentityClient, csi.NodeClient, *NodeServer) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ns := NewNodeServer("test-node")
	csi.RegisterIdentityServer(srv, IdentityServer{})
	csi.RegisterNodeServer(srv, ns)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return csi.NewIdentityClient(conn), csi.NewNodeClient(conn), ns
}

func TestIdentityService(t *testing.T) {
	idClient, _, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := idClient.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetName() != DriverName {
		t.Fatalf("got driver name %q, want %q", info.GetName(), DriverName)
	}

	probe, err := idClient.Probe(ctx, &csi.ProbeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !probe.GetReady().GetValue() {
		t.Fatal("expected Probe to report ready")
	}
}

func TestNodeGetInfo(t *testing.T) {
	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := nodeClient.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetNodeId() != "test-node" {
		t.Fatalf("got node id %q, want %q", info.GetNodeId(), "test-node")
	}
}

// TestPublishUnpublishStaticROXVolume is the DESIGN.md §22.1 static ROX
// flow end to end over the real CSI gRPC wire protocol: publish a repo
// via pkg/repo, drive NodePublishVolume with a VolumeContext pointing at
// it (as a PV's volumeAttributes would), read the mounted content back
// through the real filesystem path the request created, then
// NodeUnpublishVolume and confirm the mount is gone.
func TestPublishUnpublishStaticROXVolume(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hello from csi"), 0o644); err != nil {
		t.Fatal(err)
	}
	repoDir := t.TempDir()
	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.PublishTree(context.Background(), src, []string{"datasets", "demo"}); err != nil {
		t.Fatal(err)
	}
	r.Close() // NodePublishVolume reopens it, like a real node plugin process would

	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := filepath.Join(t.TempDir(), "target")
	pubReq := &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: target,
		Readonly:   true,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
		},
		VolumeContext: map[string]string{
			"repoPath": repoDir,
		},
	}
	if _, err := nodeClient.NodePublishVolume(ctx, pubReq); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}

	// Idempotent republish must succeed without error, per the CSI spec.
	if _, err := nodeClient.NodePublishVolume(ctx, pubReq); err != nil {
		t.Fatalf("idempotent republish: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(target, "datasets", "demo", "hello.txt"))
	if err != nil {
		t.Fatalf("read through CSI-mounted target: %v", err)
	}
	if !bytes.Equal(got, []byte("hello from csi")) {
		t.Fatalf("got %q, want %q", got, "hello from csi")
	}

	if _, err := nodeClient.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: target,
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}

	// Idempotent repeat unpublish must also succeed.
	if _, err := nodeClient.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: target,
	}); err != nil {
		t.Fatalf("idempotent re-unpublish: %v", err)
	}

	if _, err := os.ReadFile(filepath.Join(target, "datasets", "demo", "hello.txt")); err == nil {
		t.Fatal("expected the mount to be gone after NodeUnpublishVolume")
	}
}

func TestNodePublishVolumeRejectsReadWrite(t *testing.T) {
	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := nodeClient.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-2",
		TargetPath: filepath.Join(t.TempDir(), "target"),
		Readonly:   false,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		VolumeContext: map[string]string{"repoPath": t.TempDir()},
	})
	if err == nil {
		t.Fatal("expected a read-write request to be rejected (this build is ROX-only)")
	}
}

// rwxDisjointWritersRequest builds a MULTI_NODE_MULTI_WRITER
// NodePublishVolumeRequest for repoDir/target with the given rwxContract
// value (omit by passing ""), reused by the three tests below so they
// exercise the exact same request shape except for the one field each is
// isolating (DESIGN.md §22.2).
func rwxDisjointWritersRequest(volumeID, repoDir, target, class, rwxContract string) *csi.NodePublishVolumeRequest {
	vc := map[string]string{"repoPath": repoDir}
	if class != "" {
		vc["class"] = class
	}
	if rwxContract != "" {
		vc["rwxContract"] = rwxContract
	}
	return &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: target,
		Readonly:   false,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		},
		VolumeContext: vc,
	}
}

// TestNodePublishVolumeRWXDisjointWriters is the DESIGN.md §22.2
// disjoint-writers carve-out end to end over the real CSI gRPC wire
// protocol: a fresh `relaxed`-class repo, RWX access mode plus
// rwxContract: disjoint-writers, and the resulting mount is genuinely
// writable through the kernel — not just accepted and then silently
// still read-only.
func TestNodePublishVolumeRWXDisjointWriters(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}

	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repoDir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	req := rwxDisjointWritersRequest("vol-rwx", repoDir, target, "relaxed", "disjoint-writers")
	if _, err := nodeClient.NodePublishVolume(ctx, req); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	t.Cleanup(func() {
		_, _ = nodeClient.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId:   "vol-rwx",
			TargetPath: target,
		})
	})

	path := filepath.Join(target, "hello.txt")
	if err := os.WriteFile(path, []byte("written through rwx csi mount"), 0o644); err != nil {
		t.Fatalf("create+write through the CSI-mounted target: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back through a fresh open: %v", err)
	}
	if string(got) != "written through rwx csi mount" {
		t.Fatalf("got %q, want %q", got, "written through rwx csi mount")
	}
}

// TestNodePublishVolumeRWXWithoutContractStillRejected is the same
// request as TestNodePublishVolumeRWXDisjointWriters, minus the
// rwxContract parameter: DESIGN.md §22.2 says RWX on `relaxed` is
// opt-in only, so dropping the contract must still reject exactly as it
// did before this build understood disjoint-writers at all.
func TestNodePublishVolumeRWXWithoutContractStillRejected(t *testing.T) {
	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := rwxDisjointWritersRequest("vol-rwx-no-contract", t.TempDir(), filepath.Join(t.TempDir(), "target"), "relaxed", "")
	if _, err := nodeClient.NodePublishVolume(ctx, req); err == nil {
		t.Fatal("expected RWX without rwxContract to be rejected even on a relaxed-class repo")
	}
}

// TestNodePublishVolumeRWXContractRejectedForImmutable is the same
// request again, this time with the contract present but no class
// parameter — repoopen.Params' zero value is ClassImmutable, so the
// opened repo's persisted class is immutable, and DESIGN.md §22.2 rejects
// RWX on immutable unconditionally: the contract does not override the
// class check.
func TestNodePublishVolumeRWXContractRejectedForImmutable(t *testing.T) {
	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := rwxDisjointWritersRequest("vol-rwx-immutable", t.TempDir(), filepath.Join(t.TempDir(), "target"), "", "disjoint-writers")
	if _, err := nodeClient.NodePublishVolume(ctx, req); err == nil {
		t.Fatal("expected rwxContract: disjoint-writers on an immutable-class repo to be rejected")
	}
}

// TestRWXDisjointWritersFlockIsLocalOnly investigates DESIGN.md §22.2's
// "mounts with lock=error" clause for the disjoint-writers case. This
// build has no RPC-level fcntl/flock forwarding in pkg/fuseserver (real,
// separate, unbuilt scope), and NodePublishVolume does not (and, being
// CSI-layer plumbing, cannot) change that. What this test establishes is
// what a real flock(2) against a disjoint-writers-mounted file actually
// does today: nothing about the mount asks the kernel to forward FUSE_LK
// requests to userspace (fuseserver.Mount never sets
// fuse.MountOptions.EnableLocks), so the kernel's generic VFS flock
// implementation handles the call entirely in-kernel, node-locally, the
// same as it would for any other filesystem. That is not a lie: two
// independent local opens really do exclude each other, as asserted
// below. But it is not DESIGN.md §17's `error` mode either — a second
// node's disjoint-writers-violating flock() would get the same "success"
// this test observes rather than the loud ENOLCK §22.2 specifies, because
// nothing in this build's mount path implements `lock=error`. Wiring
// EnableLocks + GetLk/SetLk/SetLkw into pkg/fuseserver to close that gap
// is out of pkg/csidriver's scope; this test just pins down and
// documents the current, honestly-labeled behavior.
func TestRWXDisjointWritersFlockIsLocalOnly(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}

	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repoDir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	req := rwxDisjointWritersRequest("vol-rwx-flock", repoDir, target, "relaxed", "disjoint-writers")
	if _, err := nodeClient.NodePublishVolume(ctx, req); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	t.Cleanup(func() {
		_, _ = nodeClient.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId:   "vol-rwx-flock",
			TargetPath: target,
		})
	})

	path := filepath.Join(target, "lockme.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("create through the mount: %v", err)
	}

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()
	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	if err := syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("first LOCK_EX|LOCK_NB: got %v, want success (nothing else holds a lock yet)", err)
	}
	// A second, independent local open contending for the same lock must
	// still fail: this confirms today's behavior is real in-kernel,
	// node-local mutual exclusion (matching DESIGN.md §17's `local`
	// mode), not a no-op that would grant every locker "success"
	// regardless of contention.
	if err := syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != syscall.EWOULDBLOCK {
		t.Fatalf("contended LOCK_EX|LOCK_NB: got %v, want EWOULDBLOCK from real local exclusion", err)
	}
	if err := syscall.Flock(int(f1.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	// This is the crux of the gap this test documents: nothing rejected
	// this lock attempt with ENOLCK even though the mount is under the
	// disjoint-writers contract, because lock=error is not implemented.
	// A caller on a second node attempting the same flock() against its
	// own local kernel would observe the identical "success" seen here,
	// with no cross-node exclusion behind it.
	if err := syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("LOCK_EX|LOCK_NB after release: got %v, want success (this is the gap: DESIGN.md §22.2 wants ENOLCK here under disjoint-writers, but lock=error isn't wired up)", err)
	}
}

func TestNodePublishVolumeRequiresRepoPath(t *testing.T) {
	_, nodeClient, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := nodeClient.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:      "vol-3",
		TargetPath:    filepath.Join(t.TempDir(), "target"),
		Readonly:      true,
		VolumeContext: map[string]string{},
	})
	if err == nil {
		t.Fatal("expected an error when volume_context lacks repoPath")
	}
}

// TestPublishVolumeBackedByMetadataAuthority completes the Kubernetes
// end-to-end path: a PV whose VolumeContext names an mds address mounts
// through the authority instead of opening metadata on the node.
//
// That difference is what makes a PVC a *shared, coherent* namespace
// rather than one node's private view — every pod mounting it becomes
// its own lease holder, so a write from one is invalidated on the
// others. The assertions therefore check both that the mount works and
// that it is genuinely authority-backed.
func TestPublishVolumeBackedByMetadataAuthority(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}
	ctx := context.Background()

	// A repo with content, then an authority serving its metadata.
	repoDir := t.TempDir()
	r, err := repo.OpenWithClass(repoDir, mustLocalBackend(t, filepath.Join(repoDir, "objects")),
		repo.DefaultRegion, repo.ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "pv.txt"), []byte("served to a pod"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	r.Close()

	db, err := metadb.Open(filepath.Join(repoDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A zero lease duration is deliberate: with a real lease the client
	// would be *entitled* to keep answering from cache after the
	// authority stops, so the "stop it and watch reads fail" assertion
	// below would be testing the lease, not the dependency.
	mdsSrv := mds.NewServer(mds.Config{DB: db})
	g := grpc.NewServer()
	mdsSrv.Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	defer g.Stop()

	_, nodeClient, _ := startTestServer(t)
	target := filepath.Join(t.TempDir(), "mnt")

	_, err = nodeClient.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-mds",
		TargetPath: target,
		Readonly:   true,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		},
		VolumeContext: map[string]string{
			"repoPath": repoDir,
			"mdsAddr":  lis.Addr().String(),
		},
	})
	if err != nil {
		t.Fatalf("NodePublishVolume via authority: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(target, "pv.txt"))
	if err != nil {
		t.Fatalf("read through the authority-backed PV: %v", err)
	}
	if string(got) != "served to a pod" {
		t.Fatalf("got %q through the PV", got)
	}

	// The load-bearing assertion: stop the authority and the volume must
	// stop resolving. Without it this test would pass just as happily if
	// the driver had ignored mdsAddr and opened the local metadb.
	g.Stop()
	deadline := time.Now().Add(5 * time.Second)
	authorityWasLoadBearing := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(target, "pv.txt")); err != nil {
			authorityWasLoadBearing = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if _, err := nodeClient.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId: "vol-mds", TargetPath: target,
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if !authorityWasLoadBearing {
		t.Fatal("the volume kept resolving after the authority stopped — mdsAddr was not actually used")
	}
}

func mustLocalBackend(t *testing.T, dir string) *local.Backend {
	t.Helper()
	b, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
