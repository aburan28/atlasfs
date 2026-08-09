package csidriver

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/aburan28/atlasfs/pkg/repo"
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
