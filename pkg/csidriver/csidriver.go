// Package csidriver implements the CSI Identity and Node services for
// AtlasFS's static, read-only PV case (DESIGN.md §22.1, §22.2's ROX row).
// There is deliberately no Controller service: static provisioning means
// an admin creates the PersistentVolume directly with volumeHandle and
// volumeAttributes already pointing at a published subtree, so kubelet
// calls NodePublishVolume straight from the PV spec — CreateVolume is
// never in the flow. Implementing a Controller for dynamic provisioning
// across regions/backends is real, unbuilt scope (DESIGN.md §28 Phase 2+),
// not something this package pretends to cover.
//
// This driver does not advertise STAGE_UNSTAGE_VOLUME, so kubelet skips
// NodeStageVolume/NodeUnstageVolume entirely and calls
// NodePublishVolume/NodeUnpublishVolume directly — the right shape for a
// FUSE mount that isn't a block device needing a staging mount.
package csidriver

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/aburan28/atlasfs/pkg/fuseserver"
	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/repoopen"
)

const (
	DriverName = "csi.atlas.io"
	// PluginVersion is the vertical slice's own version, not AtlasFS's;
	// bump it when this package's wire behavior changes.
	PluginVersion = "0.1.0-phase1-slice"
)

// IdentityServer implements the CSI Identity service.
type IdentityServer struct {
	csi.UnimplementedIdentityServer
}

func (IdentityServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: PluginVersion}, nil
}

func (IdentityServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// No CONTROLLER_SERVICE capability: intentionally no Controller
	// service, per the package doc comment above.
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (IdentityServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

// NodeServer implements the CSI Node service against a repo.Repo mounted
// read-only over FUSE (pkg/fuseserver) for each published volume.
type NodeServer struct {
	csi.UnimplementedNodeServer

	NodeID string

	mu     sync.Mutex
	mounts map[string]*activeMount // keyed by TargetPath
}

type activeMount struct {
	volumeID string
	repo     *repo.Repo
	server   *fuse.Server
	done     chan struct{}
}

func NewNodeServer(nodeID string) *NodeServer {
	return &NodeServer{NodeID: nodeID, mounts: map[string]*activeMount{}}
}

func (n *NodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.NodeID}, nil
}

func (n *NodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// Deliberately empty: no STAGE_UNSTAGE_VOLUME (no staging phase, see
	// package doc), no EXPAND_VOLUME (nothing to expand — capacity for
	// an immutable, already-published subtree isn't a live quota in this
	// build, unlike the writable case DESIGN.md §22.3 describes for a
	// later phase), no GET_VOLUME_STATS (same reason: no quota system
	// yet to report from). Claiming a capability this build can't back
	// is worse than a short list.
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodePublishVolume mounts the subtree named by req's VolumeContext
// read-only at req.TargetPath. DESIGN.md §22.2: this driver only ever
// serves ROX for the `immutable` class this build implements, so any
// request that isn't read-only is rejected here rather than silently
// downgraded.
func (n *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}
	if !isReadOnlyRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "csi.atlas.io only serves read-only volumes in this build (DESIGN.md §22.2: immutable class is ROX-only)")
	}

	repoDir, params, err := repoopen.ParamsFromMap(req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "volume_context: %v", err)
	}

	n.mu.Lock()
	if _, exists := n.mounts[target]; exists {
		n.mu.Unlock()
		// Idempotent per the CSI spec: a repeat publish to an
		// already-mounted target is success, not an error.
		return &csi.NodePublishVolumeResponse{}, nil
	}
	n.mu.Unlock()

	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "create target_path: %v", err)
	}

	r, err := repoopen.Open(ctx, repoDir, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open repo: %v", err)
	}

	mounted := make(chan *fuse.Server, 1)
	mountErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		err := fuseserver.Mount(context.Background(), r, target, func(s *fuse.Server) {
			mounted <- s
		})
		if err != nil {
			select {
			case mountErr <- err:
			default:
			}
		}
		close(done)
	}()

	select {
	case server := <-mounted:
		n.mu.Lock()
		n.mounts[target] = &activeMount{volumeID: req.GetVolumeId(), repo: r, server: server, done: done}
		n.mu.Unlock()
		return &csi.NodePublishVolumeResponse{}, nil
	case err := <-mountErr:
		r.Close()
		return nil, status.Errorf(codes.Internal, "mount: %v", err)
	case <-ctx.Done():
		r.Close()
		return nil, status.Error(codes.DeadlineExceeded, "mount did not complete before context deadline")
	}
}

func isReadOnlyRequest(req *csi.NodePublishVolumeRequest) bool {
	if req.GetReadonly() {
		return true
	}
	vc := req.GetVolumeCapability()
	if vc == nil {
		return false
	}
	switch vc.GetAccessMode().GetMode() {
	case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
		return true
	default:
		return false
	}
}

// NodeUnpublishVolume unmounts and releases the repo opened by a prior
// NodePublishVolume. Also idempotent per the CSI spec: unpublishing an
// already-gone target is success.
func (n *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}

	n.mu.Lock()
	m, exists := n.mounts[target]
	if exists {
		delete(n.mounts, target)
	}
	n.mu.Unlock()
	if !exists {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if err := m.server.Unmount(); err != nil {
		return nil, status.Errorf(codes.Internal, "unmount: %v", err)
	}
	<-m.done
	if err := m.repo.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "close repo: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeStageVolume(context.Context, *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "staging not supported: NodeGetCapabilities does not advertise STAGE_UNSTAGE_VOLUME")
}

func (n *NodeServer) NodeUnstageVolume(context.Context, *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "staging not supported: NodeGetCapabilities does not advertise STAGE_UNSTAGE_VOLUME")
}

func (n *NodeServer) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("%s: no quota system in this build to expand against (DESIGN.md §22.3 is a later phase)", DriverName))
}

func (n *NodeServer) NodeGetVolumeStats(context.Context, *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not advertised in NodeGetCapabilities")
}
