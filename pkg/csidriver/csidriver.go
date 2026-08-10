// Package csidriver implements the CSI Identity and Node services for
// AtlasFS's static PV case (DESIGN.md §22.1). There is deliberately no
// Controller service: static provisioning means an admin creates the
// PersistentVolume directly with volumeHandle and volumeAttributes
// already pointing at a published subtree, so kubelet calls
// NodePublishVolume straight from the PV spec — CreateVolume is never in
// the flow. Implementing a Controller for dynamic provisioning across
// regions/backends is real, unbuilt scope (DESIGN.md §28 Phase 2+), not
// something this package pretends to cover.
//
// Per DESIGN.md §22.2, most non-read-only requests are still rejected:
// this build's mutable path only backs the `relaxed`/`session` classes
// (pkg/fuseserver), and RWX specifically requires the caller to opt into
// the `rwxContract: disjoint-writers` VolumeContext parameter on a
// `relaxed` subtree — the one row §22.2 carves out as opt-in rather than
// flatly rejected. Everything else non-read-only (immutable class, no
// contract, or a class this build doesn't support for RWX) is rejected
// exactly as before.
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/aburan28/atlasfs/pkg/fuseserver"
	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/mdsfuse"
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
// over FUSE (pkg/fuseserver) for each published volume — read-only in
// the common case, read-write for the §22.2 disjoint-writers carve-out.
type NodeServer struct {
	csi.UnimplementedNodeServer

	NodeID string

	mu     sync.Mutex
	mounts map[string]*activeMount // keyed by TargetPath
}

type activeMount struct {
	volumeID string
	repo     *repo.Repo // nil for an authority-backed mount
	server   *fuse.Server
	done     chan struct{}

	// closers run at unpublish: the gRPC connection and mds client for an
	// authority-backed mount, which have no repo.Close to hang off.
	closers []func()
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

// rwxContractDisjointWriters is the only value accepted for the
// VolumeContext's rwxContract key (DESIGN.md §22.2). It opts a RWX
// request into the `relaxed` class's disjoint-writers pattern instead of
// being rejected outright.
const rwxContractDisjointWriters = "disjoint-writers"

// NodePublishVolume mounts the subtree named by req's VolumeContext at
// req.TargetPath. DESIGN.md §22.2: a non-read-only request is rejected
// unless the caller declared rwxContract: disjoint-writers, in which case
// it's allowed but only once the opened repo's persisted class confirms
// it's actually `relaxed` — every other class this build can open (in
// particular `immutable`, and `session` since disjoint-writers is a
// `relaxed`-only contract in this build) still gets rejected exactly as
// the ROX-only build did.
func (n *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}
	readOnly := isReadOnlyRequest(req)
	disjointWriters := req.GetVolumeContext()["rwxContract"] == rwxContractDisjointWriters
	if !readOnly && !disjointWriters {
		return nil, status.Error(codes.InvalidArgument, "csi.atlas.io only serves read-only volumes in this build unless rwxContract: disjoint-writers is set (DESIGN.md §22.2)")
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

	if params.MDSAddr != "" {
		// Authority-backed volume: metadata over gRPC, bytes from object
		// storage. The class checks below do not apply — the authority
		// owns the class, and this node has no local metadata to consult
		// for it.
		return n.publishViaMDS(ctx, req, repoDir, params, target, readOnly)
	}

	r, err := repoopen.Open(ctx, repoDir, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open repo: %v", err)
	}

	if !readOnly && r.Class != repo.ClassRelaxed {
		r.Close()
		return nil, status.Errorf(codes.InvalidArgument, "csi.atlas.io: rwxContract: disjoint-writers is only valid on a relaxed-class subtree (DESIGN.md §22.2); this subtree is class %q", r.Class)
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
	for _, c := range m.closers {
		c()
	}
	if m.repo != nil {
		if err := m.repo.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "close repo: %v", err)
		}
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

// publishViaMDS mounts a volume whose metadata lives behind a remote
// authority (VolumeContext mdsAddr). This is what lets a PV point at a
// shared, coherent namespace instead of at one node's private metadata:
// every pod mounting the volume becomes its own lease holder, so a write
// from one is invalidated — or, under `posix`, recalled — on the others.
//
// The node still reads and writes chunk bytes straight to object storage
// (DESIGN.md §11), so the authority's load does not scale with data
// volume, only with metadata operations.
func (n *NodeServer) publishViaMDS(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	repoDir string,
	params repoopen.Params,
	target string,
	readOnly bool,
) (*csi.NodePublishVolumeResponse, error) {
	cc, err := grpc.NewClient(params.MDSAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), mds.DialOption())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "connect to metadata authority %s: %v", params.MDSAddr, err)
	}

	// Holder identity is per *volume mount*, not per node: two pods on one
	// node mounting the same volume are two independent caches, and a
	// shared identity would let one pod's recall ack speak for the other's.
	holder := fmt.Sprintf("%s/%s", n.NodeID, req.GetVolumeId())
	client := mds.NewClient(cc, holder, nil)
	if err := client.Subscribe(ctx); err != nil {
		cc.Close()
		return nil, status.Errorf(codes.Internal, "subscribe to invalidations: %v", err)
	}

	backend, err := repoopen.OpenBackend(ctx, repoDir, params)
	if err != nil {
		client.Close()
		cc.Close()
		return nil, status.Errorf(codes.Internal, "open backend: %v", err)
	}

	mounted := make(chan *fuse.Server, 1)
	mountErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		err := mdsfuse.Mount(context.Background(), mdsfuse.Config{
			Client:   client,
			Backend:  backend,
			ReadOnly: readOnly,
			Region:   params.Region,
			// Zero: without asking the authority for the subtree's class,
			// assume the strictest one. A `posix` volume must not have the
			// kernel answering getattr from a cache that cannot be recalled.
			KernelCacheTTL: 0,
		}, target, func(s *fuse.Server) { mounted <- s })
		if err != nil {
			select {
			case mountErr <- err:
			default:
			}
		}
		close(done)
	}()

	cleanup := []func(){client.Close, func() { cc.Close() }}
	select {
	case server := <-mounted:
		n.mu.Lock()
		n.mounts[target] = &activeMount{
			volumeID: req.GetVolumeId(),
			server:   server,
			done:     done,
			closers:  cleanup,
		}
		n.mu.Unlock()
		return &csi.NodePublishVolumeResponse{}, nil
	case err := <-mountErr:
		for _, c := range cleanup {
			c()
		}
		return nil, status.Errorf(codes.Internal, "mount via authority: %v", err)
	case <-ctx.Done():
		for _, c := range cleanup {
			c()
		}
		return nil, status.Error(codes.DeadlineExceeded, "mount did not complete before context deadline")
	}
}
