package mds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aburan28/atlasfs/pkg/coherence"
	"github.com/aburan28/atlasfs/pkg/metadb"
)

// DefaultRecallDeadline is DESIGN.md §10.6's DRECALL: how long a `posix`
// mutation waits for holders to acknowledge before proceeding without
// them. It bounds the blast radius of one unreachable client — without
// it, a single partitioned holder would stall every writer behind it
// indefinitely.
const DefaultRecallDeadline = 2 * time.Second

// Config configures a Server.
type Config struct {
	DB *metadb.DB

	// LeaseDuration is D for this subtree's class (DESIGN.md §8). Zero
	// means the `immutable` class: nothing is ever invalidated, so no
	// coherence manager is needed at all.
	LeaseDuration time.Duration

	// Posix selects §10.6 semantics: every mutation blocks on recalling
	// other holders' leases rather than relying on their expiry. This is
	// what "D = 0" means operationally — zero staleness because
	// invalidation is synchronous, not because leases are short.
	Posix bool

	// RecallDeadline overrides DefaultRecallDeadline.
	RecallDeadline time.Duration

	// Region is the locator keyspace this authority serves (DESIGN.md
	// §7.5). Empty means repo.DefaultRegion.
	Region string

	Clock coherence.Clock
}

// Server is one region's metadata authority.
type Server struct {
	db       *metadb.DB
	coh      *coherence.Manager
	posix    bool
	drecall  time.Duration
	leaseTTL time.Duration
	region   string

	mu      sync.Mutex
	streams map[string]*holderStream // by holder ID
}

// holderStream is one connected holder's push channel. Events are
// dropped rather than blocking the mutator when a holder is not draining
// its stream — for EventInvalidate that is explicitly allowed (§10.2,
// best-effort), and for EventRecall it degrades to the same outcome as an
// unreachable holder: the commit waits out DRECALL and reports the
// holder as un-acked, which is exactly what a stalled client is.
type holderStream struct {
	ch     chan Event
	cancel context.CancelFunc
}

func NewServer(cfg Config) *Server {
	s := &Server{
		db:       cfg.DB,
		posix:    cfg.Posix,
		drecall:  cfg.RecallDeadline,
		leaseTTL: cfg.LeaseDuration,
		region:   cfg.Region,
		streams:  map[string]*holderStream{},
	}
	if s.region == "" {
		s.region = defaultRegion
	}
	if s.drecall <= 0 {
		s.drecall = DefaultRecallDeadline
	}
	clock := cfg.Clock
	if clock == nil {
		clock = coherence.RealClock{}
	}
	if cfg.LeaseDuration > 0 {
		s.coh = coherence.New(clock, cfg.LeaseDuration)
	}
	return s
}

// Register attaches this server to a grpc.Server.
func (s *Server) Register(g *grpc.Server) { g.RegisterService(&serviceDesc, s) }

func inodeObj(id metadb.InodeID) string { return fmt.Sprintf("inode:%d", id) }
func dirObj(id metadb.InodeID) string   { return fmt.Sprintf("dir:%d", id) }

// grant issues a lease for obj to holder and wires up both push paths.
func (s *Server) grant(holder, obj string) Lease {
	if s.coh == nil {
		// immutable: no invalidation is possible, so a lease with no
		// expiry is not a shortcut, it is the accurate statement.
		return Lease{Object: obj}
	}
	v := s.coh.Grant(holder, obj)
	s.coh.Subscribe(holder, obj, func() { s.push(holder, Event{Kind: EventInvalidate, Object: obj}) })
	if s.posix {
		s.coh.SubscribeRecall(holder, obj, func(id uint64) {
			s.push(holder, Event{Kind: EventRecall, Object: obj, RecallID: id})
		})
	}
	return Lease{Object: obj, Version: v, TTL: s.leaseTTL}
}

// bumpDir advances both the directory's negative-cache version (§10.4)
// and its lease version, the latter firing every subscriber so holders
// drop cached dentries for this directory. Both are needed: dirver
// invalidates cached *misses*, the object bump invalidates cached
// *hits*.
func (s *Server) bumpDir(dir metadb.InodeID) {
	if s.coh == nil {
		return
	}
	s.coh.BumpDir(dirObj(dir))
	s.coh.Bump(dirObj(dir))
}

func (s *Server) push(holder string, ev Event) {
	s.mu.Lock()
	hs := s.streams[holder]
	s.mu.Unlock()
	if hs == nil {
		return
	}
	select {
	case hs.ch <- ev:
	default: // see holderStream's doc comment on why dropping is safe
	}
}

func (s *Server) GetInode(ctx context.Context, req *GetInodeRequest) (*GetInodeResponse, error) {
	rec, err := s.db.GetInode(req.Inode)
	if err != nil {
		return nil, toStatus(err)
	}
	return &GetInodeResponse{Record: rec, Lease: s.grant(req.Holder, inodeObj(req.Inode))}, nil
}

func (s *Server) Lookup(ctx context.Context, req *LookupRequest) (*LookupResponse, error) {
	resp := &LookupResponse{}
	if s.coh != nil {
		resp.DirVersion = s.coh.DirVersion(dirObj(req.Dir))
		// Lease the directory too: without it a holder could cache a
		// dentry with nothing able to invalidate it, and every path
		// resolution would have to round-trip forever (§10.1 lists
		// dentries as cached state, §10.5 gives them their own domain).
		resp.DirLease = s.grant(req.Holder, dirObj(req.Dir))
	}
	id, err := s.db.Lookup(req.Dir, req.Name)
	if errors.Is(err, metadb.ErrNotFound) {
		// A miss is cacheable too, validated against the directory
		// version rather than per name (DESIGN.md §10.4).
		if s.coh != nil {
			s.coh.GrantNegative(req.Holder, dirObj(req.Dir), req.Name)
		}
		return resp, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	rec, err := s.db.GetInode(id)
	if err != nil {
		return nil, toStatus(err)
	}
	resp.Found, resp.Inode, resp.Record = true, id, rec
	resp.Lease = s.grant(req.Holder, inodeObj(id))
	return resp, nil
}

func (s *Server) Readdir(ctx context.Context, req *ReaddirRequest) (*ReaddirResponse, error) {
	entries, err := s.db.Readdir(req.Dir)
	if err != nil {
		return nil, toStatus(err)
	}
	return &ReaddirResponse{Entries: entries, Lease: s.grant(req.Holder, dirObj(req.Dir))}, nil
}

// Commit binds name in dir. For the `posix` class it first performs
// §10.6's blocking recall against every other holder of the affected
// inode, and only then writes — which is the entire point of the
// class: a reader cannot still be serving the old attrs at the moment
// the new ones become visible.
func (s *Server) Commit(ctx context.Context, req *CommitRequest) (*CommitResponse, error) {
	resp := &CommitResponse{}

	if s.posix && s.coh != nil {
		// Recall before the write, not after. Recalling afterwards would
		// leave a window in which the authority has already committed
		// while another holder's lease still says the old version is
		// good — precisely the interleaving invRecallSafety rules out.
		if existing, err := s.db.Lookup(req.Dir, req.Name); err == nil {
			res, err := s.coh.Recall(ctx, inodeObj(existing), req.Holder, s.drecall)
			if err != nil {
				return nil, status.FromContextError(err).Err()
			}
			resp.RecalledHolders, resp.TimedOutHolders = res.Acked, res.TimedOut
		} else if !errors.Is(err, metadb.ErrNotFound) {
			return nil, toStatus(err)
		}
	}

	id, err := s.db.CommitFile(req.Dir, req.Name, req.Record)
	if err != nil {
		return nil, toStatus(err)
	}
	resp.Inode = id
	if s.coh != nil {
		resp.Version = s.coh.Bump(inodeObj(id))
	}
	s.bumpDir(req.Dir)
	return resp, nil
}

func (s *Server) Unlink(ctx context.Context, req *UnlinkRequest) (*UnlinkResponse, error) {
	resp := &UnlinkResponse{}
	id, err := s.db.Lookup(req.Dir, req.Name)
	if err != nil {
		return nil, toStatus(err)
	}
	if s.posix && s.coh != nil {
		res, err := s.coh.Recall(ctx, inodeObj(id), req.Holder, s.drecall)
		if err != nil {
			return nil, status.FromContextError(err).Err()
		}
		_ = res
	}
	if err := s.db.RemoveEntry(req.Dir, req.Name, time.Now()); err != nil {
		return nil, toStatus(err)
	}
	if s.coh != nil {
		resp.Version = s.coh.Bump(inodeObj(id))
	}
	s.bumpDir(req.Dir)
	return resp, nil
}

// Subscribe streams invalidations and recalls to one holder until the
// client goes away.
func (s *Server) Subscribe(req *SubscribeRequest, stream grpc.ServerStream) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	hs := &holderStream{ch: make(chan Event, 64), cancel: cancel}
	s.mu.Lock()
	if old := s.streams[req.Holder]; old != nil {
		// A reconnect supersedes the previous stream; the old one is
		// released rather than left to accumulate.
		old.cancel()
	}
	s.streams[req.Holder] = hs
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.streams[req.Holder] == hs {
			delete(s.streams, req.Holder)
		}
		s.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-hs.ch:
			if err := stream.SendMsg(&ev); err != nil {
				return err
			}
		}
	}
}

// GetLocator resolves a chunk to its container placement. Deliberately
// grants no lease: a locator for an immutable, content-addressed chunk
// cannot go stale in a way that matters. Compaction (DESIGN.md §19.1
// step 3) can move the bytes, but it retires the old container only
// after the grace period, so a client holding a slightly old locator
// still reads correct bytes — and reads are hash-verified regardless
// (§24.4).
func (s *Server) GetLocator(ctx context.Context, req *GetLocatorRequest) (*GetLocatorResponse, error) {
	loc, found, err := s.db.GetLocator(s.region, req.ChunkID)
	if err != nil {
		return nil, toStatus(err)
	}
	return &GetLocatorResponse{Found: found, Locator: loc}, nil
}

func (s *Server) AckRecall(ctx context.Context, req *AckRecallRequest) (*AckRecallResponse, error) {
	if s.coh != nil {
		s.coh.AckRecall(req.RecallID, req.Holder)
	}
	return &AckRecallResponse{}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, metadb.ErrNotFound):
		return withErrno(codes.NotFound, syscall.ENOENT, err)
	case errors.Is(err, metadb.ErrExists):
		return withErrno(codes.AlreadyExists, syscall.EEXIST, err)
	case errors.Is(err, metadb.ErrNotDir):
		return withErrno(codes.FailedPrecondition, syscall.ENOTDIR, err)
	case errors.Is(err, metadb.ErrIsDirectory):
		return withErrno(codes.FailedPrecondition, syscall.EISDIR, err)
	case errors.Is(err, metadb.ErrNotEmpty):
		return withErrno(codes.FailedPrecondition, syscall.ENOTEMPTY, err)
	case errors.Is(err, metadb.ErrInvalidRename):
		return withErrno(codes.InvalidArgument, syscall.EINVAL, err)
	case errors.Is(err, metadb.ErrQuotaExceeded):
		return withErrno(codes.ResourceExhausted, syscall.EDQUOT, err)
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// defaultRegion mirrors repo.DefaultRegion without importing pkg/repo,
// which would be a dependency cycle: repo does not import mds today, but
// the mds-backed mount will, and an authority needing the client's
// package to name its own keyspace would be backwards.
const defaultRegion = "local"

// PutLocator registers a chunk's placement after the client has sealed
// its container to object storage.
func (s *Server) PutLocator(ctx context.Context, req *PutLocatorRequest) (*PutLocatorResponse, error) {
	if err := s.db.PutLocator(s.region, req.ChunkID, req.Locator); err != nil {
		return nil, toStatus(err)
	}
	return &PutLocatorResponse{}, nil
}

// HasLocator answers the write path's dedup question before the client
// spends bandwidth uploading (DESIGN.md §16.1 step 2).
func (s *Server) HasLocator(ctx context.Context, req *HasLocatorRequest) (*HasLocatorResponse, error) {
	found, err := s.db.HasLocator(s.region, req.ChunkID)
	if err != nil {
		return nil, toStatus(err)
	}
	return &HasLocatorResponse{Found: found}, nil
}

// Mkdir binds a new directory. Like Commit it bumps the parent's
// directory version, which is what invalidates every holder's negative
// cache entry for this name in one step (§10.4).
func (s *Server) Mkdir(ctx context.Context, req *MkdirRequest) (*MkdirResponse, error) {
	rec := metadb.InodeRecord{
		IsDir: true, Mode: req.Mode & 0o7777, Uid: req.Uid, Gid: req.Gid, MTime: time.Now(), NLink: 2,
	}
	id, err := s.db.CommitMkdir(req.Dir, req.Name, rec)
	if err != nil {
		return nil, toStatus(err)
	}
	s.bumpDir(req.Dir)
	return &MkdirResponse{Inode: id}, nil
}

// Rmdir removes an empty directory. The emptiness check happens here
// rather than on the client because only the authority sees every
// holder's creates — a client-side check would race.
func (s *Server) Rmdir(ctx context.Context, req *RmdirRequest) (*RmdirResponse, error) {
	id, err := s.db.Lookup(req.Dir, req.Name)
	if err != nil {
		return nil, toStatus(err)
	}
	rec, err := s.db.GetInode(id)
	if err != nil {
		return nil, toStatus(err)
	}
	if !rec.IsDir {
		return nil, toStatus(fmt.Errorf("%w: %q", metadb.ErrNotDir, req.Name))
	}
	entries, err := s.db.Readdir(id)
	if err != nil {
		return nil, toStatus(err)
	}
	if len(entries) > 0 {
		return nil, toStatus(fmt.Errorf("%w: %q", metadb.ErrNotEmpty, req.Name))
	}
	if s.posix && s.coh != nil {
		if _, err := s.coh.Recall(ctx, inodeObj(id), req.Holder, s.drecall); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
	if err := s.db.RemoveEntry(req.Dir, req.Name, time.Now()); err != nil {
		return nil, toStatus(err)
	}
	if s.coh != nil {
		s.coh.Bump(inodeObj(id))
	}
	s.bumpDir(req.Dir)
	return &RmdirResponse{}, nil
}

// Rename moves a binding and bumps both directories, since a name
// appeared in one and vanished from the other.
func (s *Server) Rename(ctx context.Context, req *RenameRequest) (*RenameResponse, error) {
	if s.posix && s.coh != nil {
		// The displaced target, if any, is the inode whose holders must
		// give up their copies before the rebind is visible.
		if dstID, err := s.db.Lookup(req.NewDir, req.NewName); err == nil {
			if _, err := s.coh.Recall(ctx, inodeObj(dstID), req.Holder, s.drecall); err != nil {
				return nil, status.FromContextError(err).Err()
			}
		}
	}
	if err := s.db.Rename(req.OldDir, req.OldName, req.NewDir, req.NewName, time.Now()); err != nil {
		return nil, toStatus(err)
	}
	s.bumpDir(req.OldDir)
	if req.NewDir != req.OldDir {
		s.bumpDir(req.NewDir)
	}
	return &RenameResponse{}, nil
}

// SetAttr changes an inode's mode and/or mtime. It is a `posix`-class
// recall point like any other inode mutation: a holder caching the old
// attrs has to give that copy up before the change is visible, or it
// would keep answering getattr with the pre-chmod mode.
func (s *Server) SetAttr(ctx context.Context, req *SetAttrRequest) (*SetAttrResponse, error) {
	if s.posix && s.coh != nil {
		if _, err := s.coh.Recall(ctx, inodeObj(req.Inode), req.Holder, s.drecall); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
	rec, err := s.db.SetAttr(req.Inode, metadb.AttrMutation{
		Mode: req.Mode, Uid: req.Uid, Gid: req.Gid, MTime: req.MTime, ATime: req.ATime,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	if s.coh != nil {
		s.coh.Bump(inodeObj(req.Inode))
	}
	return &SetAttrResponse{Record: rec}, nil
}

// Statfs reports the subtree's quota and usage. It is not cached under a
// lease: usage changes on every write from every holder, so a leased copy
// would be stale far more often than not, and df is rare enough that the
// round trip costs nothing worth saving.
func (s *Server) Statfs(ctx context.Context, req *StatfsRequest) (*StatfsResponse, error) {
	bytesLimit, inodesLimit, err := s.db.GetQuotaLimits()
	if err != nil {
		return nil, toStatus(err)
	}
	bytesUsed, inodesUsed, err := s.db.GetQuotaUsage()
	if err != nil {
		return nil, toStatus(err)
	}
	return &StatfsResponse{
		BytesLimit: bytesLimit, InodesLimit: inodesLimit,
		BytesUsed: bytesUsed, InodesUsed: inodesUsed,
	}, nil
}

// Mknod creates a special file. It has no content, so unlike Commit
// there is nothing for the client to upload first.
func (s *Server) Mknod(ctx context.Context, req *MknodRequest) (*MknodResponse, error) {
	id, err := s.db.CreateSpecial(req.Dir, req.Name, req.Type, req.Rdev, req.Mode, req.Uid, req.Gid)
	if err != nil {
		return nil, toStatus(err)
	}
	s.bumpDir(req.Dir)
	return &MknodResponse{Inode: id}, nil
}

// Link binds another name to an existing inode. Both lease domains move
// (§10.5): the directory gained a name, and the inode's nlink changed.
func (s *Server) Link(ctx context.Context, req *LinkRequest) (*LinkResponse, error) {
	rec, err := s.db.Link(req.Dir, req.Name, req.Target)
	if err != nil {
		return nil, toStatus(err)
	}
	if s.coh != nil {
		s.coh.Bump(inodeObj(req.Target))
	}
	s.bumpDir(req.Dir)
	return &LinkResponse{Record: rec}, nil
}

func (s *Server) Symlink(ctx context.Context, req *SymlinkRequest) (*SymlinkResponse, error) {
	id, err := s.db.CreateSymlink(req.Dir, req.Name, req.Target, req.Uid, req.Gid)
	if err != nil {
		return nil, toStatus(err)
	}
	s.bumpDir(req.Dir)
	return &SymlinkResponse{Inode: id}, nil
}
