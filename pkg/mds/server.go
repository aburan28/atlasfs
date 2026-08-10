package mds

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

	Clock coherence.Clock
}

// Server is one region's metadata authority.
type Server struct {
	db       *metadb.DB
	coh      *coherence.Manager
	posix    bool
	drecall  time.Duration
	leaseTTL time.Duration

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
		streams:  map[string]*holderStream{},
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
		s.coh.BumpDir(dirObj(req.Dir))
	}
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
		s.coh.BumpDir(dirObj(req.Dir))
	}
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

func (s *Server) AckRecall(ctx context.Context, req *AckRecallRequest) (*AckRecallResponse, error) {
	if s.coh != nil {
		s.coh.AckRecall(req.RecallID, req.Holder)
	}
	return &AckRecallResponse{}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, metadb.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, metadb.ErrExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, metadb.ErrNotDir), errors.Is(err, metadb.ErrIsDirectory):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, metadb.ErrQuotaExceeded):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
