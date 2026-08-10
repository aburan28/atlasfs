package mds

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/coherence"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/pack"
)

// Client is one holder: a process that caches metadata under leases
// granted by a Server, and gives those leases up when told to.
//
// The cache below is the client half of DESIGN.md §10, and the two rules
// it follows are the ones that make the whole protocol work:
//
//   - An entry is trusted only while `clock.Now()` is inside the window
//     the client itself computed from the lease's TTL (§10.7). The
//     authority's clock never enters the decision, so clock skew between
//     the two cannot produce a stale read.
//   - A push arriving on the stream drops the entry early. That is the
//     only thing push does. Losing every push message costs
//     responsiveness and nothing else, which is why the tests can drop
//     the stream entirely and still assert correctness.
type Client struct {
	cc     *grpc.ClientConn
	holder string
	clock  coherence.Clock

	mu    sync.Mutex
	cache map[string]*cacheEntry
	// dentries[dirObj][name] — cached name->inode bindings, held under
	// the *directory's* lease rather than the inode's (§10.5's two
	// domains). Without this every path resolution round-trips, which
	// makes the inode lease cache nearly unreachable through a
	// filesystem: the kernel does a LOOKUP before almost everything.
	dentries map[string]map[string]metadb.InodeID
	// negatives[dirObj][name] — names known absent as of the directory's
	// lease (§10.4). Validated against the directory rather than per
	// name, which is what lets one dirver bump clear every miss at once.
	negatives map[string]map[string]struct{}
	// epochs[obj] counts invalidations of obj. A response that was in
	// flight while obj was invalidated carries a pre-invalidation view,
	// and storing it would resurrect exactly what the invalidation
	// removed — so every store captures the epoch before its RPC and
	// drops the result if the counter moved underneath it.
	epochs map[string]uint64

	// onRecall, when set, runs before the client acks a recall. Tests use
	// it to observe ordering; a real client drops any derived state here.
	onRecall func(object string)

	streamCancel context.CancelFunc
	streamDone   chan struct{}
}

type cacheEntry struct {
	record  metadb.InodeRecord
	version uint64
	expiry  time.Time
	// noExpiry marks an `immutable`-class lease: nothing can invalidate
	// it because nothing can change (DESIGN.md §8.2's republish caveat is
	// handled a layer up, by not reusing a Client across a republish).
	noExpiry bool
}

// NewClient wraps an established connection. holder must be unique per
// process — it is the identity the authority grants leases to and
// recalls them from.
func NewClient(cc *grpc.ClientConn, holder string, clock coherence.Clock) *Client {
	if clock == nil {
		clock = coherence.RealClock{}
	}
	return &Client{
		cc:        cc,
		holder:    holder,
		clock:     clock,
		cache:     map[string]*cacheEntry{},
		dentries:  map[string]map[string]metadb.InodeID{},
		negatives: map[string]map[string]struct{}{},
		epochs:    map[string]uint64{},
	}
}

// DialOption returns the call options a connection to this service needs.
// The content-subtype is what selects this package's codec; without it
// grpc-go would try to marshal these plain structs as protobuf.
func DialOption() grpc.DialOption {
	return grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName))
}

func (c *Client) Holder() string { return c.holder }

// SetRecallHook installs a callback invoked when a recall arrives, before
// the ack is sent.
func (c *Client) SetRecallHook(f func(object string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onRecall = f
}

// CachedInode returns a locally cached record if this client's lease on
// it is still valid *by its own clock*, without contacting the server.
func (c *Client) CachedInode(id metadb.InodeID) (metadb.InodeRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.cache[inodeObj(id)]
	if e == nil {
		return metadb.InodeRecord{}, false
	}
	if !e.noExpiry && c.clock.Now().After(e.expiry) {
		return metadb.InodeRecord{}, false
	}
	return e.record, true
}

// GetInode returns the record for id, from cache when the lease permits
// and from the authority otherwise.
func (c *Client) GetInode(ctx context.Context, id metadb.InodeID) (metadb.InodeRecord, error) {
	if rec, ok := c.CachedInode(id); ok {
		return rec, nil
	}
	since := c.epochOf(inodeObj(id))
	var resp GetInodeResponse
	if err := c.cc.Invoke(ctx, MethodGetInode, &GetInodeRequest{Holder: c.holder, Inode: id}, &resp); err != nil {
		return metadb.InodeRecord{}, err
	}
	c.store(inodeObj(id), resp.Record, resp.Lease, since)
	return resp.Record, nil
}

// Lookup resolves a name in a directory, answering from cache when both
// halves are still leased: the dentry binding under the directory's
// lease and the inode's record under its own. Either one lapsing or
// being invalidated sends the call back to the authority.
func (c *Client) Lookup(ctx context.Context, dir metadb.InodeID, name string) (LookupResponse, error) {
	if ino, rec, ok := c.cachedDentry(dir, name); ok {
		return LookupResponse{Found: true, Inode: ino, Record: rec}, nil
	}
	if c.cachedNegative(dir, name) {
		return LookupResponse{}, nil
	}
	sinceDir := c.epochOf(dirObj(dir))
	var resp LookupResponse
	err := c.cc.Invoke(ctx, MethodLookup, &LookupRequest{Holder: c.holder, Dir: dir, Name: name}, &resp)
	switch {
	case err != nil:
	case resp.Found:
		c.store(inodeObj(resp.Inode), resp.Record, resp.Lease, c.epochOf(inodeObj(resp.Inode)))
		if resp.DirLease.TTL > 0 {
			c.storeDentry(dir, name, resp.Inode, resp.DirLease, sinceDir)
		}
	case resp.DirLease.TTL > 0:
		c.storeNegative(dir, name, resp.DirLease, sinceDir)
	}
	return resp, err
}

// cachedDentry answers a lookup locally only if the directory's lease
// still covers the binding *and* the target inode's own lease still
// covers its record. Trusting the binding alone would serve a stale
// record for a file whose attrs were invalidated but whose name never
// moved.
func (c *Client) cachedDentry(dir metadb.InodeID, name string) (metadb.InodeID, metadb.InodeRecord, bool) {
	c.mu.Lock()
	dirEntry := c.cache[dirObj(dir)]
	if dirEntry == nil || (!dirEntry.noExpiry && c.clock.Now().After(dirEntry.expiry)) {
		c.mu.Unlock()
		return 0, metadb.InodeRecord{}, false
	}
	ino, ok := c.dentries[dirObj(dir)][name]
	c.mu.Unlock()
	if !ok {
		return 0, metadb.InodeRecord{}, false
	}
	rec, ok := c.CachedInode(ino)
	if !ok {
		return 0, metadb.InodeRecord{}, false
	}
	return ino, rec, true
}

func (c *Client) storeDentry(dir metadb.InodeID, name string, ino metadb.InodeID, dirLease Lease, since uint64) {
	c.store(dirObj(dir), metadb.InodeRecord{IsDir: true}, dirLease, since)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epochs[dirObj(dir)] != since {
		return
	}
	m := c.dentries[dirObj(dir)]
	if m == nil {
		m = map[string]metadb.InodeID{}
		c.dentries[dirObj(dir)] = m
	}
	m[name] = ino
}

// cachedNegative answers "this name is absent" locally, for as long as
// the directory's own lease holds. DESIGN.md §10.4: a miss is cached
// against the directory version, so any mutation under the directory
// clears every negative entry at once rather than needing a message per
// name.
func (c *Client) cachedNegative(dir metadb.InodeID, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.cache[dirObj(dir)]
	if e == nil || (!e.noExpiry && c.clock.Now().After(e.expiry)) {
		return false
	}
	_, ok := c.negatives[dirObj(dir)][name]
	return ok
}

func (c *Client) storeNegative(dir metadb.InodeID, name string, dirLease Lease, since uint64) {
	c.store(dirObj(dir), metadb.InodeRecord{IsDir: true}, dirLease, since)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epochs[dirObj(dir)] != since {
		return
	}
	m := c.negatives[dirObj(dir)]
	if m == nil {
		m = map[string]struct{}{}
		c.negatives[dirObj(dir)] = m
	}
	m[name] = struct{}{}
}

// epochOf reads obj's invalidation counter, to be captured before an RPC
// and handed back to store: see the epochs field.
func (c *Client) epochOf(obj string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epochs[obj]
}

func (c *Client) Readdir(ctx context.Context, dir metadb.InodeID) ([]metadb.DirEntry, error) {
	var resp ReaddirResponse
	if err := c.cc.Invoke(ctx, MethodReaddir, &ReaddirRequest{Holder: c.holder, Dir: dir}, &resp); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

func (c *Client) Commit(ctx context.Context, dir metadb.InodeID, name string, rec metadb.InodeRecord) (CommitResponse, error) {
	var resp CommitResponse
	err := c.cc.Invoke(ctx, MethodCommit, &CommitRequest{Holder: c.holder, Dir: dir, Name: name, Record: rec}, &resp)
	if err == nil {
		c.invalidate(inodeObj(resp.Inode))
		c.invalidateOwnMutation(dir)
	}
	return resp, err
}

func (c *Client) Unlink(ctx context.Context, dir metadb.InodeID, name string) error {
	var resp UnlinkResponse
	err := c.cc.Invoke(ctx, MethodUnlink, &UnlinkRequest{Holder: c.holder, Dir: dir, Name: name}, &resp)
	if err == nil {
		c.invalidateOwnMutation(dir)
	}
	return err
}

// invalidateOwnMutation drops this holder's own cached view of a
// directory it just changed.
//
// This is not redundant with the push. A holder is deliberately excluded
// from its own recall (§10.6: the mutator does not recall itself), and
// the invalidation push is asynchronous besides — so without this a
// client would keep answering from a dentry cache its own rmdir just
// invalidated, and read-your-own-writes would break on the one holder
// most certain to notice. CI caught exactly that: a stat right after a
// successful rmdir still resolved the name.
func (c *Client) invalidateOwnMutation(dir metadb.InodeID) {
	c.invalidate(dirObj(dir))
}

func (c *Client) store(obj string, rec metadb.InodeRecord, l Lease, since uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epochs[obj] != since {
		// Invalidated while this response was in flight: the lease it
		// carries was granted against a view that no longer exists.
		return
	}
	e := &cacheEntry{record: rec, version: l.Version}
	if l.TTL <= 0 {
		e.noExpiry = true
	} else {
		// Measured from now, on this client's clock — never from a
		// server-supplied timestamp (§10.7).
		e.expiry = c.clock.Now().Add(l.TTL)
	}
	c.cache[obj] = e
}

func (c *Client) invalidate(obj string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epochs[obj]++
	delete(c.cache, obj)
	// A directory invalidation drops every dentry cached under it in one
	// step — the bulk property §10.4 relies on, applied to both the
	// positive bindings and the negative ones.
	delete(c.dentries, obj)
	delete(c.negatives, obj)
}

// Subscribe opens the push stream and services it until the context is
// cancelled or Close is called. It returns once the stream is
// established, having spawned the reader; errors after that are the
// protocol's business, not the caller's, because losing the stream is
// explicitly survivable (§10.2).
func (c *Client) Subscribe(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.cc.NewStream(ctx, &serviceDesc.Streams[0], MethodSubscribe)
	if err != nil {
		cancel()
		return err
	}
	if err := stream.SendMsg(&SubscribeRequest{Holder: c.holder}); err != nil {
		cancel()
		return err
	}
	if err := stream.CloseSend(); err != nil {
		cancel()
		return err
	}

	done := make(chan struct{})
	c.mu.Lock()
	c.streamCancel, c.streamDone = cancel, done
	c.mu.Unlock()

	// Wait for the server to accept the stream before returning, so a
	// caller that immediately mutates from another client cannot race
	// ahead of this holder's registration.
	ready := make(chan struct{})
	go func() {
		defer close(done)
		close(ready)
		for {
			var ev Event
			if err := stream.RecvMsg(&ev); err != nil {
				if err == io.EOF {
					return
				}
				return
			}
			c.handleEvent(ev)
		}
	}()
	<-ready
	return nil
}

func (c *Client) handleEvent(ev Event) {
	c.invalidate(ev.Object)
	if ev.Kind != EventRecall {
		return
	}
	c.mu.Lock()
	hook := c.onRecall
	c.mu.Unlock()
	if hook != nil {
		hook(ev.Object)
	}
	// Ack only after the local copy is gone. Acking first would tell the
	// writer it is safe to commit while this holder could still answer a
	// read from the entry it has not yet dropped.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp AckRecallResponse
	_ = c.cc.Invoke(ctx, MethodAckRecall, &AckRecallRequest{Holder: c.holder, RecallID: ev.RecallID}, &resp)
}

// Close stops the push stream. The connection itself belongs to the
// caller.
func (c *Client) Close() {
	c.mu.Lock()
	cancel, done := c.streamCancel, c.streamDone
	c.streamCancel, c.streamDone = nil, nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// GetLocator resolves a chunk to its container placement. The bytes
// themselves are then fetched straight from store.Backend — the
// authority is never in the data path (DESIGN.md §11).
func (c *Client) GetLocator(ctx context.Context, id chunk.ID) (pack.Locator, bool, error) {
	var resp GetLocatorResponse
	err := c.cc.Invoke(ctx, MethodGetLocator, &GetLocatorRequest{Holder: c.holder, ChunkID: id}, &resp)
	if err != nil {
		return pack.Locator{}, false, err
	}
	return resp.Locator, resp.Found, nil
}

// Resolve walks a "/"-rooted path to its inode and record, one Lookup
// per component. Each component's answer is cached under its own lease,
// so a repeated walk down the same directory chain costs nothing after
// the first — which is what makes a per-component walk affordable
// instead of needing a batched server-side resolve.
func (c *Client) Resolve(ctx context.Context, p string) (metadb.InodeID, metadb.InodeRecord, error) {
	cur := metadb.RootInode
	rec, err := c.GetInode(ctx, cur)
	if err != nil {
		return 0, metadb.InodeRecord{}, err
	}
	for _, name := range splitPath(p) {
		resp, err := c.Lookup(ctx, cur, name)
		if err != nil {
			return 0, metadb.InodeRecord{}, err
		}
		if !resp.Found {
			return 0, metadb.InodeRecord{}, fmt.Errorf("mds: %q: %w", p, metadb.ErrNotFound)
		}
		cur, rec = resp.Inode, resp.Record
	}
	return cur, rec, nil
}

func splitPath(p string) []string {
	p = strings.Trim(path.Clean("/"+p), "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// PutLocator registers a sealed chunk's placement with the authority.
func (c *Client) PutLocator(ctx context.Context, id chunk.ID, loc pack.Locator) error {
	var resp PutLocatorResponse
	return c.cc.Invoke(ctx, MethodPutLocator, &PutLocatorRequest{Holder: c.holder, ChunkID: id, Locator: loc}, &resp)
}

// HasLocator is the write path's dedup check: skip uploading a chunk the
// region already has (DESIGN.md §16.1 step 2).
func (c *Client) HasLocator(ctx context.Context, id chunk.ID) (bool, error) {
	var resp HasLocatorResponse
	if err := c.cc.Invoke(ctx, MethodHasLocator, &HasLocatorRequest{Holder: c.holder, ChunkID: id}, &resp); err != nil {
		return false, err
	}
	return resp.Found, nil
}

func (c *Client) Mkdir(ctx context.Context, dir metadb.InodeID, name string) (metadb.InodeID, error) {
	var resp MkdirResponse
	err := c.cc.Invoke(ctx, MethodMkdir, &MkdirRequest{Holder: c.holder, Dir: dir, Name: name}, &resp)
	if err == nil {
		c.invalidateOwnMutation(dir)
	}
	return resp.Inode, err
}

func (c *Client) Rmdir(ctx context.Context, dir metadb.InodeID, name string) error {
	var resp RmdirResponse
	err := c.cc.Invoke(ctx, MethodRmdir, &RmdirRequest{Holder: c.holder, Dir: dir, Name: name}, &resp)
	if err == nil {
		c.invalidateOwnMutation(dir)
	}
	return err
}

func (c *Client) Rename(ctx context.Context, oldDir metadb.InodeID, oldName string, newDir metadb.InodeID, newName string) error {
	var resp RenameResponse
	err := c.cc.Invoke(ctx, MethodRename, &RenameRequest{
		Holder: c.holder, OldDir: oldDir, OldName: oldName, NewDir: newDir, NewName: newName,
	}, &resp)
	if err == nil {
		c.invalidateOwnMutation(oldDir)
		if newDir != oldDir {
			c.invalidateOwnMutation(newDir)
		}
	}
	return err
}

// Statfs asks the authority for the subtree's quota and usage.
func (c *Client) Statfs(ctx context.Context) (StatfsResponse, error) {
	var resp StatfsResponse
	err := c.cc.Invoke(ctx, MethodStatfs, &StatfsRequest{Holder: c.holder}, &resp)
	return resp, err
}

// Link binds another name to target and returns its updated record.
func (c *Client) Link(ctx context.Context, dir metadb.InodeID, name string, target metadb.InodeID) (metadb.InodeRecord, error) {
	var resp LinkResponse
	err := c.cc.Invoke(ctx, MethodLink, &LinkRequest{Holder: c.holder, Dir: dir, Name: name, Target: target}, &resp)
	if err == nil {
		c.invalidate(inodeObj(target))
		c.invalidateOwnMutation(dir)
	}
	return resp.Record, err
}

func (c *Client) Symlink(ctx context.Context, dir metadb.InodeID, name, target string) (metadb.InodeID, error) {
	var resp SymlinkResponse
	err := c.cc.Invoke(ctx, MethodSymlink, &SymlinkRequest{Holder: c.holder, Dir: dir, Name: name, Target: target}, &resp)
	if err == nil {
		c.invalidateOwnMutation(dir)
	}
	return resp.Inode, err
}
