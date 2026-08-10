package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aburan28/atlasfs/pkg/fuseserver"
	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/mdsfuse"
	"github.com/aburan28/atlasfs/pkg/repoopen"
)

func cmdMount(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	mdsAddr := fs.String("mds", "",
		"mount against a remote metadata authority (cmd/atlas-mds) at this address instead of opening the repo's metadata locally. The repo-dir argument then supplies only the object backend.")
	allowOther := fs.Bool("allow-other", false,
		"let users other than the one mounting reach the filesystem. Without it the kernel refuses every access from a different uid before any permission check runs. Needs root or user_allow_other in /etc/fuse.conf.")
	fsName := fs.String("fsname", "",
		"source name to report in /proc/mounts (default atlasfs). mount(8) and findmnt identify a mount by the device string they were given, so a mount helper needs to set this.")
	mdsReadOnly := fs.Bool("mds-read-only", false,
		"refuse writes on an -mds mount. Use it for an immutable subtree; otherwise the mount is read-write and commits through the authority (DESIGN.md §16.1).")
	fs.Usage = func() {
		fmt.Println("usage: atlas mount [flags] <repo-dir> <mountpoint>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) < 2 {
		fs.Usage()
		return fmt.Errorf("usage: atlas mount [flags] <repo-dir> <mountpoint>")
	}
	repoDir, mountpoint := pos[0], pos[1]

	if _, err := os.Stat(mountpoint); err != nil {
		return fmt.Errorf("mountpoint: %w", err)
	}

	if *mdsAddr != "" {
		return mountViaMDS(ctx, *mdsAddr, repoDir, mountpoint, bf, *mdsReadOnly, *allowOther, *fsName)
	}

	r, err := openRepo(ctx, repoDir, bf)
	if err != nil {
		return err
	}
	defer r.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	onMounted := func(server *fuse.Server) {
		mode := "read-only"
		if r.Class.Mutable() {
			mode = fmt.Sprintf("read-write (%s class)", r.Class)
		}
		fmt.Printf("atlasfs mounted %s: %s -> %s (Ctrl-C to unmount)\n", mode, repoDir, mountpoint)
		go func() {
			<-sigCh
			_ = server.Unmount()
		}()
	}
	var mountOpts []fuseserver.MountOption
	if *allowOther {
		mountOpts = append(mountOpts, fuseserver.AllowOther())
	}
	if *fsName != "" {
		mountOpts = append(mountOpts, fuseserver.FsName(*fsName))
	}
	return fuseserver.Mount(ctx, r, mountpoint, onMounted, mountOpts...)
}

// mountViaMDS mounts against a remote metadata authority. The repo
// directory still supplies the object backend, because DESIGN.md §11
// keeps chunk bytes out of the authority's path entirely — the mount
// talks to atlas-mds for inodes, dentries and locators, and to object
// storage for everything else.
func mountViaMDS(ctx context.Context, addr, repoDir, mountpoint string, bf backendFlags, readOnly, allowOther bool, fsName string) error {
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), mds.DialOption())
	if err != nil {
		return fmt.Errorf("connect to metadata authority %s: %w", addr, err)
	}
	defer cc.Close()

	holder, err := os.Hostname()
	if err != nil || holder == "" {
		holder = "atlas-mount"
	}
	// One holder identity per mount, not per host: two mounts on one
	// machine are two independent lease holders, and sharing an identity
	// would let one mount's recall ack speak for the other's cache.
	holder = fmt.Sprintf("%s/%d", holder, os.Getpid())

	client := mds.NewClient(cc, holder, nil)
	defer client.Close()
	// The push stream is what makes invalidation prompt; without it the
	// mount is still correct, just bounded by lease expiry (§10.2).
	if err := client.Subscribe(ctx); err != nil {
		return fmt.Errorf("subscribe to invalidations: %w", err)
	}

	backend, err := repoopen.OpenBackend(ctx, repoDir, bf.params())
	if err != nil {
		return err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	onMounted := func(server *fuse.Server) {
		mode := "read-write"
		if readOnly {
			mode = "read-only"
		}
		fmt.Printf("atlasfs mounted %s via authority %s as holder %q: %s (Ctrl-C to unmount)\n",
			mode, addr, holder, mountpoint)
		go func() {
			<-sigCh
			_ = server.Unmount()
		}()
	}
	return mdsfuse.Mount(ctx, mdsfuse.Config{
		Client:     client,
		Backend:    backend,
		ReadOnly:   readOnly,
		AllowOther: allowOther,
		FsName:     fsName,
		Region:     bf.region,
		// Conservative: without asking the authority for the subtree's
		// class, assume the strictest thing a mount might be serving and
		// let the kernel cache nothing. Plumbing the class through the
		// protocol so this can relax is follow-up.
		KernelCacheTTL: 0,
	}, mountpoint, onMounted)
}
