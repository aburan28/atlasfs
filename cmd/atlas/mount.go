package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/fuseserver"
)

func cmdMount(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
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
	return fuseserver.Mount(ctx, r, mountpoint, onMounted)
}
