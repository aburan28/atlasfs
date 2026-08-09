package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/fuseserver"
	"github.com/aburan28/atlasfs/pkg/repo"
)

func cmdMount(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: atlas mount <repo-dir> <mountpoint>")
	}
	repoDir, mountpoint := args[0], args[1]

	if _, err := os.Stat(mountpoint); err != nil {
		return fmt.Errorf("mountpoint: %w", err)
	}

	r, err := repo.Open(repoDir)
	if err != nil {
		return err
	}
	defer r.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	onMounted := func(server *fuse.Server) {
		fmt.Printf("atlasfs mounted read-only: %s -> %s (Ctrl-C to unmount)\n", repoDir, mountpoint)
		go func() {
			<-sigCh
			_ = server.Unmount()
		}()
	}
	return fuseserver.Mount(ctx, r, mountpoint, onMounted)
}
