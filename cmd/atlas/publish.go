package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aburan28/atlasfs/pkg/repo"
)

func cmdPublish(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: atlas publish <repo-dir> <src-dir> [dest-path]")
	}
	repoDir, srcDir := args[0], args[1]
	dest := "/"
	if len(args) >= 3 {
		dest = args[2]
	}

	r, err := repo.Open(repoDir)
	if err != nil {
		return err
	}
	defer r.Close()

	start := time.Now()
	files, dirs, bytesIn, err := r.PublishTree(ctx, srcDir, splitPath(dest))
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	mb := float64(bytesIn) / (1 << 20)
	fmt.Printf("published %d files, %d dirs, %.2f MiB in %s (%.1f MiB/s)\n",
		files, dirs, mb, elapsed.Round(time.Millisecond), mb/elapsed.Seconds())
	return nil
}
