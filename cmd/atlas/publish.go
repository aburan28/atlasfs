package main

import (
	"context"
	"flag"
	"fmt"
	"time"
)

func cmdPublish(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	fs.Usage = func() {
		fmt.Println("usage: atlas publish [flags] <repo-dir> <src-dir> [dest-path]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) < 2 {
		fs.Usage()
		return fmt.Errorf("usage: atlas publish [flags] <repo-dir> <src-dir> [dest-path]")
	}
	repoDir, srcDir := pos[0], pos[1]
	dest := "/"
	if len(pos) >= 3 {
		dest = pos[2]
	}

	r, err := openRepo(ctx, repoDir, bf)
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
