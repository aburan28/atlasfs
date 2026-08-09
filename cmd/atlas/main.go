// Command atlas is the AtlasFS CLI for the Phase-1 vertical slice: a
// single-node, single-region, `immutable`-class repo (DESIGN.md §28
// Phase 1). It can publish a directory tree into content-addressed
// storage and mount it read-only over FUSE.
package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "publish":
		err = cmdPublish(ctx, os.Args[2:])
	case "ls":
		err = cmdLs(ctx, os.Args[2:])
	case "cat":
		err = cmdCat(ctx, os.Args[2:])
	case "stat":
		err = cmdStat(ctx, os.Args[2:])
	case "mount":
		err = cmdMount(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "atlas: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "atlas: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `atlas — AtlasFS single-node repo tool (Phase 1: immutable class only)

Usage:
  atlas publish <repo-dir> <src-dir> [dest-path]   ingest a directory tree, content-addressed
  atlas ls      <repo-dir> [path]                  list a directory
  atlas cat     <repo-dir> <path>                  print a file's content to stdout
  atlas stat    <repo-dir> <path>                  print an inode's metadata
  atlas mount   <repo-dir> <mountpoint>             read-only FUSE mount (foreground)

repo-dir is created on first publish. See DESIGN.md §5/§16 for the format.
`)
}
