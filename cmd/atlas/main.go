// Command atlas is the AtlasFS CLI: a single-node, single-region repo
// (DESIGN.md §28 Phases 1-2). It can publish a directory tree into
// content-addressed storage and mount it over FUSE — read-only for the
// `immutable` class, read-write for `relaxed`/`session`.
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
	case "gc":
		err = cmdGC(ctx, os.Args[2:])
	case "quota":
		err = cmdQuota(ctx, os.Args[2:])
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
	fmt.Fprint(os.Stderr, `atlas — AtlasFS single-node repo tool

Usage:
  atlas publish [flags] <repo-dir> <src-dir> [dest-path]   ingest a directory tree, content-addressed
  atlas ls      [flags] <repo-dir> [path]                  list a directory
  atlas cat     [flags] <repo-dir> <path>                  print a file's content to stdout
  atlas stat    [flags] <repo-dir> <path>                  print an inode's metadata
  atlas mount   [flags] <repo-dir> <mountpoint>             FUSE mount (foreground; read-write iff -class isn't immutable)
  atlas gc      [flags] <repo-dir>                         mark-and-sweep + compact (DESIGN.md §19)
  atlas quota   [flags] <repo-dir>                         show or set the repo's quota (DESIGN.md §18.3)

repo-dir is created on first publish; metadata always lives there locally
(DESIGN.md §24.5's single-node metadb stand-in for per-region FDB).

Flags (every subcommand, DESIGN.md §24 pluggable backends and §8 classes):
  -backend local|s3|gcs|azure
                          object storage backend (default local)
  -s3-bucket NAME         S3 bucket (required for -backend=s3)
  -s3-region REGION       AWS region for the S3 client (default us-east-1)
  -s3-endpoint URL        S3-compatible endpoint override, e.g. MinIO
  -s3-prefix PREFIX       key prefix within the bucket
  -gcs-bucket NAME        GCS bucket (required for -backend=gcs)
  -azure-service-url URL  Azure blob endpoint, optionally with a SAS query string
  -azure-container NAME   Azure container (required for -backend=azure)
  -region NAME            AtlasFS locator region label (default local)
  -class immutable|relaxed|session
                          consistency class for a brand-new repo (default immutable);
                          ignored on reopen — the repo's persisted class always wins

AWS credentials for -backend=s3 follow the SDK's normal chain (env vars,
shared config, IMDS) — never passed as a flag.

See DESIGN.md §5/§16 for the on-disk format, §8 for consistency classes.
`)
}
