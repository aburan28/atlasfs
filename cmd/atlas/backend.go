package main

import (
	"context"
	"flag"

	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/repoopen"
)

// backendFlags is the CLI's surface for DESIGN.md §24's pluggable
// backends; it just fills a repoopen.Params, the same struct the CSI
// node plugin (pkg/csidriver) fills from a PV's VolumeContext, so both
// entry points share one backend-selection implementation.
type backendFlags struct {
	kind       string
	s3Bucket   string
	s3Region   string
	s3Endpoint string
	s3Prefix   string
	region     string
	class      string
}

func addBackendFlags(fs *flag.FlagSet, bf *backendFlags) {
	fs.StringVar(&bf.kind, "backend", "local", "storage backend: local|s3")
	fs.StringVar(&bf.s3Bucket, "s3-bucket", "", "S3 bucket name (--backend=s3)")
	fs.StringVar(&bf.s3Region, "s3-region", "us-east-1", "AWS region for the S3 client (--backend=s3)")
	fs.StringVar(&bf.s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint override, e.g. http://localhost:9000 (--backend=s3)")
	fs.StringVar(&bf.s3Prefix, "s3-prefix", "", "key prefix within the bucket (--backend=s3)")
	fs.StringVar(&bf.region, "region", repo.DefaultRegion, "AtlasFS region label for chunk locators (DESIGN.md §7.5) — not the AWS region")
	fs.StringVar(&bf.class, "class", string(repo.ClassImmutable),
		"consistency class for a brand-new repo (DESIGN.md §8): immutable|relaxed|session. Ignored when repo-dir already holds a repo — its persisted class always wins.")
}

// openRepo opens repoDir's metadata locally and points its object
// storage at whatever backend bf selects. AWS credentials for --backend=s3
// follow the SDK's normal resolution chain (env vars, shared config,
// IMDS) via awsconfig.LoadDefaultConfig — this CLI never takes a
// secret key as a flag.
func openRepo(ctx context.Context, repoDir string, bf backendFlags) (*repo.Repo, error) {
	return repoopen.Open(ctx, repoDir, repoopen.Params{
		Backend:    bf.kind,
		S3Bucket:   bf.s3Bucket,
		S3Region:   bf.s3Region,
		S3Endpoint: bf.s3Endpoint,
		S3Prefix:   bf.s3Prefix,
		Region:     bf.region,
		Class:      bf.class,
	})
}
