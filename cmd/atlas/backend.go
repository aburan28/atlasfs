package main

import (
	"context"
	"flag"

	"github.com/aburan28/atlasfs/pkg/pack"
	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/repoopen"
	"github.com/aburan28/atlasfs/pkg/store/dragonfly"
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

	gcsBucket string
	gcsPrefix string

	azureServiceURL string
	azureContainer  string
	azurePrefix     string

	dragonflyProxy string
	dragonflyTag   string
	gdsAlign       bool

	region string
	class  string
}

func addBackendFlags(fs *flag.FlagSet, bf *backendFlags) {
	fs.StringVar(&bf.kind, "backend", "local", "storage backend: local|s3|gcs|azure")
	fs.StringVar(&bf.s3Bucket, "s3-bucket", "", "S3 bucket name (--backend=s3)")
	fs.StringVar(&bf.s3Region, "s3-region", "us-east-1", "AWS region for the S3 client (--backend=s3)")
	fs.StringVar(&bf.s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint override, e.g. http://localhost:9000 (--backend=s3)")
	fs.StringVar(&bf.s3Prefix, "s3-prefix", "", "key prefix within the bucket (--backend=s3)")
	fs.StringVar(&bf.gcsBucket, "gcs-bucket", "", "GCS bucket name (--backend=gcs)")
	fs.StringVar(&bf.gcsPrefix, "gcs-prefix", "", "key prefix within the bucket (--backend=gcs)")
	fs.StringVar(&bf.azureServiceURL, "azure-service-url", "", "account blob endpoint, optionally with a SAS query string (--backend=azure); this build is anonymous/SAS-only, no AAD credentials")
	fs.StringVar(&bf.azureContainer, "azure-container", "", "container name (--backend=azure)")
	fs.StringVar(&bf.azurePrefix, "azure-prefix", "", "key prefix within the container (--backend=azure)")
	fs.StringVar(&bf.dragonflyProxy, "dragonfly-proxy", "",
		"route object GETs through a Dragonfly (d7y.io) peer for P2P chunk exchange (DESIGN.md §11.1); \"auto\" uses "+dragonfly.DefaultProxyURL+". Applies to -backend=s3.")
	fs.StringVar(&bf.dragonflyTag, "dragonfly-tag", "", "X-Dragonfly-Tag, separating otherwise-identical URLs into distinct P2P tasks")
	fs.BoolVar(&bf.gdsAlign, "gds-align", false,
		"pad every chunk to a 4 KiB boundary within its container so GPUDirect Storage reads are aligned (pack.GDSAlignment). Costs container space — ~4x for 1 KiB files — so enable it only where GPUs read the data.")
	fs.StringVar(&bf.region, "region", repo.DefaultRegion, "AtlasFS region label for chunk locators (DESIGN.md §7.5) — not the cloud provider's region")
	fs.StringVar(&bf.class, "class", string(repo.ClassImmutable),
		"consistency class for a brand-new repo (DESIGN.md §8): immutable|relaxed|session. Ignored when repo-dir already holds a repo — its persisted class always wins. posix is served by atlas-mds, not by a local mount.")
}

// openRepo opens repoDir's metadata locally and points its object
// storage at whatever backend bf selects. AWS credentials for --backend=s3
// follow the SDK's normal resolution chain (env vars, shared config,
// IMDS) via awsconfig.LoadDefaultConfig — this CLI never takes a
// secret key as a flag.
func openRepo(ctx context.Context, repoDir string, bf backendFlags) (*repo.Repo, error) {
	r, err := openRepoInner(ctx, repoDir, bf)
	if err != nil {
		return nil, err
	}
	if bf.gdsAlign {
		r.SetChunkAlignment(pack.GDSAlignment)
	}
	return r, nil
}

func openRepoInner(ctx context.Context, repoDir string, bf backendFlags) (*repo.Repo, error) {
	return repoopen.Open(ctx, repoDir, bf.params())
}

// params is the single translation from CLI flags to repoopen.Params,
// shared by the repo-opening and backend-only paths so the two cannot
// drift.
func (bf backendFlags) params() repoopen.Params {
	return repoopen.Params{
		Backend:    bf.kind,
		S3Bucket:   bf.s3Bucket,
		S3Region:   bf.s3Region,
		S3Endpoint: bf.s3Endpoint,
		S3Prefix:   bf.s3Prefix,

		GCSBucket: bf.gcsBucket,
		GCSPrefix: bf.gcsPrefix,

		AzureServiceURL: bf.azureServiceURL,
		AzureContainer:  bf.azureContainer,
		AzurePrefix:     bf.azurePrefix,

		DragonflyProxy: bf.dragonflyProxy,
		DragonflyTag:   bf.dragonflyTag,

		Region: bf.region,
		Class:  bf.class,
	}
}
