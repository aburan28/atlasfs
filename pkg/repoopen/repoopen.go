// Package repoopen is the one place that turns a generic set of
// key/value parameters into an open *repo.Repo against DESIGN.md §24's
// pluggable backends. It exists so the CLI (cmd/atlas, flags) and the
// CSI node plugin (pkg/csidriver, a PV's VolumeContext map) share the
// exact same backend-selection logic instead of two copies drifting
// apart — the CSI layer gets real backend pluggability, not a
// reimplementation of it.
package repoopen

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/store/local"
	"github.com/aburan28/atlasfs/pkg/store/s3"
)

// Params selects a backend and, for a brand-new repo, its consistency
// class. The zero value is "local" backend, immutable class.
type Params struct {
	Backend    string // "local" | "s3"
	S3Bucket   string
	S3Region   string
	S3Endpoint string
	S3Prefix   string
	Region     string // AtlasFS locator region (DESIGN.md §7.5), not the AWS region

	// Class is only a hint used when repoDir holds no repo yet — see
	// repo.OpenWithClass: reopening an existing repo always returns its
	// persisted class regardless of what's passed here. Empty means
	// ClassImmutable.
	Class string
}

// Open opens repoDir's local metadata store and points its object
// storage at whatever Params selects.
func Open(ctx context.Context, repoDir string, p Params) (*repo.Repo, error) {
	region := p.Region
	if region == "" {
		region = repo.DefaultRegion
	}
	class := repo.Class(p.Class)
	if class == "" {
		class = repo.ClassImmutable
	}
	switch p.Backend {
	case "", "local":
		backend, err := local.New(filepath.Join(repoDir, "objects"))
		if err != nil {
			return nil, err
		}
		return repo.OpenWithClass(repoDir, backend, region, class)
	case "s3":
		if p.S3Bucket == "" {
			return nil, fmt.Errorf("repoopen: s3 backend requires a bucket")
		}
		s3Region := p.S3Region
		if s3Region == "" {
			s3Region = "us-east-1"
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
		if err != nil {
			return nil, fmt.Errorf("repoopen: load AWS config: %w", err)
		}
		var optFns []func(*awss3.Options)
		if p.S3Endpoint != "" {
			optFns = append(optFns, func(o *awss3.Options) {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(p.S3Endpoint)
			})
		}
		backend, err := s3.New(ctx, awsCfg, s3.Config{Bucket: p.S3Bucket, Prefix: p.S3Prefix}, optFns...)
		if err != nil {
			return nil, fmt.Errorf("repoopen: open s3 backend: %w", err)
		}
		return repo.OpenWithClass(repoDir, backend, region, class)
	default:
		return nil, fmt.Errorf("repoopen: unknown backend %q (want local|s3)", p.Backend)
	}
}

// Known parameter keys used by ParamsFromMap — the CSI VolumeContext
// convention this driver defines (DESIGN.md §22.1's volumeAttributes).
const (
	KeyRepoPath   = "repoPath" // required: local directory holding the repo's metadata on this node
	KeyBackend    = "backend"
	KeyS3Bucket   = "s3Bucket"
	KeyS3Region   = "s3Region"
	KeyS3Endpoint = "s3Endpoint"
	KeyS3Prefix   = "s3Prefix"
	KeyRegion     = "region"
	KeyClass      = "class"
)

// ParamsFromMap reads repoDir and Params out of a generic string map —
// a PV's VolumeContext in the CSI case, matching the same key set
// openRepo's flags accept on the CLI.
func ParamsFromMap(m map[string]string) (repoDir string, p Params, err error) {
	repoDir = m[KeyRepoPath]
	if repoDir == "" {
		return "", Params{}, fmt.Errorf("repoopen: missing required parameter %q", KeyRepoPath)
	}
	p = Params{
		Backend:    m[KeyBackend],
		S3Bucket:   m[KeyS3Bucket],
		S3Region:   m[KeyS3Region],
		S3Endpoint: m[KeyS3Endpoint],
		S3Prefix:   m[KeyS3Prefix],
		Region:     m[KeyRegion],
		Class:      m[KeyClass],
	}
	return repoDir, p, nil
}
