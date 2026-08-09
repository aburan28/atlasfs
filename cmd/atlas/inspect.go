package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aburan28/atlasfs/pkg/repo"
)

func cmdLs(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: atlas ls <repo-dir> [path]")
	}
	repoDir := args[0]
	p := "/"
	if len(args) >= 2 {
		p = args[1]
	}
	r, err := repo.Open(repoDir)
	if err != nil {
		return err
	}
	defer r.Close()

	inode, rec, err := r.Resolve(p)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", p, err)
	}
	if !rec.IsDir {
		fmt.Println(p)
		return nil
	}
	entries, err := r.Readdir(inode)
	if err != nil {
		return err
	}
	for _, e := range entries {
		child, _ := r.DB.GetInode(e.Inode)
		kind := "-"
		if child.IsDir {
			kind = "d"
		}
		fmt.Printf("%s %10d  %s\n", kind, child.Size, e.Name)
	}
	return nil
}

func cmdCat(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: atlas cat <repo-dir> <path>")
	}
	r, err := repo.Open(args[0])
	if err != nil {
		return err
	}
	defer r.Close()

	_, rec, err := r.Resolve(args[1])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", args[1], err)
	}
	if rec.IsDir {
		return fmt.Errorf("%s is a directory", args[1])
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		return err
	}
	data, err := fr.ReadAll()
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func cmdStat(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: atlas stat <repo-dir> <path>")
	}
	r, err := repo.Open(args[0])
	if err != nil {
		return err
	}
	defer r.Close()

	inode, rec, err := r.Resolve(args[1])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", args[1], err)
	}
	fmt.Printf("path:        %s\n", args[1])
	fmt.Printf("inode:       %d\n", inode)
	fmt.Printf("is_dir:      %v\n", rec.IsDir)
	fmt.Printf("size:        %d\n", rec.Size)
	fmt.Printf("mtime:       %s\n", rec.MTime)
	fmt.Printf("has_inline:  %v", rec.HasInline)
	if rec.HasInline {
		fmt.Printf(" (%s)", rec.InlineChunk)
	}
	fmt.Println()
	fmt.Printf("has_manifest: %v", rec.HasManifest)
	if rec.HasManifest {
		fmt.Printf(" (%s)", rec.ManifestID)
	}
	fmt.Println()
	return nil
}
