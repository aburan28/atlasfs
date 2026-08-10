package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

func cmdLs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	fs.Usage = func() {
		fmt.Println("usage: atlas ls [flags] <repo-dir> [path]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) < 1 {
		fs.Usage()
		return fmt.Errorf("usage: atlas ls [flags] <repo-dir> [path]")
	}
	repoDir := pos[0]
	p := "/"
	if len(pos) >= 2 {
		p = pos[1]
	}
	r, err := openRepo(ctx, repoDir, bf)
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
		switch {
		case child.IsDir:
			kind = "d"
		case child.IsSymlink:
			kind = "l"
		}
		suffix := ""
		if child.IsSymlink {
			suffix = " -> " + child.SymlinkTarget
		}
		fmt.Printf("%s %10d  %s%s\n", kind, child.Size, e.Name, suffix)
	}
	return nil
}

func cmdCat(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cat", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	fs.Usage = func() {
		fmt.Println("usage: atlas cat [flags] <repo-dir> <path>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) < 2 {
		fs.Usage()
		return fmt.Errorf("usage: atlas cat [flags] <repo-dir> <path>")
	}
	r, err := openRepo(ctx, pos[0], bf)
	if err != nil {
		return err
	}
	defer r.Close()

	_, rec, err := r.Resolve(pos[1])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", pos[1], err)
	}
	if rec.IsDir {
		return fmt.Errorf("%s is a directory", pos[1])
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
	fs := flag.NewFlagSet("stat", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	fs.Usage = func() {
		fmt.Println("usage: atlas stat [flags] <repo-dir> <path>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) < 2 {
		fs.Usage()
		return fmt.Errorf("usage: atlas stat [flags] <repo-dir> <path>")
	}
	r, err := openRepo(ctx, pos[0], bf)
	if err != nil {
		return err
	}
	defer r.Close()

	inode, rec, err := r.Resolve(pos[1])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", pos[1], err)
	}
	fmt.Printf("path:        %s\n", pos[1])
	fmt.Printf("inode:       %d\n", inode)
	fmt.Printf("is_dir:      %v\n", rec.IsDir)
	fmt.Printf("is_symlink:  %v", rec.IsSymlink)
	if rec.IsSymlink {
		fmt.Printf(" -> %s", rec.SymlinkTarget)
	}
	fmt.Println()
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
