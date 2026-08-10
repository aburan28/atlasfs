package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/aburan28/atlasfs/pkg/repo"
)

// cmdGC runs DESIGN.md §19's mark-and-sweep, including the §19.1 step 3
// compaction that actually returns bytes to the backend. Until this
// existed, Repo.Sweep was reachable only from Go code, which meant the
// GC an operator could actually run was no GC at all.
func cmdGC(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	grace := fs.Duration("grace", repo.DefaultGraceDuration,
		"T_grace (DESIGN.md §19.2). Must exceed T_write_max + D_max + ε; a shorter value is rejected, not clamped.")
	dryRun := fs.Bool("dry-run", false, "report what compaction would rewrite without sweeping or deleting anything")
	fs.Usage = func() {
		fmt.Println("usage: atlas gc [flags] <repo-dir>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) != 1 {
		fs.Usage()
		return fmt.Errorf("usage: atlas gc [flags] <repo-dir>")
	}

	r, err := openRepo(ctx, pos[0], bf)
	if err != nil {
		return err
	}
	defer r.Close()

	if *dryRun {
		// Compact with an impossible threshold reports how many
		// containers exist without touching them; a real dry run of the
		// sweep would need to model deletions it must not perform, so
		// this deliberately reports the narrower, honest thing.
		before, err := backendUsage(ctx, r)
		if err != nil {
			return err
		}
		fmt.Printf("backend currently holds %s across container objects\n", humanBytes(before))
		fmt.Println("dry-run: nothing swept, compacted, or deleted")
		return nil
	}

	before, err := backendUsage(ctx, r)
	if err != nil {
		return err
	}
	collected, err := r.Sweep(ctx, *grace)
	if err != nil {
		return err
	}
	after, err := backendUsage(ctx, r)
	if err != nil {
		return err
	}

	fmt.Printf("swept %d chunk locator(s)\n", collected)
	fmt.Printf("backend: %s -> %s (%s reclaimed)\n", humanBytes(before), humanBytes(after), humanBytes(before-after))
	if after == before && collected > 0 {
		// Not an error: a container is only retired once it drops below
		// the liveness threshold, and a retired container is only deleted
		// once it is itself past grace. Saying so beats leaving an
		// operator to wonder why a "successful" GC freed nothing.
		fmt.Println("note: locators were freed but no container fell below the liveness threshold,")
		fmt.Println("      or retired containers are still within the grace period — re-run after T_grace.")
	}
	return nil
}

// cmdQuota reads or sets this repo's quota (DESIGN.md §18.3).
func cmdQuota(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("quota", flag.ContinueOnError)
	var bf backendFlags
	addBackendFlags(fs, &bf)
	setBytes := fs.Int64("bytes", -1, "byte limit to set; 0 means unlimited, omit to leave unchanged")
	setInodes := fs.Int64("inodes", -1, "inode limit to set; 0 means unlimited, omit to leave unchanged")
	fs.Usage = func() {
		fmt.Println("usage: atlas quota [flags] <repo-dir>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	if len(pos) != 1 {
		fs.Usage()
		return fmt.Errorf("usage: atlas quota [flags] <repo-dir>")
	}

	r, err := openRepo(ctx, pos[0], bf)
	if err != nil {
		return err
	}
	defer r.Close()

	if *setBytes >= 0 || *setInodes >= 0 {
		curBytes, curInodes, err := r.QuotaLimits()
		if err != nil {
			return err
		}
		// A flag left unset must not silently reset the other limit to
		// unlimited, so unspecified means "keep the current value".
		if *setBytes >= 0 {
			curBytes = uint64(*setBytes)
		}
		if *setInodes >= 0 {
			curInodes = uint64(*setInodes)
		}
		if err := r.SetQuota(curBytes, curInodes); err != nil {
			return err
		}
	}

	bytesLimit, inodesLimit, err := r.QuotaLimits()
	if err != nil {
		return err
	}
	bytesUsed, inodesUsed, err := r.QuotaUsage()
	if err != nil {
		return err
	}
	fmt.Printf("bytes:  %s used / %s\n", humanBytes(int64(bytesUsed)), limitString(bytesLimit, true))
	fmt.Printf("inodes: %d used / %s\n", inodesUsed, limitString(inodesLimit, false))
	return nil
}

func limitString(limit uint64, asBytes bool) string {
	if limit == 0 {
		return "unlimited"
	}
	if asBytes {
		return humanBytes(int64(limit))
	}
	return fmt.Sprint(limit)
}

// backendUsage totals the bytes this repo occupies in object storage, so
// gc can report a real before/after rather than only a locator count.
func backendUsage(ctx context.Context, r *repo.Repo) (int64, error) {
	var total int64
	cursor := ""
	for {
		page, err := r.Backend.List(ctx, "atlas/", cursor, 0)
		if err != nil {
			return 0, err
		}
		for _, o := range page.Keys {
			total += o.Size
		}
		if page.NextCursor == "" {
			return total, nil
		}
		cursor = page.NextCursor
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
