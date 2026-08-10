// Command atlas-mds runs one region's metadata authority (DESIGN.md §7:
// a subtree's home region owns its metadata). Clients talk to it for
// inodes, dentries, and leases, and go to object storage directly for
// chunk data (§11) — the authority is never in the data path.
//
// This is the process boundary that makes §10's coherence protocol real
// rather than a set of in-process function calls: leases are granted to
// named remote holders, invalidations are pushed over a stream that can
// genuinely be lost, and a `posix` mutation blocks on acknowledgements
// from other machines.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9713", "address to serve the metadata service on")
	repoDir := flag.String("repo", "", "repo directory whose metadata this authority owns (required)")
	class := flag.String("class", string(repo.ClassRelaxed),
		"consistency class this authority serves (DESIGN.md §8): immutable|relaxed|session|posix")
	drecall := flag.Duration("recall-deadline", mds.DefaultRecallDeadline,
		"DRECALL (DESIGN.md §10.6): how long a posix mutation waits for holders to acknowledge before proceeding without them")
	flag.Parse()

	if *repoDir == "" {
		fmt.Fprintln(os.Stderr, "atlas-mds: -repo is required")
		flag.Usage()
		os.Exit(2)
	}

	c := repo.Class(*class)
	switch c {
	case repo.ClassImmutable, repo.ClassRelaxed, repo.ClassSession, repo.ClassPosix:
	default:
		fmt.Fprintf(os.Stderr, "atlas-mds: unknown class %q\n", *class)
		os.Exit(2)
	}

	db, err := metadb.Open(filepath.Join(*repoDir, "meta.db"))
	if err != nil {
		log.Fatalf("atlas-mds: open metadata store: %v", err)
	}
	defer db.Close()

	srv := mds.NewServer(mds.Config{
		DB:             db,
		LeaseDuration:  c.LeaseDuration(),
		Posix:          c == repo.ClassPosix,
		RecallDeadline: *drecall,
	})

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("atlas-mds: listen: %v", err)
	}
	g := grpc.NewServer()
	srv.Register(g)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("atlas-mds: shutting down")
		// GracefulStop lets in-flight recalls finish rather than
		// abandoning a writer mid-commit.
		g.GracefulStop()
	}()

	log.Printf("atlas-mds: serving %s class %q on %s (D=%s, DRECALL=%s)",
		*repoDir, c, *addr, durString(c.LeaseDuration()), *drecall)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("atlas-mds: serve: %v", err)
	}
}

func durString(d time.Duration) string {
	if d <= 0 {
		return "none (immutable)"
	}
	return d.String()
}
