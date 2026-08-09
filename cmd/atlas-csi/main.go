// Command atlas-csi is the AtlasFS CSI node plugin (DESIGN.md §22.1),
// serving Identity and Node gRPC services over a Unix domain socket per
// the CSI spec's deployment convention. There is no Controller service —
// see pkg/csidriver's package doc for why static provisioning doesn't
// need one.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	"github.com/aburan28/atlasfs/pkg/csidriver"
)

func main() {
	endpoint := flag.String("csi-endpoint", "unix:///var/lib/kubelet/plugins/csi.atlas.io/csi.sock", "gRPC endpoint, unix:// or tcp://")
	nodeID := flag.String("node-id", "", "this node's ID (defaults to hostname)")
	flag.Parse()

	id := *nodeID
	if id == "" {
		h, err := os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "atlas-csi: hostname: %v\n", err)
			os.Exit(1)
		}
		id = h
	}

	network, address, err := parseEndpoint(*endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "atlas-csi: %v\n", err)
		os.Exit(1)
	}
	if network == "unix" {
		_ = os.Remove(address) // stale socket from a previous run
		if err := os.MkdirAll(filepath.Dir(address), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "atlas-csi: %v\n", err)
			os.Exit(1)
		}
	}

	lis, err := net.Listen(network, address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "atlas-csi: listen: %v\n", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, csidriver.IdentityServer{})
	csi.RegisterNodeServer(srv, csidriver.NewNodeServer(id))

	fmt.Printf("atlas-csi: node %q serving on %s\n", id, *endpoint)
	if err := srv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "atlas-csi: serve: %v\n", err)
		os.Exit(1)
	}
}
