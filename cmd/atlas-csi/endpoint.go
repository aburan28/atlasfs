package main

import (
	"fmt"
	"strings"
)

// parseEndpoint splits a CSI-style endpoint URL ("unix:///path/to.sock"
// or "tcp://host:port") into the network and address net.Listen wants.
func parseEndpoint(endpoint string) (network, address string, err error) {
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		return "unix", strings.TrimPrefix(endpoint, "unix://"), nil
	case strings.HasPrefix(endpoint, "tcp://"):
		return "tcp", strings.TrimPrefix(endpoint, "tcp://"), nil
	default:
		return "", "", fmt.Errorf("invalid --csi-endpoint %q: want unix://path or tcp://host:port", endpoint)
	}
}
