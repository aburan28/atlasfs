package main

import (
	"path"
	"strings"
)

// splitPath turns a "/"-rooted CLI path argument into path components.
func splitPath(p string) []string {
	p = strings.Trim(path.Clean("/"+p), "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}
