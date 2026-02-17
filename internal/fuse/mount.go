package fuse

import (
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// MountFS mounts the FUSE filesystem at mountpoint backed by repo.
// Returns the server (call server.Wait() to block, server.Unmount() to stop).
func MountFS(mountpoint string, repo *dag.Repository) (*gofuse.Server, error) {
	root := &RootNode{repo: repo}

	opts := &fs.Options{
		MountOptions: gofuse.MountOptions{
			FsName:        "memex",
			Name:          "memex",
			DisableXAttrs: true,
			Debug:         true,
		},
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return nil, err
	}
	return server, nil
}
