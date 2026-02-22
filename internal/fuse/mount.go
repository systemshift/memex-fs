package fuse

import (
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
	"github.com/systemshift/memex-fs/internal/dagit"
)

// MountFS mounts the FUSE filesystem at mountpoint backed by repo.
// Returns the server (call server.Wait() to block, server.Unmount() to stop).
// feedManager may be nil to disable /feeds/.
func MountFS(mountpoint string, repo *dag.Repository, feedManager *dagit.FeedManager, debug bool) (*gofuse.Server, error) {
	root := &RootNode{repo: repo, feedManager: feedManager}

	opts := &fs.Options{
		MountOptions: gofuse.MountOptions{
			FsName:        "memex",
			Name:          "memex",
			DisableXAttrs: true,
			AllowOther:    false,
			Debug:         debug,
		},
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return nil, err
	}
	return server, nil
}
