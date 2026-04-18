package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// neighborsLimit caps how many neighbors are materialized per node. The
// ranker is cheap but large fan-outs (e.g. a hub node) can produce very
// long lists; 50 is enough for agent navigation.
const neighborsLimit = 50

// NeighborsDir is /nodes/{id}/neighbors/ — a ranked, virtual directory of
// nodes related to {id}. Entries are symlinks into ../../../nodes/{peer}.
// The ranking blends explicit links, two-hop topology, co-change, shared
// type, and de-weighted co-access (see internal/dag/neighbors.go).
type NeighborsDir struct {
	fs.Inode
	repo   *dag.Repository
	nodeID string
}

var _ = (fs.NodeLookuper)((*NeighborsDir)(nil))
var _ = (fs.NodeReaddirer)((*NeighborsDir)(nil))
var _ = (fs.NodeGetattrer)((*NeighborsDir)(nil))

func (d *NeighborsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("nodes/" + d.nodeID + "/neighbors")
	return fs.OK
}

func (d *NeighborsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	peers := d.repo.Neighbors.Neighbors(d.nodeID, neighborsLimit)
	entries := make([]fuse.DirEntry, len(peers))
	for i, id := range peers {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("nodes/" + d.nodeID + "/neighbors/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *NeighborsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	peers := d.repo.Neighbors.Neighbors(d.nodeID, neighborsLimit)
	for _, id := range peers {
		if id == name {
			sym := &LinkSymlink{target: "../../" + name}
			child := d.NewInode(ctx, sym, fs.StableAttr{
				Mode: syscall.S_IFLNK,
				Ino:  stableIno("nodes/" + d.nodeID + "/neighbors/" + name),
			})
			return child, fs.OK
		}
	}
	return nil, syscall.ENOENT
}
