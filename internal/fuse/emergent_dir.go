package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// EmergentRootDir is /emergent/ — the top-level view over shapes that fall
// out of the explicit graph. Currently exposes clusters/. Future: patterns/
// for frequent (source-type)-(link-type)-(target-type) triples.
type EmergentRootDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*EmergentRootDir)(nil))
var _ = (fs.NodeReaddirer)((*EmergentRootDir)(nil))
var _ = (fs.NodeGetattrer)((*EmergentRootDir)(nil))

func (d *EmergentRootDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("emergent")
	return fs.OK
}

func (d *EmergentRootDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "clusters", Mode: syscall.S_IFDIR, Ino: stableIno("emergent/clusters")},
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *EmergentRootDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "clusters" {
		child := d.NewInode(ctx, &EmergentClustersDir{repo: d.repo}, fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("emergent/clusters"),
		})
		return child, fs.OK
	}
	return nil, syscall.ENOENT
}

// EmergentClustersDir is /emergent/clusters/. Each entry is a cluster ID
// directory whose contents are its member nodes as symlinks.
type EmergentClustersDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*EmergentClustersDir)(nil))
var _ = (fs.NodeReaddirer)((*EmergentClustersDir)(nil))
var _ = (fs.NodeGetattrer)((*EmergentClustersDir)(nil))

func (d *EmergentClustersDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("emergent/clusters")
	return fs.OK
}

func (d *EmergentClustersDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	clusters := d.repo.Emergent.Clusters()
	entries := make([]fuse.DirEntry, len(clusters))
	for i, c := range clusters {
		entries[i] = fuse.DirEntry{
			Name: c.ID,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("emergent/clusters/" + c.ID),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *EmergentClustersDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	c := d.repo.Emergent.ClusterByID(name)
	if c == nil {
		return nil, syscall.ENOENT
	}
	child := d.NewInode(ctx, &EmergentClusterDir{id: c.ID, members: c.Members}, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("emergent/clusters/" + c.ID),
	})
	return child, fs.OK
}

// EmergentClusterDir is /emergent/clusters/{id}/. Members are symlinks into
// ../../../nodes/{member}.
type EmergentClusterDir struct {
	fs.Inode
	id      string
	members []string
}

var _ = (fs.NodeLookuper)((*EmergentClusterDir)(nil))
var _ = (fs.NodeReaddirer)((*EmergentClusterDir)(nil))
var _ = (fs.NodeGetattrer)((*EmergentClusterDir)(nil))

func (d *EmergentClusterDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("emergent/clusters/" + d.id)
	return fs.OK
}

func (d *EmergentClusterDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, len(d.members))
	for i, m := range d.members {
		entries[i] = fuse.DirEntry{
			Name: m,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("emergent/clusters/" + d.id + "/" + m),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *EmergentClusterDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	for _, m := range d.members {
		if m == name {
			sym := &LinkSymlink{target: "../../../nodes/" + name}
			child := d.NewInode(ctx, sym, fs.StableAttr{
				Mode: syscall.S_IFLNK,
				Ino:  stableIno("emergent/clusters/" + d.id + "/" + name),
			})
			return child, fs.OK
		}
	}
	return nil, syscall.ENOENT
}
