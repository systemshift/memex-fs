package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// TypesDir lists all known types as subdirectories.
type TypesDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*TypesDir)(nil))
var _ = (fs.NodeReaddirer)((*TypesDir)(nil))
var _ = (fs.NodeGetattrer)((*TypesDir)(nil))

func (d *TypesDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("types")
	return fs.OK
}

func (d *TypesDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	types := d.repo.Search.AllTypes()
	entries := make([]fuse.DirEntry, len(types))
	for i, t := range types {
		entries[i] = fuse.DirEntry{
			Name: t,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("types/" + t),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *TypesDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Check if this type has any nodes
	ids := d.repo.Search.FilterByType(name, 0)
	if len(ids) == 0 {
		return nil, syscall.ENOENT
	}
	group := &TypeGroupDir{repo: d.repo, typeName: name}
	child := d.NewInode(ctx, group, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("types/" + name),
	})
	return child, fs.OK
}

// TypeGroupDir lists all nodes of a specific type as symlinks to ../../nodes/{id}.
type TypeGroupDir struct {
	fs.Inode
	repo     *dag.Repository
	typeName string
}

var _ = (fs.NodeLookuper)((*TypeGroupDir)(nil))
var _ = (fs.NodeReaddirer)((*TypeGroupDir)(nil))
var _ = (fs.NodeGetattrer)((*TypeGroupDir)(nil))

func (d *TypeGroupDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("types/" + d.typeName)
	return fs.OK
}

func (d *TypeGroupDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids := d.repo.Search.FilterByType(d.typeName, 0)
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("types/" + d.typeName + "/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *TypeGroupDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Verify the node exists and is of this type
	node, err := d.repo.GetNode(name)
	if err != nil || node.Type != d.typeName {
		return nil, syscall.ENOENT
	}

	sym := &TypeSymlink{nodeID: name}
	child := d.NewInode(ctx, sym, fs.StableAttr{
		Mode: syscall.S_IFLNK,
		Ino:  stableIno("types/" + d.typeName + "/" + name),
	})
	return child, fs.OK
}

// TypeSymlink points to ../../nodes/{id}.
type TypeSymlink struct {
	fs.Inode
	nodeID string
}

var _ = (fs.NodeReadlinker)((*TypeSymlink)(nil))
var _ = (fs.NodeGetattrer)((*TypeSymlink)(nil))

func (s *TypeSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target := "../../nodes/" + s.nodeID
	return []byte(target), fs.OK
}

func (s *TypeSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	target := "../../nodes/" + s.nodeID
	out.Mode = 0777 | syscall.S_IFLNK
	out.Size = uint64(len(target))
	return fs.OK
}
