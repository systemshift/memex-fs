package fuse

import (
	"context"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// RootNode is the mountpoint directory. Contains "nodes/" and "types/".
type RootNode struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeOnAdder)((*RootNode)(nil))
var _ = (fs.NodeGetattrer)((*RootNode)(nil))

func (r *RootNode) OnAdd(ctx context.Context) {
	nodesDir := &NodesDir{repo: r.repo}
	nodesInode := r.NewPersistentInode(ctx, nodesDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("nodes"),
	})
	r.AddChild("nodes", nodesInode, true)

	typesDir := &TypesDir{repo: r.repo}
	typesInode := r.NewPersistentInode(ctx, typesDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("types"),
	})
	r.AddChild("types", typesInode, true)
}

func (r *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("/")
	return fs.OK
}

// NodesDir lists all non-deleted nodes. mkdir creates, rmdir deletes.
type NodesDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*NodesDir)(nil))
var _ = (fs.NodeReaddirer)((*NodesDir)(nil))
var _ = (fs.NodeGetattrer)((*NodesDir)(nil))
var _ = (fs.NodeMkdirer)((*NodesDir)(nil))
var _ = (fs.NodeRmdirer)((*NodesDir)(nil))

func (n *NodesDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("nodes")
	return fs.OK
}

func (n *NodesDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids, err := n.repo.ListNodes(0)
	if err != nil {
		return nil, syscall.EIO
	}
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("nodes/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (n *NodesDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	_, err := n.repo.GetNode(name)
	if err != nil {
		return nil, syscall.ENOENT
	}
	nodeDir := &NodeDir{repo: n.repo, nodeID: name}
	child := n.NewInode(ctx, nodeDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("nodes/" + name),
	})
	return child, fs.OK
}

func (n *NodesDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Parse type from id: "person:alice" -> type="Person"
	typ := "Node"
	if idx := strings.Index(name, ":"); idx > 0 {
		t := name[:idx]
		typ = strings.ToUpper(t[:1]) + t[1:]
	}

	_, err := n.repo.CreateNode(name, typ, nil, nil)
	if err != nil {
		return nil, syscall.EEXIST
	}

	nodeDir := &NodeDir{repo: n.repo, nodeID: name}
	child := n.NewInode(ctx, nodeDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("nodes/" + name),
	})
	return child, fs.OK
}

func (n *NodesDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	err := n.repo.DeleteNode(name, false)
	if err != nil {
		return syscall.ENOENT
	}
	return fs.OK
}
