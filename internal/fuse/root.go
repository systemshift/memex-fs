package fuse

import (
	"context"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// RootNode is the mountpoint directory. Contains "nodes/", "types/", and "log/".
type RootNode struct {
	fs.Inode
	repo      *dag.Repository
	accessLog *AccessLog
}

var _ = (fs.NodeOnAdder)((*RootNode)(nil))
var _ = (fs.NodeGetattrer)((*RootNode)(nil))

func (r *RootNode) OnAdd(ctx context.Context) {
	r.accessLog = NewAccessLog(filepath.Join(r.repo.MxDir(), "access.jsonl"))

	nodesDir := &NodesDir{repo: r.repo, accessLog: r.accessLog}
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

	logDir := &LogDir{repo: r.repo}
	logInode := r.NewPersistentInode(ctx, logDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("log"),
	})
	r.AddChild("log", logInode, true)

	searchDir := &SearchRootDir{repo: r.repo}
	searchInode := r.NewPersistentInode(ctx, searchDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("search"),
	})
	r.AddChild("search", searchInode, true)

	relatedDir := &RelatedRootDir{repo: r.repo}
	relatedInode := r.NewPersistentInode(ctx, relatedDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("related"),
	})
	r.AddChild("related", relatedInode, true)

	lensesDir := &LensesRootDir{repo: r.repo}
	lensesInode := r.NewPersistentInode(ctx, lensesDir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("lenses"),
	})
	r.AddChild("lenses", lensesInode, true)

	// Wire co-access callback: access log → co-access index
	r.accessLog.OnAccess = func(nodeID string, ts time.Time) {
		r.repo.CoAccess.Record(nodeID, ts)
	}
}

func (r *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("/")
	return fs.OK
}

// NodesDir lists all non-deleted nodes. mkdir creates, rmdir deletes.
type NodesDir struct {
	fs.Inode
	repo      *dag.Repository
	accessLog *AccessLog
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
	nodeDir := &NodeDir{repo: n.repo, nodeID: name, accessLog: n.accessLog}
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

	nodeDir := &NodeDir{repo: n.repo, nodeID: name, accessLog: n.accessLog}
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
