package fuse

import (
	"context"
	"encoding/json"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// AtRootDir is /at/ — a lookup-only directory that resolves a key (either a
// commit CID in base32 form, or an RFC3339 timestamp) to a read-only
// snapshot of the repo at that point in time.
//
// It intentionally lists nothing on Readdir: the set of valid keys is
// effectively unbounded (any commit CID, any timestamp in the project's
// lifetime). Use /log/ to browse commits.
type AtRootDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*AtRootDir)(nil))
var _ = (fs.NodeReaddirer)((*AtRootDir)(nil))
var _ = (fs.NodeGetattrer)((*AtRootDir)(nil))

func (d *AtRootDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("at")
	return fs.OK
}

func (d *AtRootDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream(nil), fs.OK
}

func (d *AtRootDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	commit, err := d.repo.Commits.Resolve(name)
	if err != nil {
		return nil, syscall.ENOENT
	}
	snap := dag.NewSnapshot(commit, d.repo.Store)
	child := d.NewInode(ctx, &AtSnapshotDir{snap: snap, key: name}, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("at/" + name),
	})
	return child, fs.OK
}

// AtSnapshotDir is /at/{key}/ — exposes the snapshot as a read-only nodes/
// directory plus a tiny commit.json describing the commit itself.
type AtSnapshotDir struct {
	fs.Inode
	snap *dag.Snapshot
	key  string
}

var _ = (fs.NodeLookuper)((*AtSnapshotDir)(nil))
var _ = (fs.NodeReaddirer)((*AtSnapshotDir)(nil))
var _ = (fs.NodeGetattrer)((*AtSnapshotDir)(nil))

func (d *AtSnapshotDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("at/" + d.key)
	return fs.OK
}

func (d *AtSnapshotDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "nodes", Mode: syscall.S_IFDIR, Ino: stableIno("at/" + d.key + "/nodes")},
		{Name: "commit.json", Mode: syscall.S_IFREG, Ino: stableIno("at/" + d.key + "/commit.json")},
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *AtSnapshotDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "nodes":
		child := d.NewInode(ctx, &AtNodesDir{snap: d.snap, key: d.key}, fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("at/" + d.key + "/nodes"),
		})
		return child, fs.OK
	case "commit.json":
		child := d.NewInode(ctx, &AtCommitInfoFile{snap: d.snap, key: d.key}, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno("at/" + d.key + "/commit.json"),
		})
		return child, fs.OK
	}
	return nil, syscall.ENOENT
}

// AtCommitInfoFile exposes the commit timestamp and message as JSON.
type AtCommitInfoFile struct {
	fs.Inode
	snap *dag.Snapshot
	key  string
}

var _ = (fs.NodeGetattrer)((*AtCommitInfoFile)(nil))
var _ = (fs.NodeOpener)((*AtCommitInfoFile)(nil))
var _ = (fs.NodeReader)((*AtCommitInfoFile)(nil))

func (f *AtCommitInfoFile) info() []byte {
	m := map[string]interface{}{
		"timestamp": f.snap.Timestamp(),
		"message":   f.snap.Message(),
	}
	data, _ := json.MarshalIndent(m, "", "  ")
	return append(data, '\n')
}

func (f *AtCommitInfoFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Size = uint64(len(f.info()))
	out.Ino = stableIno("at/" + f.key + "/commit.json")
	return fs.OK
}

func (f *AtCommitInfoFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *AtCommitInfoFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data := f.info()
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// AtNodesDir is /at/{key}/nodes/ — lists all nodes as of the snapshot.
type AtNodesDir struct {
	fs.Inode
	snap *dag.Snapshot
	key  string
}

var _ = (fs.NodeLookuper)((*AtNodesDir)(nil))
var _ = (fs.NodeReaddirer)((*AtNodesDir)(nil))
var _ = (fs.NodeGetattrer)((*AtNodesDir)(nil))

func (d *AtNodesDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("at/" + d.key + "/nodes")
	return fs.OK
}

func (d *AtNodesDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids := d.snap.ListNodes()
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("at/" + d.key + "/nodes/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *AtNodesDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if _, err := d.snap.GetNode(name); err != nil {
		return nil, syscall.ENOENT
	}
	child := d.NewInode(ctx, &AtNodeDir{snap: d.snap, key: d.key, nodeID: name}, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("at/" + d.key + "/nodes/" + name),
	})
	return child, fs.OK
}

// AtNodeDir is /at/{key}/nodes/{id}/ — a read-only snapshot node directory.
type AtNodeDir struct {
	fs.Inode
	snap   *dag.Snapshot
	key    string
	nodeID string
}

var _ = (fs.NodeLookuper)((*AtNodeDir)(nil))
var _ = (fs.NodeReaddirer)((*AtNodeDir)(nil))
var _ = (fs.NodeGetattrer)((*AtNodeDir)(nil))

func (d *AtNodeDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("at/" + d.key + "/nodes/" + d.nodeID)
	return fs.OK
}

func (d *AtNodeDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "content", Mode: syscall.S_IFREG, Ino: stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/content")},
		{Name: "meta.json", Mode: syscall.S_IFREG, Ino: stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/meta.json")},
		{Name: "type", Mode: syscall.S_IFREG, Ino: stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/type")},
		{Name: "links", Mode: syscall.S_IFDIR, Ino: stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/links")},
		{Name: "backlinks", Mode: syscall.S_IFDIR, Ino: stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/backlinks")},
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *AtNodeDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	basePath := "at/" + d.key + "/nodes/" + d.nodeID
	switch name {
	case "content":
		child := d.NewInode(ctx, &AtContentFile{snap: d.snap, nodeID: d.nodeID, path: basePath + "/content"}, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno(basePath + "/content"),
		})
		return child, fs.OK
	case "meta.json":
		child := d.NewInode(ctx, &AtMetaFile{snap: d.snap, nodeID: d.nodeID, path: basePath + "/meta.json"}, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno(basePath + "/meta.json"),
		})
		return child, fs.OK
	case "type":
		child := d.NewInode(ctx, &AtTypeFile{snap: d.snap, nodeID: d.nodeID, path: basePath + "/type"}, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno(basePath + "/type"),
		})
		return child, fs.OK
	case "links":
		child := d.NewInode(ctx, &AtLinksDir{snap: d.snap, nodeID: d.nodeID, key: d.key, reverse: false}, fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  stableIno(basePath + "/links"),
		})
		return child, fs.OK
	case "backlinks":
		child := d.NewInode(ctx, &AtLinksDir{snap: d.snap, nodeID: d.nodeID, key: d.key, reverse: true}, fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  stableIno(basePath + "/backlinks"),
		})
		return child, fs.OK
	}
	return nil, syscall.ENOENT
}

// AtContentFile exposes a snapshot node's content as a read-only file.
type AtContentFile struct {
	fs.Inode
	snap   *dag.Snapshot
	nodeID string
	path   string
}

var _ = (fs.NodeGetattrer)((*AtContentFile)(nil))
var _ = (fs.NodeOpener)((*AtContentFile)(nil))
var _ = (fs.NodeReader)((*AtContentFile)(nil))

func (f *AtContentFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	node, err := f.snap.GetNode(f.nodeID)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = 0444
	out.Size = uint64(len(node.Content))
	out.Ino = stableIno(f.path)
	return fs.OK
}

func (f *AtContentFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *AtContentFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	node, err := f.snap.GetNode(f.nodeID)
	if err != nil {
		return nil, syscall.ENOENT
	}
	data := node.Content
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// AtMetaFile exposes a snapshot node's meta as read-only JSON.
type AtMetaFile struct {
	fs.Inode
	snap   *dag.Snapshot
	nodeID string
	path   string
}

var _ = (fs.NodeGetattrer)((*AtMetaFile)(nil))
var _ = (fs.NodeOpener)((*AtMetaFile)(nil))
var _ = (fs.NodeReader)((*AtMetaFile)(nil))

func (f *AtMetaFile) metaBytes() ([]byte, error) {
	node, err := f.snap.GetNode(f.nodeID)
	if err != nil {
		return nil, err
	}
	m := node.Meta
	if m == nil {
		m = make(map[string]interface{})
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (f *AtMetaFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	data, err := f.metaBytes()
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = 0444
	out.Size = uint64(len(data))
	out.Ino = stableIno(f.path)
	return fs.OK
}

func (f *AtMetaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *AtMetaFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := f.metaBytes()
	if err != nil {
		return nil, syscall.ENOENT
	}
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// AtTypeFile exposes a snapshot node's type as a read-only file.
type AtTypeFile struct {
	fs.Inode
	snap   *dag.Snapshot
	nodeID string
	path   string
}

var _ = (fs.NodeGetattrer)((*AtTypeFile)(nil))
var _ = (fs.NodeOpener)((*AtTypeFile)(nil))
var _ = (fs.NodeReader)((*AtTypeFile)(nil))

func (f *AtTypeFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	node, err := f.snap.GetNode(f.nodeID)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = 0444
	out.Size = uint64(len(node.Type) + 1)
	out.Ino = stableIno(f.path)
	return fs.OK
}

func (f *AtTypeFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *AtTypeFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	node, err := f.snap.GetNode(f.nodeID)
	if err != nil {
		return nil, syscall.ENOENT
	}
	data := []byte(node.Type + "\n")
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// AtLinksDir lists links (or backlinks when reverse=true) of a snapshot node
// as read-only symlinks pointing back into ../../{id}.
type AtLinksDir struct {
	fs.Inode
	snap    *dag.Snapshot
	nodeID  string
	key     string
	reverse bool
}

var _ = (fs.NodeLookuper)((*AtLinksDir)(nil))
var _ = (fs.NodeReaddirer)((*AtLinksDir)(nil))
var _ = (fs.NodeGetattrer)((*AtLinksDir)(nil))

func (d *AtLinksDir) subdir() string {
	if d.reverse {
		return "backlinks"
	}
	return "links"
}

func (d *AtLinksDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/" + d.subdir())
	return fs.OK
}

func (d *AtLinksDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var links []dag.LinkEntry
	if d.reverse {
		links = d.snap.LinksTo(d.nodeID)
	} else {
		links = d.snap.LinksFrom(d.nodeID)
	}
	var entries []fuse.DirEntry
	for _, l := range links {
		peer := l.Target
		if d.reverse {
			peer = l.Source
		}
		name := l.Type + ":" + peer
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/" + d.subdir() + "/" + name),
		})
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *AtLinksDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	idx := strings.Index(name, ":")
	if idx < 0 {
		return nil, syscall.ENOENT
	}
	linkType := name[:idx]
	peer := name[idx+1:]

	var links []dag.LinkEntry
	if d.reverse {
		links = d.snap.LinksTo(d.nodeID)
	} else {
		links = d.snap.LinksFrom(d.nodeID)
	}
	for _, l := range links {
		candidate := l.Target
		if d.reverse {
			candidate = l.Source
		}
		if l.Type == linkType && candidate == peer {
			sym := &LinkSymlink{target: "../../" + peer}
			child := d.NewInode(ctx, sym, fs.StableAttr{
				Mode: syscall.S_IFLNK,
				Ino:  stableIno("at/" + d.key + "/nodes/" + d.nodeID + "/" + d.subdir() + "/" + name),
			})
			return child, fs.OK
		}
	}
	return nil, syscall.ENOENT
}
