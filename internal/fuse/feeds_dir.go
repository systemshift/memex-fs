package fuse

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dagit"
)

// ---------------------------------------------------------------------------
// FeedsDir — /feeds/
// ---------------------------------------------------------------------------

// FeedsDir is the root feeds directory. Lists identity, post, sync, following/, mine/.
type FeedsDir struct {
	fs.Inode
	fm *dagit.FeedManager
}

var _ = (fs.NodeGetattrer)((*FeedsDir)(nil))
var _ = (fs.NodeReaddirer)((*FeedsDir)(nil))
var _ = (fs.NodeLookuper)((*FeedsDir)(nil))

func (d *FeedsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("feeds")
	return fs.OK
}

func (d *FeedsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "identity", Mode: syscall.S_IFREG, Ino: stableIno("feeds/identity")},
		{Name: "post", Mode: syscall.S_IFREG, Ino: stableIno("feeds/post")},
		{Name: "sync", Mode: syscall.S_IFREG, Ino: stableIno("feeds/sync")},
		{Name: "following", Mode: syscall.S_IFDIR, Ino: stableIno("feeds/following")},
		{Name: "mine", Mode: syscall.S_IFDIR, Ino: stableIno("feeds/mine")},
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *FeedsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "identity":
		f := &FeedsIdentityFile{fm: d.fm}
		return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("feeds/identity")}), fs.OK
	case "post":
		f := &FeedsPostFile{fm: d.fm}
		return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("feeds/post")}), fs.OK
	case "sync":
		f := &FeedsSyncFile{fm: d.fm}
		return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("feeds/sync")}), fs.OK
	case "following":
		dir := &FollowingDir{fm: d.fm}
		return d.NewInode(ctx, dir, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("feeds/following")}), fs.OK
	case "mine":
		dir := &MineDir{fm: d.fm}
		return d.NewInode(ctx, dir, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("feeds/mine")}), fs.OK
	default:
		return nil, syscall.ENOENT
	}
}

// ---------------------------------------------------------------------------
// FeedsIdentityFile — /feeds/identity (read-only, returns DID)
// ---------------------------------------------------------------------------

type FeedsIdentityFile struct {
	fs.Inode
	fm *dagit.FeedManager
}

var _ = (fs.NodeGetattrer)((*FeedsIdentityFile)(nil))
var _ = (fs.NodeOpener)((*FeedsIdentityFile)(nil))
var _ = (fs.NodeReader)((*FeedsIdentityFile)(nil))

func (f *FeedsIdentityFile) content() []byte {
	return []byte(f.fm.DID() + "\n")
}

func (f *FeedsIdentityFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Size = uint64(len(f.content()))
	out.Ino = stableIno("feeds/identity")
	return fs.OK
}

func (f *FeedsIdentityFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *FeedsIdentityFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data := f.content()
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// ---------------------------------------------------------------------------
// FeedsPostFile — /feeds/post (write-only, publishes a post on flush)
// ---------------------------------------------------------------------------

type FeedsPostFile struct {
	fs.Inode
	fm *dagit.FeedManager
}

var _ = (fs.NodeGetattrer)((*FeedsPostFile)(nil))
var _ = (fs.NodeSetattrer)((*FeedsPostFile)(nil))
var _ = (fs.NodeOpener)((*FeedsPostFile)(nil))

func (f *FeedsPostFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0222
	out.Size = 0
	out.Ino = stableIno("feeds/post")
	return fs.OK
}

func (f *FeedsPostFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return f.Getattr(ctx, fh, out)
}

func (f *FeedsPostFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	wh := &PostWriteHandle{fm: f.fm}
	return wh, fuse.FOPEN_DIRECT_IO, fs.OK
}

// PostWriteHandle buffers written data and publishes on Flush.
type PostWriteHandle struct {
	fm  *dagit.FeedManager
	buf []byte
}

var _ = (fs.FileWriter)((*PostWriteHandle)(nil))
var _ = (fs.FileFlusher)((*PostWriteHandle)(nil))

func (h *PostWriteHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	end := int(off) + len(data)
	if end > len(h.buf) {
		newBuf := make([]byte, end)
		copy(newBuf, h.buf)
		h.buf = newBuf
	}
	copy(h.buf[off:], data)
	return uint32(len(data)), fs.OK
}

func (h *PostWriteHandle) Flush(ctx context.Context) syscall.Errno {
	if len(h.buf) == 0 {
		return fs.OK
	}
	content := strings.TrimRight(string(h.buf), "\n")
	if content == "" {
		return fs.OK
	}
	cid, err := h.fm.PublishPost(content, nil, nil)
	if err != nil {
		fmt.Printf("memex-fs: publish post failed: %v\n", err)
		return syscall.EIO
	}
	fmt.Printf("memex-fs: published post %s\n", cid)
	return fs.OK
}

// ---------------------------------------------------------------------------
// FeedsSyncFile — /feeds/sync (read triggers feed check)
// ---------------------------------------------------------------------------

type FeedsSyncFile struct {
	fs.Inode
	fm *dagit.FeedManager
}

var _ = (fs.NodeGetattrer)((*FeedsSyncFile)(nil))
var _ = (fs.NodeOpener)((*FeedsSyncFile)(nil))
var _ = (fs.NodeReader)((*FeedsSyncFile)(nil))

func (f *FeedsSyncFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Size = 4096 // hint
	out.Ino = stableIno("feeds/sync")
	return fs.OK
}

func (f *FeedsSyncFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Trigger feed check on open, return the result as a read handle
	summary, err := f.fm.CheckFeeds()
	if err != nil {
		summary = fmt.Sprintf("error: %v", err)
	}
	rh := &SyncReadHandle{data: []byte(summary + "\n")}
	return rh, fuse.FOPEN_DIRECT_IO, fs.OK
}

func (f *FeedsSyncFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if rh, ok := fh.(*SyncReadHandle); ok {
		return rh.Read(ctx, dest, off)
	}
	return fuse.ReadResultData(nil), fs.OK
}

// SyncReadHandle holds the result of a sync check.
type SyncReadHandle struct {
	data []byte
}

var _ = (fs.FileReader)((*SyncReadHandle)(nil))

func (h *SyncReadHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(h.data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(h.data)) {
		end = int64(len(h.data))
	}
	return fuse.ReadResultData(h.data[off:end]), fs.OK
}

// ---------------------------------------------------------------------------
// FollowingDir — /feeds/following/ (mkdir to follow, rmdir to unfollow)
// ---------------------------------------------------------------------------

type FollowingDir struct {
	fs.Inode
	fm *dagit.FeedManager
}

var _ = (fs.NodeGetattrer)((*FollowingDir)(nil))
var _ = (fs.NodeReaddirer)((*FollowingDir)(nil))
var _ = (fs.NodeLookuper)((*FollowingDir)(nil))
var _ = (fs.NodeMkdirer)((*FollowingDir)(nil))
var _ = (fs.NodeRmdirer)((*FollowingDir)(nil))

func (d *FollowingDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("feeds/following")
	return fs.OK
}

func (d *FollowingDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := d.fm.ListFollowing()
	if err != nil {
		return fs.NewListDirStream(nil), fs.OK
	}
	var dirEntries []fuse.DirEntry
	for _, e := range entries {
		name := e.Alias
		if name == "" {
			name = dagit.PetnameFromDID(e.DID)
		}
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("feeds/following/" + name),
		})
	}
	return fs.NewListDirStream(dirEntries), fs.OK
}

func (d *FollowingDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	entries, err := d.fm.ListFollowing()
	if err != nil {
		return nil, syscall.ENOENT
	}
	for _, e := range entries {
		alias := e.Alias
		if alias == "" {
			alias = dagit.PetnameFromDID(e.DID)
		}
		if alias == name {
			dir := &FollowedFeedDir{fm: d.fm, did: e.DID, alias: alias}
			return d.NewInode(ctx, dir, fs.StableAttr{
				Mode: syscall.S_IFDIR,
				Ino:  stableIno("feeds/following/" + name),
			}), fs.OK
		}
	}
	return nil, syscall.ENOENT
}

// Mkdir follows a DID. The name should be a DID; alias is auto-generated.
func (d *FollowingDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// name is expected to be a DID
	if err := d.fm.Follow(name, ""); err != nil {
		fmt.Printf("memex-fs: follow failed: %v\n", err)
		return nil, syscall.EINVAL
	}
	alias := dagit.PetnameFromDID(name)
	dir := &FollowedFeedDir{fm: d.fm, did: name, alias: alias}
	child := d.NewInode(ctx, dir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("feeds/following/" + alias),
	})
	return child, fs.OK
}

// Rmdir unfollows by alias.
func (d *FollowingDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := d.fm.Unfollow(name); err != nil {
		return syscall.ENOENT
	}
	return fs.OK
}

// ---------------------------------------------------------------------------
// FollowedFeedDir — /feeds/following/{alias}/ (contains did, posts/)
// ---------------------------------------------------------------------------

type FollowedFeedDir struct {
	fs.Inode
	fm    *dagit.FeedManager
	did   string
	alias string
}

var _ = (fs.NodeGetattrer)((*FollowedFeedDir)(nil))
var _ = (fs.NodeReaddirer)((*FollowedFeedDir)(nil))
var _ = (fs.NodeLookuper)((*FollowedFeedDir)(nil))

func (d *FollowedFeedDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("feeds/following/" + d.alias)
	return fs.OK
}

func (d *FollowedFeedDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "did", Mode: syscall.S_IFREG, Ino: stableIno("feeds/following/" + d.alias + "/did")},
		{Name: "posts", Mode: syscall.S_IFDIR, Ino: stableIno("feeds/following/" + d.alias + "/posts")},
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *FollowedFeedDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "did":
		f := &StaticReadFile{data: []byte(d.did + "\n"), inoPath: "feeds/following/" + d.alias + "/did"}
		return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("feeds/following/" + d.alias + "/did")}), fs.OK
	case "posts":
		dir := &FollowedPostsDir{fm: d.fm, did: d.did, alias: d.alias}
		return d.NewInode(ctx, dir, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("feeds/following/" + d.alias + "/posts")}), fs.OK
	default:
		return nil, syscall.ENOENT
	}
}

// ---------------------------------------------------------------------------
// FollowedPostsDir — /feeds/following/{alias}/posts/ (symlinks to nodes)
// ---------------------------------------------------------------------------

type FollowedPostsDir struct {
	fs.Inode
	fm    *dagit.FeedManager
	did   string
	alias string
}

var _ = (fs.NodeGetattrer)((*FollowedPostsDir)(nil))
var _ = (fs.NodeReaddirer)((*FollowedPostsDir)(nil))
var _ = (fs.NodeLookuper)((*FollowedPostsDir)(nil))

func (d *FollowedPostsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("feeds/following/" + d.alias + "/posts")
	return fs.OK
}

func (d *FollowedPostsDir) postIDs() []string {
	return d.fm.PostIDsByAuthor(d.did)
}

func (d *FollowedPostsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids := d.postIDs()
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("feeds/following/" + d.alias + "/posts/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *FollowedPostsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	ids := d.postIDs()
	for _, id := range ids {
		if id == name {
			target := "../../../../nodes/" + name
			sym := &LinkSymlink{target: target}
			return d.NewInode(ctx, sym, fs.StableAttr{
				Mode: syscall.S_IFLNK,
				Ino:  stableIno("feeds/following/" + d.alias + "/posts/" + name),
			}), fs.OK
		}
	}
	return nil, syscall.ENOENT
}

// ---------------------------------------------------------------------------
// MineDir — /feeds/mine/ (symlinks to own published posts)
// ---------------------------------------------------------------------------

type MineDir struct {
	fs.Inode
	fm *dagit.FeedManager
}

var _ = (fs.NodeGetattrer)((*MineDir)(nil))
var _ = (fs.NodeReaddirer)((*MineDir)(nil))
var _ = (fs.NodeLookuper)((*MineDir)(nil))

func (d *MineDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("feeds/mine")
	return fs.OK
}

func (d *MineDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids := d.fm.OwnPostIDs()
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("feeds/mine/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *MineDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	ids := d.fm.OwnPostIDs()
	for _, id := range ids {
		if id == name {
			target := "../../nodes/" + name
			sym := &LinkSymlink{target: target}
			return d.NewInode(ctx, sym, fs.StableAttr{
				Mode: syscall.S_IFLNK,
				Ino:  stableIno("feeds/mine/" + name),
			}), fs.OK
		}
	}
	return nil, syscall.ENOENT
}

// ---------------------------------------------------------------------------
// StaticReadFile — generic read-only file with fixed content
// ---------------------------------------------------------------------------

type StaticReadFile struct {
	fs.Inode
	data    []byte
	inoPath string
}

var _ = (fs.NodeGetattrer)((*StaticReadFile)(nil))
var _ = (fs.NodeOpener)((*StaticReadFile)(nil))
var _ = (fs.NodeReader)((*StaticReadFile)(nil))

func (f *StaticReadFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Size = uint64(len(f.data))
	out.Ino = stableIno(f.inoPath)
	return fs.OK
}

func (f *StaticReadFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *StaticReadFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(f.data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return fuse.ReadResultData(f.data[off:end]), fs.OK
}
