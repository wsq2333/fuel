package fuse

import (
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sync/singleflight"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/config"
)

// FuelRoot 是挂载点根节点，持有所有依赖 (IMPL_DESIGN §5.2)。
// 所有 FuelNode 通过 root 指针共享这些依赖。
type FuelRoot struct {
	fs.Inode
	store     api.ObjectStore
	dataCache api.DataCache
	metaCache cache.MetaCache
	metaEng   api.MetadataEngine
	flight    singleflight.Group
	cfg       *config.Config
	uid       uint32
	gid       uint32
}

// NewFuelRoot 构造 FUSE 根节点。
func NewFuelRoot(
	store api.ObjectStore,
	dataCache api.DataCache,
	metaCache cache.MetaCache,
	metaEng api.MetadataEngine,
	cfg *config.Config,
) *FuelRoot {
	return &FuelRoot{
		store:     store,
		dataCache: dataCache,
		metaCache: metaCache,
		metaEng:   metaEng,
		cfg:       cfg,
		uid:       uint32(os.Getuid()),
		gid:       uint32(os.Getgid()),
	}
}

// FuelNode 代表一个文件或目录 (IMPL_DESIGN §5.2)。
// path 是对象存储 key（相对于 bucket，不含前导 "/"）。
type FuelNode struct {
	fs.Inode
	root *FuelRoot
	path string
}

// pathJoin 拼接父路径与子名称，返回规范化 key。
func pathJoin(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

// metaEntryToAttr 将 api.MetaEntry 转换为 fuse.Attr。
func metaEntryToAttr(me *api.MetaEntry, out *fuse.Attr) {
	out.Ino = me.Inode
	out.Size = uint64(me.Size)
	out.Mode = me.Mode
	if me.IsDir {
		out.Mode |= syscall.S_IFDIR
	} else {
		out.Mode |= syscall.S_IFREG
	}
	out.Nlink = me.Nlink
	out.Owner.Uid = me.Uid
	out.Owner.Gid = me.Gid
	out.Mtime = uint64(me.MTime.Unix())
	out.Mtimensec = uint32(me.MTime.Nanosecond())
	out.Atime = uint64(me.ATime.Unix())
	out.Atimensec = uint32(me.ATime.Nanosecond())
	out.Ctime = out.Mtime
	out.Ctimensec = out.Mtimensec
	out.Blksize = 4096
	out.Blocks = (out.Size + 511) / 512
}

// fillEntryOut 填充 fuse.EntryOut（Lookup 成功时返回）。
func fillEntryOut(me *api.MetaEntry, out *fuse.EntryOut) {
	metaEntryToAttr(me, &out.Attr)
	out.SetEntryTimeout(0)
	out.SetAttrTimeout(0)
}

// fillAttrOut 填充 fuse.AttrOut（Getattr 成功时返回）。
func fillAttrOut(me *api.MetaEntry, out *fuse.AttrOut) {
	metaEntryToAttr(me, &out.Attr)
	out.SetTimeout(0)
}

// getAttr 获取 path 的元数据，按 L1 → L2 → 真相来源 顺序查询。
// 命中 L1 时直接返回；miss 时回源并写回各级缓存。
// 返回 ENOENT 表示对象不存在（已写入负缓存）。
func (r *FuelRoot) getAttr(ctx context.Context, path string) (*api.MetaEntry, syscall.Errno) {
	if path == "" {
		return api.DirMetaEntry("/", r.uid, r.gid), 0
	}

	if entry, ok := r.metaCache.GetStat(path); ok {
		return entry, 0
	}
	if r.metaCache.GetNeg(path) {
		return nil, syscall.ENOENT
	}

	entry, err := r.metaEng.GetAttr(ctx, path)
	if err == nil {
		r.metaCache.SetStat(path, entry)
		return entry, 0
	}

	if errors.Is(err, syscall.ENOENT) {
		r.metaCache.SetNeg(path)
		return nil, syscall.ENOENT
	}
	return nil, syscall.EIO
}

// fetchAndCache 缓存未命中时从对象存储拉取整文件并写入数据缓存。
// 使用 singleflight 去重并发拉取 (IMPL_DESIGN §7.1)。
func (r *FuelRoot) fetchAndCache(ctx context.Context, key, etag string, size int64) (string, error) {
	v, err, _ := r.flight.Do(key, func() (interface{}, error) {
		reader, err := r.store.Get(ctx, key, 0, 0)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return r.dataCache.Put(key, etag, size, reader)
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// --- FuelRoot 自身作为根目录的 Node 方法 ---

// Lookup 查找根目录下的直接子项。
func (r *FuelRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := name
	me, errno := r.getAttr(ctx, childPath)
	if errno != 0 {
		return nil, errno
	}
	fillEntryOut(me, out)
	child := &FuelNode{root: r, path: childPath}
	stable := fs.StableAttr{Mode: out.Mode, Ino: me.Inode}
	return r.NewInode(ctx, child, stable), 0
}

// Getattr 返回根目录属性。
func (r *FuelRoot) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	me := api.DirMetaEntry("/", r.uid, r.gid)
	fillAttrOut(me, out)
	return 0
}

// Readdir 列出根目录（bucket 根前缀）。
func (r *FuelRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return r.listDir(ctx, "")
}

// --- FuelNode 的 Node 方法 ---

// Lookup 查找当前目录下的子项。
func (n *FuelNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := pathJoin(n.path, name)
	me, errno := n.root.getAttr(ctx, childPath)
	if errno != 0 {
		return nil, errno
	}
	fillEntryOut(me, out)
	child := &FuelNode{root: n.root, path: childPath}
	stable := fs.StableAttr{Mode: out.Mode, Ino: me.Inode}
	return n.NewInode(ctx, child, stable), 0
}

// Getattr 返回当前节点属性。
func (n *FuelNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	me, errno := n.root.getAttr(ctx, n.path)
	if errno != 0 {
		return errno
	}
	fillAttrOut(me, out)
	return 0
}

// Readdir 列出当前目录。
func (n *FuelNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return n.root.listDir(ctx, n.path)
}

// listDir 列出 dirPath 的直接子项，按 L1 → L2 → 真相来源 顺序查询。
// 命中 L2/真相来源时写回各级缓存，并将每个子项 Meta 预填充到 stat 缓存。
func (r *FuelRoot) listDir(ctx context.Context, dirPath string) (fs.DirStream, syscall.Errno) {
	if entries, ok := r.metaCache.GetDir(dirPath); ok {
		return dirStreamFrom(entries), 0
	}

	entries, err := r.metaEng.ListDir(ctx, dirPath)
	if err != nil {
		return nil, syscall.EIO
	}

	r.metaCache.SetDir(dirPath, entries)
	for i := range entries {
		if entries[i].Meta != nil {
			childPath := pathJoin(dirPath, entries[i].Name)
			r.metaCache.SetStat(childPath, entries[i].Meta)
		}
	}
	return dirStreamFrom(entries), 0
}

// dirStreamFrom 将 []api.DirEntry 转换为 fs.DirStream。
func dirStreamFrom(entries []api.DirEntry) fs.DirStream {
	list := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = syscall.S_IFDIR
		}
		list = append(list, fuse.DirEntry{
			Name: e.Name,
			Mode: mode,
			Ino:  e.Meta.Inode,
		})
	}
	return fs.NewListDirStream(list)
}

// --- 文件打开与读取 ---

// Open 打开文件。只读模式检查元数据并尝试命中数据缓存；写模式返回 ENOTSUP。
func (n *FuelNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.ENOTSUP
	}

	me, errno := n.root.getAttr(ctx, n.path)
	if errno != 0 {
		return nil, 0, errno
	}
	if me.IsDir {
		return nil, 0, syscall.EISDIR
	}

	key := n.path
	localPath, hit, err := n.root.dataCache.Get(key, me.ETag)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	var localPathForOpen string
	if hit {
		localPathForOpen = localPath
	}
	fh, errno := newFileHandle(n, key, me.ETag, me.Size, localPathForOpen)
	if errno != 0 {
		return nil, 0, errno
	}
	return fh, fuse.FOPEN_KEEP_CACHE, 0
}

// Read 从文件读取数据。缓存命中时 pread 本地文件；未命中时 singleflight 拉取后 pread。
func (n *FuelNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh, ok := f.(*fileHandle)
	if !ok || fh == nil {
		return nil, syscall.EIO
	}
	return fh.read(ctx, dest, off)
}

// --- 编译期接口检查 ---

var (
	_ fs.NodeLookuper  = (*FuelRoot)(nil)
	_ fs.NodeGetattrer = (*FuelRoot)(nil)
	_ fs.NodeReaddirer = (*FuelRoot)(nil)
	_ fs.NodeLookuper  = (*FuelNode)(nil)
	_ fs.NodeGetattrer = (*FuelNode)(nil)
	_ fs.NodeReaddirer = (*FuelNode)(nil)
	_ fs.NodeOpener    = (*FuelNode)(nil)
	_ fs.NodeReader    = (*FuelNode)(nil)
)
