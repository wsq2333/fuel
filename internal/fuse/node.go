package fuse

import (
	"context"
	"errors"
	"os"
	"strings"
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
	batch     *cache.BatchPrefetcher // 小文件批量预取 (PLAN §4.3)
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
		batch:     cache.NewBatchPrefetcher(cfg.Prefetch.Enabled),
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

// prefetchBatch 异步批量预取 dirPath 下除 opened 外的后续小文件 (PLAN §4.3)。
// 复用 listDirEntries 获取目录 entries（多数场景命中 L1 dir cache，因为 PyTorch
// 通常先 readdir 再批量 open）。对每个未缓存的小文件调用 fetchAndCache（singleflight
// 自动去重，已并发拉取的不会再拉）。
// 静默失败（优化性质，不影响主路径）。
func (r *FuelRoot) prefetchBatch(ctx context.Context, opened string) {
	dir := parentDir(opened)
	go func() {
		bgCtx := context.Background()
		entries, errno := r.listDirEntries(bgCtx, dir)
		if errno != 0 {
			return
		}
		targets := cache.PrefetchAfter(dir, opened, entries, 0)
		for _, key := range targets {
			// 先取元数据（通常命中 listDirEntries 预填的 L1），拿到真实 ETag 再判存。
			// 空 ETag 无法做身份校验（INV-9），跳过预取。
			me, errno := r.getAttr(bgCtx, key)
			if errno != 0 || me == nil || me.ETag == "" {
				continue
			}
			if r.dataCache.Contains(key, me.ETag) {
				continue
			}
			if _, err := r.fetchAndCache(bgCtx, key, me.ETag, me.Size); err != nil {
				continue
			}
		}
	}()
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
	entries, errno := r.listDirEntries(ctx, dirPath)
	if errno != 0 {
		return nil, errno
	}
	return dirStreamFrom(entries), 0
}

// listDirEntries 返回 dirPath 的 []DirEntry（不写 DirStream 包装）。
// 命中 L1 dir cache 直接返回；miss 时回源 ListDir，写回 L1 dir cache，
// 并将每个子项 Meta 预填充到 L1 stat 缓存（避免后续 stat N+1）。
// 若某子项 Meta 为 nil（ListDir 实现未内联元数据），并发调用 BatchGetAttr 补齐
// (PLAN §4.3 readdir 元数据并行预取)。
func (r *FuelRoot) listDirEntries(ctx context.Context, dirPath string) ([]api.DirEntry, syscall.Errno) {
	if entries, ok := r.metaCache.GetDir(dirPath); ok {
		return entries, 0
	}

	entries, err := r.metaEng.ListDir(ctx, dirPath)
	if err != nil {
		return nil, syscall.EIO
	}

	r.fillMissingMeta(ctx, dirPath, entries)

	r.metaCache.SetDir(dirPath, entries)
	for i := range entries {
		if entries[i].Meta == nil {
			continue
		}
		// 文件缺 ETag（fillMissingMeta 失败的降级路径）不预填 stat 缓存：
		// 空 ETag 会让 Open 的身份校验失效（INV-9），下次访问回源 HEAD 获取。
		if !entries[i].IsDir && entries[i].Meta.ETag == "" {
			continue
		}
		childPath := pathJoin(dirPath, entries[i].Name)
		r.metaCache.SetStat(childPath, entries[i].Meta)
	}
	return entries, 0
}

// fillMissingMeta 对 entries 中 Meta 为 nil 或 Meta.ETag 为空的项，
// 通过 metaEng.BatchGetAttr 批量补齐元数据（readdir 元数据并行预取）。
// 单发 BatchGetAttr 调用让 Redis/MySQL 引擎能用 MGET/pipeline 并发 N 个 HEAD，
// direct 引擎则内部并发 fallback 到 GetAttr。
func (r *FuelRoot) fillMissingMeta(ctx context.Context, dirPath string, entries []api.DirEntry) {
	var paths []string
	var idxs []int
	for i, e := range entries {
		if e.IsDir {
			continue
		}
		if e.Meta != nil && e.Meta.ETag != "" {
			continue
		}
		paths = append(paths, pathJoin(dirPath, e.Name))
		idxs = append(idxs, i)
	}
	if len(paths) == 0 {
		return
	}
	fetched, err := r.metaEng.BatchGetAttr(ctx, paths)
	if err != nil {
		return
	}
	for j, p := range paths {
		if me, ok := fetched[p]; ok && me != nil {
			entries[idxs[j]].Meta = me
		}
	}
}

// dirStreamFrom 将 []api.DirEntry 转换为 fs.DirStream。
func dirStreamFrom(entries []api.DirEntry) fs.DirStream {
	list := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = syscall.S_IFDIR
		}
		var ino uint64
		if e.Meta != nil {
			ino = e.Meta.Inode
		} else {
			ino = api.InodeFromPath(e.Name)
		}
		list = append(list, fuse.DirEntry{
			Name: e.Name,
			Mode: mode,
			Ino:  ino,
		})
	}
	return fs.NewListDirStream(list)
}

// --- 文件打开与读取 ---

// Open 打开文件。只读模式检查元数据并尝试命中数据缓存；
// O_WRONLY|O_TRUNC 为整文件覆盖写（一次写语义，见 ops.go）；
// 其余写模式（O_RDWR / O_APPEND / 无 O_TRUNC 的原地写）返回 ENOTSUP。
func (n *FuelNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		if flags&syscall.O_ACCMODE == syscall.O_WRONLY &&
			flags&syscall.O_TRUNC != 0 && flags&syscall.O_APPEND == 0 {
			return n.openForWrite(ctx)
		}
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

	// 4.3 小文件批量预取：检测同目录连续小文件 Open，触发异步预取后续文件。
	if n.root.batch.OnOpen(parentDir(n.path), me.Size) {
		n.root.prefetchBatch(ctx, n.path)
	}

	return fh, fuse.FOPEN_KEEP_CACHE, 0
}

// openForWrite 以整文件覆盖语义打开已有对象（O_CREAT 路径由 Create 处理）。
func (n *FuelNode) openForWrite(ctx context.Context) (fs.FileHandle, uint32, syscall.Errno) {
	me, errno := n.root.getAttr(ctx, n.path)
	if errno != 0 {
		return nil, 0, errno
	}
	if me.IsDir {
		return nil, 0, syscall.EISDIR
	}
	tmp, err := n.root.newWriteTmp()
	if err != nil {
		return nil, 0, syscall.EIO
	}
	n.root.metaCache.DeleteNeg(n.path)
	return &fileHandle{node: n, key: n.path, tmp: tmp}, 0, 0
}

// parentDir 返回对象 key 的父目录（无 "/" 时为 ""）。
func parentDir(key string) string {
	i := strings.LastIndex(key, "/")
	if i < 0 {
		return ""
	}
	return key[:i]
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
	_ fs.NodeCreater   = (*FuelRoot)(nil)
	_ fs.NodeMkdirer   = (*FuelRoot)(nil)
	_ fs.NodeUnlinker  = (*FuelRoot)(nil)
	_ fs.NodeRmdirer   = (*FuelRoot)(nil)
	_ fs.NodeRenamer   = (*FuelRoot)(nil)

	_ fs.NodeLookuper  = (*FuelNode)(nil)
	_ fs.NodeGetattrer = (*FuelNode)(nil)
	_ fs.NodeReaddirer = (*FuelNode)(nil)
	_ fs.NodeOpener    = (*FuelNode)(nil)
	_ fs.NodeReader    = (*FuelNode)(nil)
	_ fs.NodeCreater   = (*FuelNode)(nil)
	_ fs.NodeWriter    = (*FuelNode)(nil)
	_ fs.NodeFlusher   = (*FuelNode)(nil)
	_ fs.NodeFsyncer   = (*FuelNode)(nil)
	_ fs.NodeMkdirer   = (*FuelNode)(nil)
	_ fs.NodeUnlinker  = (*FuelNode)(nil)
	_ fs.NodeRmdirer   = (*FuelNode)(nil)
	_ fs.NodeRenamer   = (*FuelNode)(nil)
)
