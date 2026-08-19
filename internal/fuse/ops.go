package fuse

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"

	"fuel/api"
)

// 写路径实现 (PLAN §8.1, IMPL_DESIGN §6.3)。
//
// 语义约束（AGENTS.md §1.2 一次写多次读）：仅支持整文件创建与覆盖写
// （Create，或 Open 携带 O_WRONLY|O_TRUNC），不支持原地随机写 / 追加写
// （O_RDWR / O_APPEND / 无 O_TRUNC 的 O_WRONLY → ENOTSUP）。
// 写流程：Create/Write 暂存本地临时文件 → Flush 整文件 PutObject（INV-3）
// → 按 ARCH_SPEC §7.2 顺序失效缓存。

// writeTmpPrefix 写临时文件前缀。临时文件落在 {cache.dir}/{bucket} 下，
// 进程崩溃残留由 NewNVMeCache 启动时的 cleanOrphanTemps（".fuel-" 前缀）清理。
const writeTmpPrefix = ".fuel-write-"

// newWriteTmp 在与数据缓存相同的文件系统上创建写临时文件。
func (r *FuelRoot) newWriteTmp() (*os.File, error) {
	dir := filepath.Join(r.cfg.Cache.Dir, r.cfg.Storage.Bucket)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, writeTmpPrefix+"*")
}

// invalidateAfterWrite 写/删操作后的缓存失效（ARCH_SPEC §7.2）。
// L1 进程内必成功（保证单节点写后读强一致）；L2（metaEng）失败降级——
// 其他节点靠 TTL 兜底；数据缓存删除失败降级——陈旧条目在读时 ETag
// 不匹配自愈（INV-9）。两者不掩盖写操作本身的成功。
func (r *FuelRoot) invalidateAfterWrite(ctx context.Context, path string) {
	r.metaCache.DeleteStat(path)
	r.metaCache.DeleteNeg(path)
	r.metaCache.InvalidatePrefix(path)
	r.metaCache.DeleteDir(parentDir(path))
	if err := r.metaEng.Invalidate(ctx, path); err != nil {
		zap.L().Warn("invalidate L2 failed, other nodes rely on TTL", zap.String("path", path), zap.Error(err))
	}
	if err := r.dataCache.Remove(path); err != nil {
		zap.L().Warn("remove data cache failed, stale entry self-heals on etag mismatch", zap.String("path", path), zap.Error(err))
	}
}

// --- Create / Write / Flush / Fsync ---

// create 创建文件并返回写句柄（数据暂存临时文件，Flush 时整文件上传）。
// 目标已存在时要求 O_TRUNC（整文件覆盖写），否则拒绝原地修改。
func (r *FuelRoot) create(ctx context.Context, parent *fs.Inode, parentPath, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE == syscall.O_RDWR || flags&syscall.O_APPEND != 0 {
		return nil, nil, 0, syscall.ENOTSUP
	}
	childPath := pathJoin(parentPath, name)
	switch _, errno := r.getAttr(ctx, childPath); errno {
	case 0:
		if flags&syscall.O_TRUNC == 0 {
			return nil, nil, 0, syscall.ENOTSUP // 已存在且无 O_TRUNC：原地改写不支持
		}
	case syscall.ENOENT:
	default:
		return nil, nil, 0, errno
	}

	tmp, err := r.newWriteTmp()
	if err != nil {
		zap.L().Error("create: alloc write tmp failed", zap.String("key", childPath), zap.Error(err))
		return nil, nil, 0, syscall.EIO
	}
	// 新文件即将存在：清理可能存在的负缓存（含父目录）
	r.metaCache.DeleteNeg(childPath)
	if parentPath != "" {
		r.metaCache.DeleteNeg(parentPath)
	}

	child := &FuelNode{root: r, path: childPath}
	fh := &fileHandle{node: child, key: childPath, tmp: tmp}

	now := time.Now()
	me := &api.MetaEntry{
		Path:  childPath,
		Inode: api.InodeFromPath(childPath),
		Mode:  mode & 0o7777,
		Uid:   r.uid,
		Gid:   r.gid,
		MTime: now,
		ATime: now,
		Nlink: 1,
	}
	fillEntryOut(me, out)
	return parent.NewInode(ctx, child, fs.StableAttr{Mode: out.Mode, Ino: me.Inode}), fh, 0, 0
}

func (r *FuelRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return r.create(ctx, &r.Inode, "", name, flags, mode, out)
}

func (n *FuelNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return n.root.create(ctx, &n.Inode, n.path, name, flags, mode, out)
}

func (n *FuelNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	fh, ok := f.(*fileHandle)
	if !ok || fh == nil {
		return 0, syscall.EIO
	}
	return fh.write(data, off)
}

func (n *FuelNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	fh, ok := f.(*fileHandle)
	if !ok || fh == nil {
		return syscall.EIO
	}
	return fh.flush(ctx)
}

// Fsync 同 Flush（对象存储无 fsync 语义，IMPL_DESIGN §5.3）。
func (n *FuelNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	fh, ok := f.(*fileHandle)
	if !ok || fh == nil {
		return syscall.EIO
	}
	return fh.flush(ctx)
}

// --- Unlink ---

func (r *FuelRoot) unlink(ctx context.Context, parentPath, name string) syscall.Errno {
	key := pathJoin(parentPath, name)
	me, errno := r.getAttr(ctx, key)
	if errno != 0 {
		return errno
	}
	if me.IsDir {
		return syscall.EISDIR
	}
	if err := r.store.Delete(ctx, key); err != nil {
		zap.L().Error("unlink: delete object failed", zap.String("key", key), zap.Error(err))
		return syscall.EIO
	}
	r.invalidateAfterWrite(ctx, key)
	return 0
}

func (r *FuelRoot) Unlink(ctx context.Context, name string) syscall.Errno {
	return r.unlink(ctx, "", name)
}

func (n *FuelNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.root.unlink(ctx, n.path, name)
}

// --- Rename ---

// rename 通过 Copy + Delete 实现（IMPL_DESIGN §5.3）。
// 仅支持文件：目录 rename 需要递归拷贝前缀下全部对象，超出 MVP（PLAN §11 D9）。
// RENAME_NOREPLACE / RENAME_EXCHANGE 等 flags 不支持。
func (r *FuelRoot) rename(ctx context.Context, oldParent, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags != 0 {
		return syscall.ENOTSUP
	}
	var newDir string
	switch p := newParent.(type) {
	case *FuelNode:
		newDir = p.path
	case *FuelRoot:
	default:
		return syscall.EIO
	}

	oldKey := pathJoin(oldParent, name)
	newKey := pathJoin(newDir, newName)
	if oldKey == newKey {
		return 0
	}

	me, errno := r.getAttr(ctx, oldKey)
	if errno != 0 {
		return errno
	}
	if me.IsDir {
		return syscall.ENOTSUP
	}
	if err := r.store.Copy(ctx, oldKey, newKey); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return syscall.ENOENT
		}
		zap.L().Error("rename: copy failed", zap.String("old", oldKey), zap.String("new", newKey), zap.Error(err))
		return syscall.EIO
	}
	if err := r.store.Delete(ctx, oldKey); err != nil {
		zap.L().Error("rename: delete src failed", zap.String("key", oldKey), zap.Error(err))
		return syscall.EIO
	}
	r.invalidateAfterWrite(ctx, oldKey)
	r.invalidateAfterWrite(ctx, newKey)
	return 0
}

func (r *FuelRoot) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return r.rename(ctx, "", name, newParent, newName, flags)
}

func (n *FuelNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return n.root.rename(ctx, n.path, name, newParent, newName, flags)
}

// --- Mkdir / Rmdir ---

// mkdir 上传零字节占位对象 key+"/"（IMPL_DESIGN §5.3），并失效父目录列表缓存。
func (r *FuelRoot) mkdir(ctx context.Context, parent *fs.Inode, parentPath, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	key := pathJoin(parentPath, name)
	switch _, errno := r.getAttr(ctx, key); errno {
	case 0:
		return nil, syscall.EEXIST
	case syscall.ENOENT:
	default:
		return nil, errno
	}

	if _, err := r.store.Put(ctx, key+"/", bytes.NewReader(nil), 0); err != nil {
		zap.L().Error("mkdir: put marker failed", zap.String("key", key), zap.Error(err))
		return nil, syscall.EIO
	}
	r.invalidateAfterWrite(ctx, key)
	if parentPath != "" {
		r.metaCache.DeleteNeg(parentPath)
	}

	me := api.DirMetaEntry(key, r.uid, r.gid)
	if mode&0o7777 != 0 {
		me.Mode = mode & 0o7777
	}
	r.metaCache.SetStat(key, me)

	fillEntryOut(me, out)
	child := &FuelNode{root: r, path: key}
	return parent.NewInode(ctx, child, fs.StableAttr{Mode: out.Mode, Ino: me.Inode}), 0
}

func (r *FuelRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return r.mkdir(ctx, &r.Inode, "", name, mode, out)
}

func (n *FuelNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.root.mkdir(ctx, &n.Inode, n.path, name, mode, out)
}

// rmdir 删除占位对象。POSIX 语义要求目录为空：key+"/" 前缀下
// 除占位对象自身外存在任何子项 → ENOTEMPTY。
func (r *FuelRoot) rmdir(ctx context.Context, parentPath, name string) syscall.Errno {
	key := pathJoin(parentPath, name)
	marker := key + "/"

	entries, _, err := r.store.List(ctx, marker, "", 2)
	if err != nil {
		zap.L().Error("rmdir: list failed", zap.String("key", key), zap.Error(err))
		return syscall.EIO
	}
	hasMarker := false
	for _, e := range entries {
		if e.Key == marker {
			hasMarker = true
			continue
		}
		return syscall.ENOTEMPTY
	}
	if !hasMarker {
		return syscall.ENOENT
	}

	if err := r.store.Delete(ctx, marker); err != nil {
		zap.L().Error("rmdir: delete marker failed", zap.String("key", key), zap.Error(err))
		return syscall.EIO
	}
	r.invalidateAfterWrite(ctx, key)
	return 0
}

func (r *FuelRoot) Rmdir(ctx context.Context, name string) syscall.Errno {
	return r.rmdir(ctx, "", name)
}

func (n *FuelNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.root.rmdir(ctx, n.path, name)
}
