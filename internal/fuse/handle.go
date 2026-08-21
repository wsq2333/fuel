package fuse

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/monitor"
)

// fileHandle 是打开文件的句柄。
// 读句柄：local 非空时持有缓存文件描述符（pread 零拷贝路径）；
// local 为空时延迟到首次 Read 通过 singleflight 拉取缓存。
// 写句柄（tmp 非空，Create/O_TRUNC 创建）：Write 写本地临时文件，
// Flush 时整文件 PutObject 上传（IMPL_DESIGN §6.3）。
type fileHandle struct {
	node       *FuelNode
	key        string
	etag       string
	size       int64
	local      *os.File
	prefetcher *cache.Prefetcher
	mu         sync.Mutex

	// 写路径状态。tmp 非空表示写句柄且尚未上传；Flush 成功后置 nil。
	tmp     *os.File
	written int64
}

// newFileHandle 构造 fileHandle。localPath 非空时立即打开本地文件。
func newFileHandle(node *FuelNode, key, etag string, size int64, localPath string) (*fileHandle, syscall.Errno) {
	fh := &fileHandle{node: node, key: key, etag: etag, size: size}
	if localPath != "" {
		f, err := os.Open(localPath)
		if err != nil {
			return nil, syscall.EIO
		}
		fh.local = f
	}

	// 初始化预读器（Phase 2）
	cfg := node.root.cfg.Prefetch
	fh.prefetcher = cache.NewPrefetcher(
		key, size,
		node.root.store,
		node.root.dataCache,
		cfg.Enabled,
		cfg.Readahead.Initial,
		cfg.Readahead.Max,
	)

	return fh, 0
}

// read 实现读逻辑（IMPL_DESIGN §6.1 / §6.2）。
func (fh *fileHandle) read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	start := time.Now()
	defer func() { monitor.ObserveFuseRead(time.Since(start)) }()

	fh.mu.Lock()
	if fh.local == nil {
		zap.L().Debug("read cache miss, fetching", zap.String("key", fh.key), zap.Int64("off", off))
		localPath, err := fh.node.root.fetchAndCache(ctx, fh.key, fh.etag, fh.size)
		if err != nil {
			fh.mu.Unlock()
			zap.L().Error("read: fetch from object store failed", zap.String("key", fh.key), zap.Error(err))
			return nil, syscall.EIO
		}
		f, err := os.Open(localPath)
		if err != nil {
			fh.mu.Unlock()
			zap.L().Error("read: open cached file failed", zap.String("key", fh.key), zap.String("path", localPath), zap.Error(err))
			return nil, syscall.EIO
		}
		fh.local = f
	} else {
		zap.L().Debug("read cache hit", zap.String("key", fh.key), zap.Int64("off", off), zap.Int("len", len(dest)))
	}
	local := fh.local
	fh.mu.Unlock()

	if off >= fh.size {
		return fuse.ReadResultData(nil), 0
	}
	n, err := local.ReadAt(dest, off)
	if err != nil && n == 0 {
		return nil, syscall.EIO
	}

	// 触发预读（Phase 2）
	if fh.prefetcher != nil && n > 0 {
		fh.prefetcher.OnRead(ctx, fh.etag, off, n)
	}

	return fuse.ReadResultData(dest[:n]), 0
}

// write 将数据写入本地临时文件（整文件上传前的暂存）。
// Flush 后 tmp 已清理，再写返回 ENOTSUP（一次写语义，不支持写后追写）。
func (fh *fileHandle) write(data []byte, off int64) (uint32, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.tmp == nil {
		return 0, syscall.ENOTSUP
	}
	n, err := fh.tmp.WriteAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	if end := off + int64(n); end > fh.written {
		fh.written = end
	}
	return uint32(n), 0
}

// flush 将写临时文件整文件上传对象存储（INV-3），随后按 §7.2 失效缓存并回写
// L1 stat（新 ETag）。幂等：tmp 为 nil（从未写或已上传）时直接返回成功。
func (fh *fileHandle) flush(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.flushLocked(ctx)
}

// flushLocked 是 flush 的锁内版本，调用方必须已持有 fh.mu。
func (fh *fileHandle) flushLocked(ctx context.Context) syscall.Errno {
	if fh.tmp == nil {
		return 0
	}
	root := fh.node.root
	if _, err := fh.tmp.Seek(0, 0); err != nil {
		zap.L().Error("flush: seek tmp failed", zap.String("key", fh.key), zap.Error(err))
		return syscall.EIO
	}
	om, err := root.store.Put(ctx, fh.key, fh.tmp, fh.written)
	if err != nil {
		zap.L().Error("flush: put object failed", zap.String("key", fh.key), zap.Error(err))
		return syscall.EIO
	}
	root.invalidateAfterWrite(ctx, fh.key)
	// L1 写入最新元数据：单节点写后读强一致（ARCH_SPEC §7.2）。
	root.metaCache.SetStat(fh.key, api.MetaEntryFromObjectMeta(om, root.uid, root.gid))

	// 新内容尽力写入数据缓存（写完即读场景免回源）；失败下次读回源，不致命。
	if _, serr := fh.tmp.Seek(0, 0); serr == nil {
		if _, perr := root.dataCache.Put(fh.key, om.ETag, fh.written, fh.tmp); perr != nil {
			zap.L().Warn("flush: populate data cache failed", zap.String("key", fh.key), zap.Error(perr))
		}
	}
	fh.etag = om.ETag
	fh.size = fh.written
	fh.closeTmpLocked()
	return 0
}

// closeTmpLocked 关闭并删除写临时文件，调用方必须已持有 fh.mu。
func (fh *fileHandle) closeTmpLocked() {
	if fh.tmp == nil {
		return
	}
	_ = fh.tmp.Close()
	_ = os.Remove(fh.tmp.Name())
	fh.tmp = nil
}

// Release 关闭本地文件描述符。写句柄若未经 Flush（异常路径），兜底尽力上传，
// 避免静默丢数据；上传失败仍清理临时文件。
func (fh *fileHandle) Release(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.local != nil {
		_ = fh.local.Close()
		fh.local = nil
	}
	if fh.tmp != nil {
		errno := fh.flushLocked(ctx)
		fh.closeTmpLocked()
		if errno != 0 {
			return errno
		}
	}
	return 0
}

var _ fs.FileReleaser = (*fileHandle)(nil)
