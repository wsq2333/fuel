package fuse

import (
	"context"
	"os"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// fileHandle 是打开文件的句柄。
// local 非空时持有缓存文件描述符（pread 零拷贝路径）；
// local 为空时延迟到首次 Read 通过 singleflight 拉取缓存。
type fileHandle struct {
	node  *FuelNode
	key   string
	etag  string
	size  int64
	local *os.File
	mu    sync.Mutex
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
	return fh, 0
}

// read 实现读逻辑（IMPL_DESIGN §6.1 / §6.2）。
func (fh *fileHandle) read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.mu.Lock()
	if fh.local == nil {
		localPath, err := fh.node.root.fetchAndCache(ctx, fh.key, fh.etag, fh.size)
		if err != nil {
			fh.mu.Unlock()
			return nil, syscall.EIO
		}
		f, err := os.Open(localPath)
		if err != nil {
			fh.mu.Unlock()
			return nil, syscall.EIO
		}
		fh.local = f
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
	return fuse.ReadResultData(dest[:n]), 0
}

// Release 关闭本地文件描述符。
func (fh *fileHandle) Release(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.local != nil {
		_ = fh.local.Close()
		fh.local = nil
	}
	return 0
}

var _ fs.FileReleaser = (*fileHandle)(nil)
