package fuse

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"fuel/internal/config"
)

// Mount 将 FUSE 文件系统挂载到 {cfg.Fuse.MountPoint}/{cfg.Storage.Bucket} (ARCH_SPEC §6)。
// 返回的 *fuse.Server 已启动 goroutine 处理内核请求；调用方通过 Wait 阻塞直到 Unmount。
// 挂载失败返回明确错误（§3.2-3），不 panic。
func Mount(root *FuelRoot, cfg *config.Config) (*fuse.Server, error) {
	mountPoint := filepath.Join(cfg.Fuse.MountPoint, cfg.Storage.Bucket)
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return nil, fmt.Errorf("create mount point %s: %w", mountPoint, err)
	}

	attrTTL := cfg.Metadata.Cache.StatTTL
	if attrTTL <= 0 {
		attrTTL = time.Second
	}
	dirTTL := cfg.Metadata.Cache.DirTTL
	if dirTTL <= 0 {
		dirTTL = time.Second
	}

	nodeFS := fs.NewNodeFS(root, &fs.Options{
		EntryTimeout:    &dirTTL,
		AttrTimeout:     &attrTTL,
		NegativeTimeout: &attrTTL,
		UID:             root.uid,
		GID:             root.gid,
	})

	maxRead := cfg.Fuse.MaxRead
	if maxRead <= 0 {
		maxRead = 1 << 20
	}

	server, err := fuse.NewServer(nodeFS, mountPoint, &fuse.MountOptions{
		AllowOther:    true,
		MaxWrite:      maxRead,
		MaxReadAhead:  maxRead,
		MaxBackground: 16,
		FsName:        "fuel-" + cfg.Storage.Bucket,
		Name:          "fuel",
		Options:       cfg.Fuse.Options,
	})
	if err != nil {
		return nil, fmt.Errorf("fuse mount %s: %w", mountPoint, err)
	}
	return server, nil
}