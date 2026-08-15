package metadata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"fuel/api"
	"fuel/internal/config"
)

// directEngine 是模式 A：直查对象存储的元数据引擎。
// 无本地存储，每次调用直接查询 ObjectStore，结果不回写（缓存由上层 L1/L2 负责）。
type directEngine struct {
	store api.ObjectStore
	uid   uint32
	gid   uint32
}

// init 注册 direct 引擎到工厂。
func init() {
	RegisterMetadataEngine("direct", newDirectEngine)
}

// newDirectEngine 构造 direct 引擎。uid/gid 取挂载进程的实际值。
func newDirectEngine(cfg *config.Config, store api.ObjectStore) (api.MetadataEngine, error) {
	if store == nil {
		return nil, fmt.Errorf("direct engine requires a non-nil ObjectStore")
	}
	return &directEngine{
		store: store,
		uid:   uint32(os.Getuid()),
		gid:   uint32(os.Getgid()),
	}, nil
}

// GetAttr 获取 path 的元数据。
// 对象存储无目录实体，按以下顺序推断：
//  1. path 为根 ("/") → 目录
//  2. HEAD key → 文件
//  3. HEAD key+"/" → 显式目录标记 (mkdir 创建的 0 字节对象)
//  4. List(key+"/", maxKeys=1) 有子项 → 隐式目录（由 key 前缀构成）
//  5. 都不存在 → ENOENT
func (e *directEngine) GetAttr(ctx context.Context, path string) (*api.MetaEntry, error) {
	key := normalizeKey(path)
	if key == "" {
		return api.DirMetaEntry("/", e.uid, e.gid), nil
	}

	om, err := e.store.Head(ctx, key)
	if err == nil {
		return api.MetaEntryFromObjectMeta(om, e.uid, e.gid), nil
	}
	if !errors.Is(err, syscall.ENOENT) {
		return nil, fmt.Errorf("head %s: %w", key, err)
	}

	dirKey := key + "/"
	if _, err := e.store.Head(ctx, dirKey); err == nil {
		return api.DirMetaEntry(path, e.uid, e.gid), nil
	} else if !errors.Is(err, syscall.ENOENT) {
		return nil, fmt.Errorf("head %s: %w", dirKey, err)
	}

	entries, prefixes, err := e.store.List(ctx, dirKey, "/", 1)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dirKey, err)
	}
	if len(entries) > 0 || len(prefixes) > 0 {
		return api.DirMetaEntry(path, e.uid, e.gid), nil
	}

	return nil, fmt.Errorf("path %s: %w", path, syscall.ENOENT)
}

// SetAttr 在 direct 模式下是 no-op（无本地存储，直查即最新）。
func (e *directEngine) SetAttr(ctx context.Context, path string, entry *api.MetaEntry) error {
	return nil
}

// DeleteAttr 在 direct 模式下是 no-op。
func (e *directEngine) DeleteAttr(ctx context.Context, path string) error {
	return nil
}

// ListDir 列出目录的直接子项（内联元数据，避免 N+1 HEAD）。
// 通过 List(prefix=dir/, delimiter="/") 同时得到文件（entries）与子目录（prefixes）。
func (e *directEngine) ListDir(ctx context.Context, dirPath string) ([]api.DirEntry, error) {
	key := normalizeKey(dirPath)
	prefix := key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	entries, prefixes, err := e.store.List(ctx, prefix, "/", 0)
	if err != nil {
		return nil, fmt.Errorf("list dir %s: %w", prefix, err)
	}

	result := make([]api.DirEntry, 0, len(entries)+len(prefixes))
	for _, cp := range prefixes {
		name := strings.TrimSuffix(strings.TrimPrefix(cp, prefix), "/")
		if name == "" {
			continue
		}
		result = append(result, api.DirEntry{
			Name:  name,
			IsDir: true,
			Meta:  api.DirMetaEntry(strings.TrimSuffix(cp, "/"), e.uid, e.gid),
		})
	}
	for _, obj := range entries {
		name := strings.TrimPrefix(obj.Key, prefix)
		if name == "" {
			continue
		}
		// 目录标记对象 (key 以 "/" 结尾的 0 字节对象) 不作为文件子项返回
		if strings.HasSuffix(name, "/") {
			continue
		}
		result = append(result, api.DirEntry{
			Name:  name,
			IsDir: false,
			Meta: api.MetaEntryFromObjectMeta(&api.ObjectMeta{
				Key:  obj.Key,
				Size: obj.Size,
			}, e.uid, e.gid),
		})
	}
	return result, nil
}

// SetDir 在 direct 模式下是 no-op。
func (e *directEngine) SetDir(ctx context.Context, dirPath string, entries []api.DirEntry) error {
	return nil
}

// DeleteDir 在 direct 模式下是 no-op。
func (e *directEngine) DeleteDir(ctx context.Context, dirPath string) error {
	return nil
}

// BatchGetAttr 并发直查多个 path。direct 模式逐个 Head（无批量接口）。
func (e *directEngine) BatchGetAttr(ctx context.Context, paths []string) (map[string]*api.MetaEntry, error) {
	result := make(map[string]*api.MetaEntry, len(paths))
	for _, p := range paths {
		entry, err := e.GetAttr(ctx, p)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			return nil, err
		}
		result[p] = entry
	}
	return result, nil
}

// Invalidate 在 direct 模式下是 no-op（无缓存可失效）。
func (e *directEngine) Invalidate(ctx context.Context, path string) error {
	return nil
}

// HealthCheck 检查对象存储可达性（通过 bucket 根 List 一次）。
func (e *directEngine) HealthCheck(ctx context.Context) error {
	if _, _, err := e.store.List(ctx, "", "/", 1); err != nil {
		return fmt.Errorf("object store unreachable: %w", err)
	}
	return nil
}

// Close 释放资源。direct 模式无持有资源。
func (e *directEngine) Close() error {
	return nil
}

// normalizeKey 将 FUSE 路径 ("/a/b") 归一化为对象存储 key ("a/b")。
func normalizeKey(path string) string {
	return strings.Trim(path, "/")
}
