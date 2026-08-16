package cache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"fuel/api"
)

// tmpFilePrefix 是写入临时文件的前缀（os.CreateTemp 生成 .fuel-XXXXXX）。
// 正式缓存文件名是对象 key，不含此前缀，二者命名隔离。
const tmpFilePrefix = ".fuel-"

// nvmeCache 是 DataCache 的 NVMe 整文件缓存实现 (INV-2)。
// 缓存文件路径 = {dir}/{bucket}/{key}，内容为对象完整字节副本。
type nvmeCache struct {
	dir                string
	bucket             string
	capacity           int64
	highWatermarkBytes int64
	lowWatermarkBytes  int64
	maxFileSize        int64

	index *cacheIndex

	hits   atomic.Int64
	misses atomic.Int64
}

// NewNVMeCache 构造 NVMe 数据缓存。dir 为缓存根目录，bucket 决定路径前缀 {dir}/{bucket}。
// highWatermark/lowWatermark 为容量水位（0~1 的小数，相对 capacity），maxFileSize 为字节数。
func NewNVMeCache(dir, bucket string, capacity int64, highWatermark, lowWatermark float64, maxFileSize int64) (api.DataCache, error) {
	if dir == "" {
		return nil, fmt.Errorf("cache dir is required")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("cache capacity must be positive, got %d", capacity)
	}
	if highWatermark <= 0 || highWatermark > 1 || lowWatermark <= 0 || lowWatermark > 1 {
		return nil, fmt.Errorf("watermarks must be in (0,1], got high=%v low=%v", highWatermark, lowWatermark)
	}
	if lowWatermark >= highWatermark {
		return nil, fmt.Errorf("lowWatermark (%v) must be < highWatermark (%v)", lowWatermark, highWatermark)
	}
	root := filepath.Join(dir, bucket)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create cache root %s: %w", root, err)
	}
	c := &nvmeCache{
		dir:                dir,
		bucket:             bucket,
		capacity:           capacity,
		highWatermarkBytes: int64(float64(capacity) * highWatermark),
		lowWatermarkBytes:  int64(float64(capacity) * lowWatermark),
		maxFileSize:        maxFileSize,
		index:              newCacheIndex(),
	}
	c.cleanOrphanTemps()
	return c, nil
}

// localPath 返回 key 对应的缓存文件路径 {dir}/{bucket}/{key}。
func (c *nvmeCache) localPath(key string) string {
	return filepath.Join(c.dir, c.bucket, filepath.FromSlash(key))
}

// Get 查找缓存。命中返回本地文件路径并更新 LRU；ETag 不匹配则删除缓存并返回 miss。
func (c *nvmeCache) Get(key, etag string) (localPath string, hit bool, err error) {
	if !sanitizeKey(key) {
		return "", false, fmt.Errorf("invalid cache key %q", key)
	}
	entry, ok := c.index.get(key)
	if !ok {
		c.misses.Add(1)
		return "", false, nil
	}
	if entry.ETag != etag {
		// ETag 变化 → 缓存失效，删除条目和文件
		c.index.remove(key)
		_ = os.Remove(entry.LocalPath)
		c.misses.Add(1)
		return "", false, nil
	}
	if _, statErr := os.Stat(entry.LocalPath); statErr != nil {
		// 索引有但文件丢失（如外部清理）→ 移除索引，返回 miss
		c.index.remove(key)
		c.misses.Add(1)
		return "", false, nil
	}
	c.hits.Add(1)
	return entry.LocalPath, true, nil
}

// Put 流式写入整文件缓存（临时文件 + fsync + atomic rename），返回本地文件路径。
// size > maxFileSize 时不缓存，返回错误（上层降级为直透对象存储）。
// 写入前若超过高水位则触发 LRU 淘汰；写入遇 ENOSPC 时再淘汰重试一次。
func (c *nvmeCache) Put(key, etag string, size int64, r io.Reader) (localPath string, err error) {
	if !sanitizeKey(key) {
		return "", fmt.Errorf("invalid cache key %q", key)
	}
	if c.maxFileSize > 0 && size > c.maxFileSize {
		return "", fmt.Errorf("file %s size %d exceeds maxFileSize %d, skip caching", key, size, c.maxFileSize)
	}

	finalPath := c.localPath(key)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return "", fmt.Errorf("create cache dir for %s: %w", key, err)
	}

	if c.needEviction(size) {
		c.evictFor(size)
	}

	written, err := c.writeTmpAndRename(finalPath, r)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			// 磁盘满 → 再淘汰一次后重试
			c.evictFor(size)
			written, err = c.writeTmpAndRename(finalPath, r)
		}
		if err != nil {
			return "", fmt.Errorf("write cache %s: %w", key, err)
		}
	}

	c.index.put(&cacheEntry{
		Key:       key,
		ETag:      etag,
		Size:      written,
		LocalPath: finalPath,
	})
	return finalPath, nil
}

// writeTmpAndRename 在同目录写临时文件，fsync 后原子 rename 为正式缓存文件。
// 返回实际写入字节数。临时文件与目标同目录以保证 rename 原子性（同文件系统）。
func (c *nvmeCache) writeTmpAndRename(finalPath string, r io.Reader) (int64, error) {
	dir := filepath.Dir(finalPath)
	tmp, err := os.CreateTemp(dir, tmpFilePrefix+"*")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	// 任何失败都清理临时文件
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()

	written, err := io.Copy(tmp, r)
	if err != nil {
		return written, err
	}
	if err := tmp.Sync(); err != nil {
		return written, err
	}
	if err := tmp.Close(); err != nil {
		return written, err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return written, err
	}
	return written, nil
}

// cleanOrphanTemps 清理缓存根目录下残留的写入临时文件（.fuel-*）。
// 这些文件是进程在 io.Copy/Rename 前被强杀（kill -9/panic/断电）留下的半成品，
// 不会被索引收录、永不命中、永不淘汰，会泄漏 NVMe 空间。启动时清理。
// 单个文件删除失败不阻塞启动（继续清理其余）。返回清理的文件数。
func (c *nvmeCache) cleanOrphanTemps() int {
	root := filepath.Join(c.dir, c.bucket)
	removed := 0
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), tmpFilePrefix) {
			return nil
		}
		if removeErr := os.Remove(path); removeErr == nil {
			removed++
		}
		return nil
	})
	return removed
}

// Remove 删除缓存条目及对应磁盘文件。key 不存在时不报错。
func (c *nvmeCache) Remove(key string) error {
	if !sanitizeKey(key) {
		return fmt.Errorf("invalid cache key %q", key)
	}
	entry := c.index.remove(key)
	if entry == nil {
		return nil
	}
	if err := os.Remove(entry.LocalPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove cache file %s: %w", entry.LocalPath, err)
	}
	return nil
}

// Contains 判断指定 etag 的缓存是否存在（不改变 LRU 顺序）。
func (c *nvmeCache) Contains(key, etag string) bool {
	if !sanitizeKey(key) {
		return false
	}
	entry, ok := c.index.peek(key)
	if !ok {
		return false
	}
	if entry.ETag != etag {
		return false
	}
	_, err := os.Stat(entry.LocalPath)
	return err == nil
}

// Stats 返回缓存统计。
func (c *nvmeCache) Stats() api.CacheStats {
	used, count, evicted := c.index.stats()
	return api.CacheStats{
		HitCount:      c.hits.Load(),
		MissCount:     c.misses.Load(),
		UsedBytes:     used,
		CapacityBytes: c.capacity,
		EntryCount:    count,
		EvictionCount: evicted,
	}
}

// sanitizeKey 校验 key 不含路径穿越（防御性，正常 key 由对象存储保证）。
func sanitizeKey(key string) bool {
	return key != "" && !strings.Contains(key, "..") && !filepath.IsAbs(key)
}
