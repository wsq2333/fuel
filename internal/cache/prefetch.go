package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"fuel/api"
	"golang.org/x/sync/errgroup"
)

// Prefetcher 是单个文件的预读状态管理器（借鉴 goofys 设计）。
// 每个打开的文件句柄持有一个 Prefetcher，根据读模式（顺序/乱序）决定预读策略。
// 线程安全，支持并发 Read。
type Prefetcher struct {
	key  string
	size int64

	store     api.ObjectStore
	dataCache api.DataCache

	mu              sync.Mutex
	lastOffset      int64 // 上次读的结束位置
	readaheadSize   int64 // 当前预读窗口大小（倍增）
	oooReadCount    int   // 乱序读计数
	prefetchRunning atomic.Bool

	// 配置
	enabled         bool
	initialReadahead int64
	maxReadahead     int64
	oooThreshold     int
}

// NewPrefetcher 构造预读器。
func NewPrefetcher(
	key string,
	size int64,
	store api.ObjectStore,
	dataCache api.DataCache,
	enabled bool,
	initialReadahead, maxReadahead int64,
) *Prefetcher {
	if initialReadahead <= 0 {
		initialReadahead = 1 << 20 // 1MB 默认
	}
	if maxReadahead <= 0 {
		maxReadahead = 16 << 20 // 16MB 默认
	}
	return &Prefetcher{
		key:              key,
		size:             size,
		store:            store,
		dataCache:        dataCache,
		enabled:          enabled,
		initialReadahead: initialReadahead,
		maxReadahead:     maxReadahead,
		readaheadSize:    initialReadahead,
		oooThreshold:     3, // 连续 3 次乱序读禁用预读
	}
}

// OnRead 在每次 Read 后调用，更新预读状态并触发预读。
// offset 是本次读的起始位置，n 是读取的字节数。
func (p *Prefetcher) OnRead(ctx context.Context, etag string, offset int64, n int) {
	if !p.enabled {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	readEnd := offset + int64(n)

	// 顺序读检测：本次读的起始位置在上次读结束位置附近（向前，允许小幅间隙）。
	// 不把向后跳跃（seek backward）当作顺序读。
	isSequential := offset >= p.lastOffset && offset <= p.lastOffset+int64(n)

	if isSequential {
		// 顺序读：倍增预读窗口
		p.oooReadCount = 0
		if p.readaheadSize < p.maxReadahead {
			p.readaheadSize *= 2
			if p.readaheadSize > p.maxReadahead {
				p.readaheadSize = p.maxReadahead
			}
		}

		p.lastOffset = readEnd

		// 触发异步预读：预读从 readEnd 开始的 readaheadSize 字节
		nextOffset := readEnd
		if nextOffset >= p.size {
			return // 已到文件末尾
		}
		prefetchEnd := nextOffset + p.readaheadSize
		if prefetchEnd > p.size {
			prefetchEnd = p.size
		}

		// 如果已有预读在进行，跳过（避免重复预读）
		if !p.prefetchRunning.CompareAndSwap(false, true) {
			return
		}

		// 使用 context.Background()：预读是后台优化，不应受 FUSE 请求 context 取消影响。
		go p.doPrefetch(context.Background(), etag, nextOffset, prefetchEnd)
	} else {
		// 乱序读：累计计数，超过阈值禁用预读
		p.oooReadCount++
		if p.oooReadCount >= p.oooThreshold {
			p.enabled = false
			p.readaheadSize = 0
		} else {
			// 重置预读窗口为初始值
			p.readaheadSize = p.initialReadahead
		}
		p.lastOffset = readEnd
	}
}

// doPrefetch 异步预读 [start, end) 范围的数据到缓存。
// 成功或失败都不影响正常读路径（仅优化），静默失败。
func (p *Prefetcher) doPrefetch(ctx context.Context, etag string, start, end int64) {
	defer p.prefetchRunning.Store(false)

	// 检查缓存是否已存在完整文件
	if p.dataCache.Contains(p.key, etag) {
		return
	}

	// Range GET [start, end)
	length := end - start
	if length <= 0 {
		return
	}

	reader, err := p.store.Get(ctx, p.key, start, length)
	if err != nil {
		// 预读失败静默忽略（下次正常读会重试）
		return
	}
	defer reader.Close()

	// 将预读数据丢弃（只触发对象存储侧的 page cache/CDN 缓存）。
	// 如果要写入本地缓存，需要支持 partial cache（当前 INV-2 要求整文件缓存）。
	// Phase 2 的预读主要优化对象存储侧的预热，不改变 INV-2。
	_, _ = io.Copy(io.Discard, reader)
}

// Reset 重置预读状态（文件 seek 时调用，避免错误预测）。
func (p *Prefetcher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastOffset = 0
	p.readaheadSize = p.initialReadahead
	p.oooReadCount = 0
}

const (
	defaultConcurrentBlockSize = 4 << 20 // 4MB
	defaultConcurrency          = 4
)

var _ api.ConcurrentPutter = (*nvmeCache)(nil)

// PutConcurrent 并发拉取整对象并写入缓存 (PLAN §4.2)。
//
// 缓存未命中大文件（size > blockSize）时，按 blockSize 切分为多个 block，
// 并发 goroutine 各自发起 GET Range [blockStart, blockEnd)，
// 用 pwrite (os.File.WriteAt) 写入同一临时文件对应偏移。
// 全部 block 成功后 fsync + atomic rename 为正式缓存文件并注册索引 (INV-2 整文件缓存)。
// 任意 block 失败 → 等待其他 goroutine 退出 → 清理临时文件 → 返回错误，
// 不污染索引（拉取中断/半拉取不入库，IMPL_DESIGN §7.3）。
//
// 磁盘错误（ENOSPC/EIO）和网络错误（GET Range 失败）均导致整体失败，
// 调用方（singleflight.Do）会在下次访问时重新发起完整拉取。
func (c *nvmeCache) PutConcurrent(
	ctx context.Context,
	key, etag string,
	size int64,
	store api.ObjectStore,
	concurrency, blockSize int64,
) (localPath string, err error) {
	if !sanitizeKey(key) {
		return "", fmt.Errorf("invalid cache key %q", key)
	}
	if size <= 0 {
		return "", fmt.Errorf("size must be positive, got %d", size)
	}
	if store == nil {
		return "", fmt.Errorf("ObjectStore is required")
	}
	if c.maxFileSize > 0 && size > c.maxFileSize {
		return "", fmt.Errorf("file %s size %d exceeds maxFileSize %d, skip caching", key, size, c.maxFileSize)
	}
	if blockSize <= 0 {
		blockSize = defaultConcurrentBlockSize
	}
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	if concurrency > (size+blockSize-1)/blockSize {
		concurrency = (size + blockSize - 1) / blockSize
	}

	finalPath := c.localPath(key)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return "", fmt.Errorf("create cache dir for %s: %w", key, err)
	}

	if c.needEviction(size) {
		c.evictFor(size)
	}

	tmp, tmpPath, err := c.createConcurrentTmp(finalPath, size)
	if err != nil {
		return "", fmt.Errorf("create tmp for %s: %w", key, err)
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	blocks := planBlocks(size, blockSize)
	if gerr := c.fetchBlocksConcurrently(ctx, store, key, tmp, blocks, concurrency); gerr != nil {
		err = gerr
		return "", fmt.Errorf("concurrent fetch %s: %w", key, gerr)
	}

	if err = c.finalizeTmp(tmp, tmpPath, finalPath); err != nil {
		return "", fmt.Errorf("finalize cache %s: %w", key, err)
	}

	c.index.put(&cacheEntry{
		Key:       key,
		ETag:      etag,
		Size:      size,
		LocalPath: finalPath,
	})
	return finalPath, nil
}

// blockRange 描述一个待拉取的 block 范围 [start, end)，end <= size。
type blockRange struct {
	start, end int64
}

// planBlocks 按 blockSize 切分 [0, size) 为连续 block 列表。
func planBlocks(size, blockSize int64) []blockRange {
	var blocks []blockRange
	for off := int64(0); off < size; off += blockSize {
		end := off + blockSize
		if end > size {
			end = size
		}
		blocks = append(blocks, blockRange{start: off, end: end})
	}
	return blocks
}

// createConcurrentTmp 创建临时文件并预分配 size 字节（避免并发 pwrite 时碎片化）。
// 使用 fallocate 失败不致命（如文件系统不支持），仅记录。
func (c *nvmeCache) createConcurrentTmp(finalPath string, size int64) (*os.File, string, error) {
	dir := filepath.Dir(finalPath)
	tmp, err := os.CreateTemp(dir, tmpFilePrefix+"*")
	if err != nil {
		return nil, "", fmt.Errorf("create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Truncate(size); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return nil, "", fmt.Errorf("truncate tmp to %d: %w", size, err)
	}
	return tmp, tmpPath, nil
}

// fetchBlocksConcurrently 并发拉取所有 block，每个 goroutine pwrite 到对应偏移。
// 任意 block 失败 → errgroup 短路其余 goroutine，等待全部退出后返回首个错误。
// concurrency 限制同时进行的 GET Range 数量（信号量）。
func (c *nvmeCache) fetchBlocksConcurrently(
	ctx context.Context,
	store api.ObjectStore,
	key string,
	dst *os.File,
	blocks []blockRange,
	concurrency int64,
) error {
	g, gctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, concurrency)

	for i := range blocks {
		b := blocks[i]
		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			return gctx.Err()
		}
		g.Go(func() error {
			defer func() { <-sem }()
			return c.fetchBlock(gctx, store, key, dst, b)
		})
	}
	return g.Wait()
}

// fetchBlock 拉取单个 block 并 pwrite 到临时文件对应偏移。
// 复用 errgroup 上下文：gctx 取消时未启动的 block 不再发起 GET。
func (c *nvmeCache) fetchBlock(
	ctx context.Context,
	store api.ObjectStore,
	key string,
	dst *os.File,
	b blockRange,
) error {
	length := b.end - b.start
	reader, err := store.Get(ctx, key, b.start, length)
	if err != nil {
		return fmt.Errorf("GET %s [%d,%d): %w", key, b.start, b.end, err)
	}
	defer reader.Close()

	written, err := io.Copy(&pwriteAtWriter{f: dst, off: b.start}, reader)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			return fmt.Errorf("pwrite %s @%d: %w (disk full)", key, b.start, err)
		}
		return fmt.Errorf("pwrite %s @%d: %w", key, b.start, err)
	}
	if written != length {
		return fmt.Errorf("short block %s [%d,%d): wrote %d, want %d", key, b.start, b.end, written, length)
	}
	return nil
}

// pwriteAtWriter 适配 io.Copy 到 os.File.WriteAt，固定起始偏移。
// 每次 Write 调用推进 off；多个 goroutine 各自持独立实例，pwrite 互不影响。
type pwriteAtWriter struct {
	f   *os.File
	off int64
}

func (w *pwriteAtWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	if err != nil {
		return n, err
	}
	w.off += int64(n)
	return n, nil
}

// finalizeTmp fsync 临时文件、关闭、atomic rename 为正式缓存路径。
// 失败时调用方清理临时文件。
func (c *nvmeCache) finalizeTmp(tmp *os.File, tmpPath, finalPath string) error {
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename tmp to final: %w", err)
	}
	return nil
}

// 4.3 小文件批量预取 (PLAN §4.3)。
// 检测同目录连续小文件 Open（如 PyTorch DataLoader 顺序扫一个目录的样本），
// 当连续数超过 triggerThreshold 时，异步批量预取同目录下后续 batchSize 个文件。
// 小文件判定: size <= smallFileThreshold（默认 1MB）。
// 静默失败（优化性质，不影响主路径）。
const (
	batchPrefetchTriggerThreshold = 3
	batchPrefetchBatchSize        = 7
	smallFileThreshold            = 1 << 20 // 1MB
)

// BatchPrefetcher 跟踪同目录的连续 Open 行为，触发批量预取。
// 线程安全；状态只在 Open 时更新。
type BatchPrefetcher struct {
	mu sync.Mutex

	lastDir    string // 上一次 Open 的目录
	consec     int    // 同目录连续 Open 次数
	prefetched map[string]struct{} // 已预取的目录（避免重复触发）

	enabled bool
}

// NewBatchPrefetcher 构造批量预取器。
func NewBatchPrefetcher(enabled bool) *BatchPrefetcher {
	return &BatchPrefetcher{
		enabled:    enabled,
		prefetched: make(map[string]struct{}),
	}
}

// OnOpen 在文件 Open 时调用，跟踪同目录连续 Open 并决定是否触发批量预取。
// 返回 true 表示应触发批量预取；调用方在 goroutine 中执行实际预取。
// size 用于判定是否为小文件（<= smallFileThreshold 才计入连续计数）。
func (b *BatchPrefetcher) OnOpen(dir string, size int64) bool {
	if !b.enabled || size > smallFileThreshold {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if dir == b.lastDir {
		b.consec++
	} else {
		b.lastDir = dir
		b.consec = 1
	}

	if b.consec < batchPrefetchTriggerThreshold {
		return false
	}
	if _, done := b.prefetched[dir]; done {
		return false
	}
	b.prefetched[dir] = struct{}{}
	return true
}

// Reset 清空跟踪状态（目录 TTL 失效或显式重置时调用）。
func (b *BatchPrefetcher) Reset(dir string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.prefetched, dir)
	if b.lastDir == dir {
		b.lastDir = ""
		b.consec = 0
	}
}

// prefetchAfter 返回批量预取的目标：dirPath 下除已打开文件外的前 batchSize 个小文件。
// 调用方持有 entries（来自 metaCache.GetDir 或 listDir），此处仅过滤和截断。
// 返回的 key 是完整对象 key（pathJoin(dir, name)）。
func PrefetchAfter(dirPath string, opened string, entries []api.DirEntry, limit int) []string {
	if limit <= 0 {
		limit = batchPrefetchBatchSize
	}
	out := make([]string, 0, limit)
	for _, e := range entries {
		if e.IsDir || e.Meta == nil {
			continue
		}
		key := pathJoinKey(dirPath, e.Name)
		if key == opened {
			continue
		}
		if e.Meta.Size > smallFileThreshold {
			continue
		}
		out = append(out, key)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// pathJoinKey 拼接目录与文件名，返回对象存储 key。
// 与 fuse 包 pathJoin 一致，但定义在 cache 包内避免循环依赖。
func pathJoinKey(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
