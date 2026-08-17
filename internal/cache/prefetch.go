package cache

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"fuel/api"
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

	// 顺序读检测：本次读的起始位置 ≈ 上次读的结束位置（允许小幅重叠）
	isSequential := offset >= p.lastOffset-int64(n) && offset <= p.lastOffset+int64(n)

	if isSequential {
		// 顺序读：倍增预读窗口
		p.oooReadCount = 0
		if p.readaheadSize < p.maxReadahead {
			p.readaheadSize *= 2
			if p.readaheadSize > p.maxReadahead {
				p.readaheadSize = p.maxReadahead
			}
		}

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

		go p.doPrefetch(ctx, etag, nextOffset, prefetchEnd)
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
	}

	p.lastOffset = readEnd
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
