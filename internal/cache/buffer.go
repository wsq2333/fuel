package cache

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// 5.1 BufferPool (PLAN §5.1)：借鉴 goofys 的内存 buffer 池。
//
// 复用固定大小（默认 5MB）的 []byte buffer，避免大对象频繁分配/GC 压力。
//
// 设计：
//   - 池用 buffered channel 实现，容量即 maxBuffers；
//   - Get 非阻塞：池有则复用，无则新分配（不阻塞调用方）；
//   - Put 非阻塞：池未满则归还，池满则丢弃（GC 回收），避免高负载下池内存膨胀突破容器内存限制；
//   - inUse 记录借出中的 buffer 数（Get 增 / Put 减）；
//   - hits/misses/dropped 统计池命中/新分配/丢弃次数（用于监控 fuel_buffer_*）。
//
// cgroup 感知：构造时读取容器内存限制（cgroup v2 /sys/fs/cgroup/memory.max
// 或 v1 memory.limit_in_bytes），池上限 = (memLimit * fraction) / bufferSize。
// macOS / 非容器 / 读取失败时降级为 defaultMaxBuffers（50）。

const (
	// defaultBufferSize 默认单 buffer 大小（5MB，匹配 readahead 上限）。
	defaultBufferSize = 5 << 20

	// defaultPoolFraction 池内存占容器内存限制的比例（10%）。
	// 例如 8GB 容器 → 池预算 800MB → 160 个 5MB buffer。
	defaultPoolFraction = 0.10

	// defaultMaxBuffers 池上限兜底值（无法读取 cgroup 时）。
	// 50 个 5MB buffer = 250MB。
	defaultMaxBuffers = 50

	// hardMaxBuffers 硬上限，防止配置错误导致 OOM。
	hardMaxBuffers = 1024
)

// BufferPool 是固定大小 []byte 的复用池。线程安全（channel + atomic）。
type BufferPool struct {
	ch chan []byte

	bufferSize int
	maxBuffers int

	inUse   atomic.Int64 // 借出中的 buffer 数
	hits    atomic.Int64 // 池命中次数
	misses  atomic.Int64 // 新分配次数
	dropped atomic.Int64 // 池满丢弃次数
}

// BufferPoolConfig BufferPool 配置。
type BufferPoolConfig struct {
	BufferSize int     // 单 buffer 字节数；<=0 时取 defaultBufferSize
	MaxBuffers int     // 池上限；<=0 时根据 cgroup 计算
	Fraction   float64 // 池内存占容器内存限制的比例；(0,1]，0 时取默认
}

// NewBufferPool 构造 BufferPool。
// 显式 MaxBuffers > 0 时直接使用；否则尝试从 cgroup 读取内存限制推算。
func NewBufferPool(cfg BufferPoolConfig) *BufferPool {
	size := cfg.BufferSize
	if size <= 0 {
		size = defaultBufferSize
	}

	maxBuf := cfg.MaxBuffers
	if maxBuf <= 0 {
		frac := cfg.Fraction
		if frac <= 0 {
			frac = defaultPoolFraction
		}
		maxBuf = maxBuffersFromCgroup(size, frac)
	}
	if maxBuf > hardMaxBuffers {
		maxBuf = hardMaxBuffers
	}

	return &BufferPool{
		ch:         make(chan []byte, maxBuf),
		bufferSize: size,
		maxBuffers: maxBuf,
	}
}

// Get 从池获取一个 buffer。池空时新分配，永不阻塞。
// 返回的 buffer 长度为 bufferSize，始终非 nil。
func (bp *BufferPool) Get() []byte {
	select {
	case b := <-bp.ch:
		bp.inUse.Add(1)
		bp.hits.Add(1)
		return b
	default:
		bp.inUse.Add(1)
		bp.misses.Add(1)
		return make([]byte, bp.bufferSize)
	}
}

// Put 归还 buffer 到池。池满时丢弃（GC 回收）。
// nil 或长度不等于 bufferSize 的 buf 被静默丢弃（防御外部传入错误大小的 buffer）。
func (bp *BufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}
	bp.inUse.Add(-1)
	if len(buf) != bp.bufferSize {
		return
	}
	select {
	case bp.ch <- buf:
	default:
		bp.dropped.Add(1)
	}
}

// BufferSize 返回单 buffer 大小。
func (bp *BufferPool) BufferSize() int {
	return bp.bufferSize
}

// MaxBuffers 返回池上限。
func (bp *BufferPool) MaxBuffers() int {
	return bp.maxBuffers
}

// BufferPoolStats 池统计（用于监控 fuel_buffer_*）。
type BufferPoolStats struct {
	Idle    int64 // 池内空闲数（len(ch) 快照）
	InUse   int64 // 借出中数
	Hits    int64 // 池命中次数
	Misses  int64 // 新分配次数
	Dropped int64 // 池满丢弃次数
}

// Stats 返回池统计。Idle 是瞬时快照，与 InUse 求和近似等于创建的 buffer 总数。
func (bp *BufferPool) Stats() BufferPoolStats {
	return BufferPoolStats{
		Idle:    int64(len(bp.ch)),
		InUse:   bp.inUse.Load(),
		Hits:    bp.hits.Load(),
		Misses:  bp.misses.Load(),
		Dropped: bp.dropped.Load(),
	}
}

// maxBuffersFromCgroup 根据容器内存限制计算池上限。
// 读取失败（非容器 / macOS / cgroup 未挂载）时返回 defaultMaxBuffers。
func maxBuffersFromCgroup(bufferSize int, fraction float64) int {
	limit, err := readCgroupMemoryLimit()
	if err != nil || limit <= 0 {
		return defaultMaxBuffers
	}
	budget := int64(float64(limit) * fraction)
	n := budget / int64(bufferSize)
	if n <= 0 {
		return 1
	}
	if n > hardMaxBuffers {
		return hardMaxBuffers
	}
	return int(n)
}

// readCgroupMemoryLimit 读取容器内存限制（字节）。
// 优先 cgroup v2 (/sys/fs/cgroup/memory.max)，回退 v1 (/sys/fs/cgroup/memory/memory.limit_in_bytes)。
// "max"（无限制）视为不可用，返回错误。
func readCgroupMemoryLimit() (int64, error) {
	if v, err := parseCgroupLimitFile("/sys/fs/cgroup/memory.max"); err == nil {
		return v, nil
	}
	if v, err := parseCgroupLimitFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		return v, nil
	}
	return 0, errors.New("cgroup memory limit not available")
}

// parseCgroupLimitFile 读取并解析 cgroup 限制文件。
// 内容为 "max"（cgroup v2 无限制标记）时返回错误，由调用方降级。
func parseCgroupLimitFile(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0, errors.New("unlimited")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	return v, nil
}