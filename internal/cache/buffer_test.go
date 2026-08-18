package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// TestBufferPool_DefaultSizes 默认构造取 5MB buffer + 兜底 maxBuffers。
func TestBufferPool_DefaultSizes(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{})
	if bp.BufferSize() != defaultBufferSize {
		t.Errorf("expected default buffer size %d, got %d", defaultBufferSize, bp.BufferSize())
	}
	if bp.MaxBuffers() != defaultMaxBuffers {
		t.Errorf("expected default maxBuffers %d, got %d", defaultMaxBuffers, bp.MaxBuffers())
	}
}

// TestBufferPool_ExplicitConfig 显式配置生效。
func TestBufferPool_ExplicitConfig(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1 << 20, MaxBuffers: 8})
	if bp.BufferSize() != 1<<20 {
		t.Errorf("expected 1MB buffer, got %d", bp.BufferSize())
	}
	if bp.MaxBuffers() != 8 {
		t.Errorf("expected 8 max buffers, got %d", bp.MaxBuffers())
	}
}

// TestBufferPool_Reuse 归还后再 Get 命中池。
func TestBufferPool_Reuse(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 4})

	buf := bp.Get()
	if len(buf) != 1024 {
		t.Fatalf("expected 1024-byte buffer, got %d", len(buf))
	}
	s := bp.Stats()
	if s.InUse != 1 || s.Hits != 0 || s.Misses != 1 {
		t.Errorf("after first Get: %+v", s)
	}

	bp.Put(buf)
	s = bp.Stats()
	if s.Idle != 1 || s.InUse != 0 {
		t.Errorf("after Put: %+v", s)
	}

	buf2 := bp.Get()
	s = bp.Stats()
	if s.Hits != 1 {
		t.Errorf("second Get should hit pool, got %+v", s)
	}
	if &buf2[0] != &buf[0] {
		t.Error("reused buffer should be the same allocation")
	}
}

// TestBufferPool_OverCapDropped 池满时 Put 丢弃 buffer。
func TestBufferPool_OverCapDropped(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 2})

	// 取出 3 个 buffer（第 3 个是空池新分配）
	b1 := bp.Get()
	b2 := bp.Get()
	b3 := bp.Get()
	// 全部归还：池容量 2，第 3 个被丢弃
	bp.Put(b1)
	bp.Put(b2)
	bp.Put(b3)

	s := bp.Stats()
	if s.Idle != 2 {
		t.Errorf("expected pool idle 2, got %d", s.Idle)
	}
	if s.Dropped != 1 {
		t.Errorf("expected 1 dropped, got %d", s.Dropped)
	}
}

// TestBufferPool_WrongSizeDropped 归还错误大小 buffer 被静默丢弃。
func TestBufferPool_WrongSizeDropped(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 4})
	bp.Put(make([]byte, 512)) // 错误大小
	bp.Put(make([]byte, 2048))
	bp.Put(nil)
	s := bp.Stats()
	if s.Idle != 0 {
		t.Errorf("wrong-size buffers should be dropped, idle=%d", s.Idle)
	}
}

// TestBufferPool_PutThenDropRefill 池满丢、取空后重新分配不 panic。
func TestBufferPool_PutThenDropRefill(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 2})
	var bufs [][]byte
	for i := 0; i < 5; i++ {
		bufs = append(bufs, bp.Get())
	}
	for _, b := range bufs {
		bp.Put(b)
	}
	// 池只保留 2 个
	bufs = nil
	for i := 0; i < 5; i++ {
		bufs = append(bufs, bp.Get())
		if len(bufs[i]) != 1024 {
			t.Fatalf("buffer %d wrong size", i)
		}
	}
}

// TestBufferPool_Concurrent 并发 Get/Put 无 data race 且统计一致。
func TestBufferPool_Concurrent(t *testing.T) {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 16})

	const workers = 32
	const loops = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				buf := bp.Get()
				buf[0] = byte(i)
				bp.Put(buf)
			}
		}()
	}
	// 另起一组：取而不还，模拟借出场景
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				bp.Get()
			}
			// 不归还，借出数累计
		}()
	}
	wg.Wait()

	s := bp.Stats()
	total := s.Hits + s.Misses
	expTotal := int64(workers*loops + 8*loops)
	if total != expTotal {
		t.Errorf("expected %d total Get calls, got %d", expTotal, total)
	}
	if s.Idle < 0 || s.InUse < 0 || s.Hits < 0 || s.Misses < 0 {
		t.Errorf("all stats should be non-negative: %+v", s)
	}
}

// TestParseCgroupLimitFile 验证 cgroup 限制文件解析。
func TestParseCgroupLimitFile(t *testing.T) {
	dir := t.TempDir()

	// 正常数值
	p1 := filepath.Join(dir, "max")
	if err := os.WriteFile(p1, []byte("8589934592\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if v, err := parseCgroupLimitFile(p1); err != nil || v != 8589934592 {
		t.Errorf("expected 8589934592, got %d err=%v", v, err)
	}

	// "max" 无限制 → 错误
	p2 := filepath.Join(dir, "unlimited")
	if err := os.WriteFile(p2, []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := parseCgroupLimitFile(p2); err == nil {
		t.Error("expected error for 'max'")
	}

	// 空文件 → 错误
	p3 := filepath.Join(dir, "empty")
	if err := os.WriteFile(p3, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := parseCgroupLimitFile(p3); err == nil {
		t.Error("expected error for empty file")
	}

	// 不存在 → 错误
	if _, err := parseCgroupLimitFile(filepath.Join(dir, "nope")); err == nil {
		t.Error("expected error for missing file")
	}
}

// TestMaxBuffersFromCgroup 验证 cgroup 限制驱动的池上限计算。
func TestMaxBuffersFromCgroup(t *testing.T) {
	// macOS 无 cgroup → 降级默认值
	if runtime.GOOS == "darwin" {
		if n := maxBuffersFromCgroup(1<<20, defaultPoolFraction); n != defaultMaxBuffers {
			t.Errorf("fallback expected %d, got %d", defaultMaxBuffers, n)
		}
	} else {
		// Linux 上允许读真实 cgroup，只要 n > 0 即可
		if n := maxBuffersFromCgroup(1<<20, defaultPoolFraction); n <= 0 {
			t.Errorf("expected positive pool size, got %d", n)
		}
	}
}

// TestBufferPool_FractionBounds 预算小数被夹紧到 [1, hardMaxBuffers]。
func TestBufferPool_FractionBounds(t *testing.T) {
	// 无法轻易 mock cgroup，验证显式超大 MaxBuffers 被夹到硬上限
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 100000})
	if bp.MaxBuffers() != hardMaxBuffers {
		t.Errorf("expected hard cap %d, got %d", hardMaxBuffers, bp.MaxBuffers())
	}
}

// ExampleBufferPool 演示 Get/Put 用法。
func ExampleBufferPool() {
	bp := NewBufferPool(BufferPoolConfig{BufferSize: 1024, MaxBuffers: 4})
	buf := bp.Get()
	copy(buf, "hello")
	fmt.Println(string(buf[:5]))
	bp.Put(buf)
	// Output: hello
}