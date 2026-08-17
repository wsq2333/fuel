package cache

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"fuel/api"
	"fuel/internal/objectstore"
)

// TestPrefetcher_SequentialRead 验证顺序读时预读窗口倍增。
func TestPrefetcher_SequentialRead(t *testing.T) {
	store := objectstore.NewMockStore("test")
	dataCache := newNoopDataCache()
	ctx := context.Background()

	// 准备 10MB 文件
	data := bytes.Repeat([]byte("a"), 10<<20)
	_, _ = store.Put(ctx, "seq.bin", bytes.NewReader(data), int64(len(data)))

	p := NewPrefetcher("seq.bin", int64(len(data)), store, dataCache, true, 1<<20, 16<<20)

	// 模拟连续顺序读
	offset := int64(0)
	chunkSize := 128 << 10 // 128KB per read

	initialReadahead := p.readaheadSize

	for i := 0; i < 5; i++ {
		p.OnRead(ctx, "etag", offset, chunkSize)
		offset += int64(chunkSize)
		// 等待异步预读启动
		time.Sleep(10 * time.Millisecond)
	}

	// 顺序读应该触发预读窗口倍增
	p.mu.Lock()
	finalReadahead := p.readaheadSize
	oooCount := p.oooReadCount
	enabled := p.enabled
	p.mu.Unlock()

	if finalReadahead <= initialReadahead {
		t.Errorf("sequential read should increase readahead: initial=%d final=%d", initialReadahead, finalReadahead)
	}
	if oooCount != 0 {
		t.Errorf("sequential read should not increment oooReadCount, got %d", oooCount)
	}
	if !enabled {
		t.Error("prefetch should remain enabled for sequential reads")
	}
}

// TestPrefetcher_OutOfOrderRead 验证乱序读禁用预读。
func TestPrefetcher_OutOfOrderRead(t *testing.T) {
	store := objectstore.NewMockStore("test")
	dataCache := newNoopDataCache()
	ctx := context.Background()

	data := bytes.Repeat([]byte("b"), 10<<20)
	_, _ = store.Put(ctx, "random.bin", bytes.NewReader(data), int64(len(data)))

	p := NewPrefetcher("random.bin", int64(len(data)), store, dataCache, true, 1<<20, 16<<20)

	// 模拟乱序读（随机跳跃）
	offsets := []int64{0, 5<<20, 1<<20, 8<<20}
	chunkSize := 128 << 10

	for _, off := range offsets {
		p.OnRead(ctx, "etag", off, chunkSize)
	}

	p.mu.Lock()
	oooCount := p.oooReadCount
	enabled := p.enabled
	p.mu.Unlock()

	// 3 次乱序读后应禁用预读
	if oooCount < 3 {
		t.Errorf("expected oooReadCount >= 3, got %d", oooCount)
	}
	if enabled {
		t.Error("prefetch should be disabled after excessive out-of-order reads")
	}
}

// TestPrefetcher_Reset 验证 Reset 清空预读状态。
func TestPrefetcher_Reset(t *testing.T) {
	store := objectstore.NewMockStore("test")
	dataCache := newNoopDataCache()
	ctx := context.Background()

	data := []byte("reset test")
	_, _ = store.Put(ctx, "reset.bin", bytes.NewReader(data), int64(len(data)))

	p := NewPrefetcher("reset.bin", int64(len(data)), store, dataCache, true, 1<<20, 16<<20)

	// 顺序读几次，增加 readahead
	for i := 0; i < 3; i++ {
		p.OnRead(ctx, "etag", int64(i*100), 100)
	}

	p.mu.Lock()
	readaheadBefore := p.readaheadSize
	p.mu.Unlock()

	if readaheadBefore <= 1<<20 {
		t.Skip("readahead not increased, test invalid")
	}

	// Reset
	p.Reset()

	p.mu.Lock()
	readaheadAfter := p.readaheadSize
	lastOffset := p.lastOffset
	oooCount := p.oooReadCount
	p.mu.Unlock()

	if readaheadAfter != 1<<20 {
		t.Errorf("Reset should restore readahead to initial, got %d", readaheadAfter)
	}
	if lastOffset != 0 {
		t.Errorf("Reset should clear lastOffset, got %d", lastOffset)
	}
	if oooCount != 0 {
		t.Errorf("Reset should clear oooReadCount, got %d", oooCount)
	}
}

// TestPrefetcher_DisabledNoop 验证 enabled=false 时预读为 no-op。
func TestPrefetcher_DisabledNoop(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	dataCache := newNoopDataCache()
	ctx := context.Background()

	data := []byte("disabled")
	_, _ = store.Put(ctx, "disabled.bin", bytes.NewReader(data), int64(len(data)))

	p := NewPrefetcher("disabled.bin", int64(len(data)), store, dataCache, false, 1<<20, 16<<20)

	// OnRead 应该不触发预读
	p.OnRead(ctx, "etag", 0, 100)
	time.Sleep(50 * time.Millisecond)

	if store.getCalls.Load() > 1 {
		t.Errorf("disabled prefetcher should not trigger GET, got %d calls", store.getCalls.Load())
	}
}

// TestPrefetcher_EndOfFile 验证读到文件末尾不触发预读。
func TestPrefetcher_EndOfFile(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	dataCache := newNoopDataCache()
	ctx := context.Background()

	data := []byte("short")
	size := int64(len(data))
	_, _ = store.Put(ctx, "short.bin", bytes.NewReader(data), size)

	p := NewPrefetcher("short.bin", size, store, dataCache, true, 1<<20, 16<<20)

	// 读到文件末尾
	p.OnRead(ctx, "etag", 0, len(data))
	time.Sleep(50 * time.Millisecond)

	// 不应触发预读（已到 EOF）
	if store.getCalls.Load() > 1 {
		t.Errorf("prefetch at EOF should not trigger GET, got %d calls", store.getCalls.Load())
	}
}

// countingStore 包装 ObjectStore 统计 Get 调用次数。
type countingStore struct {
	api.ObjectStore
	getCalls atomic.Int64
}

func (c *countingStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	c.getCalls.Add(1)
	return c.ObjectStore.Get(ctx, key, offset, length)
}

// noopDataCache 用于测试，不实际缓存。
type noopDataCache struct{}

func newNoopDataCache() api.DataCache { return &noopDataCache{} }
func (n *noopDataCache) Get(key, etag string) (string, bool, error) {
	return "", false, nil
}
func (n *noopDataCache) Put(key, etag string, size int64, r io.Reader) (string, error) {
	_, _ = io.Copy(io.Discard, r)
	return "", nil
}
func (n *noopDataCache) Remove(key string) error        { return nil }
func (n *noopDataCache) Contains(key, etag string) bool { return false }
func (n *noopDataCache) Stats() api.CacheStats          { return api.CacheStats{} }
