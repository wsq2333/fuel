package cache

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fuel/api"
	"fuel/internal/objectstore"
)

// TestPrefetcher_BackwardSeek 验证向后 seek（回到文件开头）被正确检测为乱序读。
func TestPrefetcher_BackwardSeek(t *testing.T) {
	store := objectstore.NewMockStore("test")
	dataCache := newNoopDataCache()
	ctx := context.Background()

	data := bytes.Repeat([]byte("x"), 10<<20)
	_, _ = store.Put(ctx, "seek.bin", bytes.NewReader(data), int64(len(data)))

	p := NewPrefetcher("seek.bin", int64(len(data)), store, dataCache, true, 1<<20, 16<<20)

	chunkSize := 128 << 10

	// 顺序读推进到 512KB
	for i := 0; i < 4; i++ {
		p.OnRead(ctx, "etag", int64(i*chunkSize), chunkSize)
	}

	p.mu.Lock()
	ooo := p.oooReadCount
	p.mu.Unlock()
	if ooo != 0 {
		t.Fatalf("sequential reads should not increment ooo, got %d", ooo)
	}

	// 向后 seek 到 offset=0（典型场景：重新扫描文件头）
	p.OnRead(ctx, "etag", 0, chunkSize)

	p.mu.Lock()
	ooo = p.oooReadCount
	p.mu.Unlock()
	if ooo == 0 {
		t.Error("backward seek (to offset 0) should be detected as out-of-order")
	}
}

// TestPrefetcher_ConcurrentOnRead 多 goroutine 并发调用 OnRead 不 panic、不 data race。
func TestPrefetcher_ConcurrentOnRead(t *testing.T) {
	store := objectstore.NewMockStore("test")
	dataCache := newNoopDataCache()
	ctx := context.Background()

	data := bytes.Repeat([]byte("c"), 10<<20)
	_, _ = store.Put(ctx, "conc.bin", bytes.NewReader(data), int64(len(data)))

	p := NewPrefetcher("conc.bin", int64(len(data)), store, dataCache, true, 1<<20, 16<<20)

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			off := int64(idx) * 64 << 10
			p.OnRead(ctx, "etag", off, 64<<10)
		}(i)
	}
	wg.Wait()
	// 无 panic、race detector 不报错即通过
}

// TestPrefetcher_ZeroSizeFile 文件大小为 0 时预读不会触发。
func TestPrefetcher_ZeroSizeFile(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	dataCache := newNoopDataCache()
	ctx := context.Background()

	_, _ = store.Put(ctx, "empty", bytes.NewReader(nil), 0)

	p := NewPrefetcher("empty", 0, store, dataCache, true, 1<<20, 16<<20)

	// OnRead with n=0 (can happen for empty file read at EOF)
	p.OnRead(ctx, "etag", 0, 0)
	time.Sleep(30 * time.Millisecond)

	if store.getCalls.Load() > 0 {
		t.Errorf("zero-size file should not trigger prefetch, got %d GET calls", store.getCalls.Load())
	}
}

// TestPrefetcher_ReadaheadMaxCap 验证预读窗口倍增不超过 maxReadahead。
func TestPrefetcher_ReadaheadMaxCap(t *testing.T) {
	store := objectstore.NewMockStore("test")
	dataCache := newNoopDataCache()
	ctx := context.Background()

	size := int64(100 << 20) // 100MB
	data := bytes.Repeat([]byte("m"), int(size))
	_, _ = store.Put(ctx, "max.bin", bytes.NewReader(data), size)

	maxRA := int64(4 << 20) // cap at 4MB
	p := NewPrefetcher("max.bin", size, store, dataCache, true, 1<<20, maxRA)

	// 连续顺序读 10 次 → readahead 应 1MB→2MB→4MB→4MB→...
	chunkSize := 128 << 10
	offset := int64(0)
	for i := 0; i < 10; i++ {
		p.OnRead(ctx, "etag", offset, chunkSize)
		offset += int64(chunkSize)
		time.Sleep(5 * time.Millisecond)
	}

	p.mu.Lock()
	ra := p.readaheadSize
	p.mu.Unlock()

	if ra > maxRA {
		t.Errorf("readaheadSize %d should not exceed maxReadahead %d", ra, maxRA)
	}
}

// TestPrefetcher_DisabledAfterOOO_CannotReEnable 验证禁用后调用 OnRead 不再重新启用。
func TestPrefetcher_DisabledAfterOOO_CannotReEnable(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	dataCache := newNoopDataCache()
	ctx := context.Background()

	size := int64(10 << 20)
	data := bytes.Repeat([]byte("d"), int(size))
	_, _ = store.Put(ctx, "dis.bin", bytes.NewReader(data), size)

	p := NewPrefetcher("dis.bin", size, store, dataCache, true, 1<<20, 16<<20)

	// 触发 3 次乱序读禁用
	p.OnRead(ctx, "etag", 0, 1<<10)
	p.OnRead(ctx, "etag", 5<<20, 1<<10)
	p.OnRead(ctx, "etag", 1<<20, 1<<10)
	p.OnRead(ctx, "etag", 8<<20, 1<<10) // 第 4 次确保禁用

	// 等待任何正在进行的异步预读完成
	time.Sleep(100 * time.Millisecond)

	p.mu.Lock()
	if p.enabled {
		p.mu.Unlock()
		t.Fatal("should be disabled after OOO threshold")
	}
	p.mu.Unlock()

	// 重置计数器，验证后续读不触发新的 GET
	store.getCalls.Store(0)

	// 即使后续是完美顺序读，也不会重新启用（除非 Reset）
	for i := 0; i < 5; i++ {
		p.OnRead(ctx, "etag", int64(i)*1<<10, 1<<10)
	}
	time.Sleep(50 * time.Millisecond)
	if store.getCalls.Load() > 0 {
		t.Errorf("disabled prefetcher should not trigger GET even for sequential reads, got %d", store.getCalls.Load())
	}
}

// TestPrefetcher_CacheAlreadyExists 缓存已存在时 doPrefetch 跳过下载。
func TestPrefetcher_CacheAlreadyExists(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	ctx := context.Background()

	size := int64(10 << 20)
	data := bytes.Repeat([]byte("e"), int(size))
	_, _ = store.Put(ctx, "exists.bin", bytes.NewReader(data), size)

	// 使用 containsDataCache 模拟缓存已存在
	dc := &containsDataCache{contains: true}

	p := NewPrefetcher("exists.bin", size, store, dc, true, 1<<20, 16<<20)

	// 顺序读触发预读 → doPrefetch 应看到 Contains=true 并跳过
	p.OnRead(ctx, "etag", 0, 128<<10)
	time.Sleep(50 * time.Millisecond)

	if store.getCalls.Load() > 0 {
		t.Errorf("prefetch should skip when cache already exists, got %d GET calls", store.getCalls.Load())
	}
}

// TestPrefetcher_StoreGetFails 对象存储 GET 失败时不影响正常读路径。
func TestPrefetcher_StoreGetFails(t *testing.T) {
	failStore := &alwaysFailStore{}
	dataCache := newNoopDataCache()
	ctx := context.Background()

	p := NewPrefetcher("fail.bin", 10<<20, failStore, dataCache, true, 1<<20, 16<<20)

	// OnRead 触发预读，store.Get 失败 → 不 panic，静默处理
	p.OnRead(ctx, "etag", 0, 128<<10)
	time.Sleep(50 * time.Millisecond)

	// 验证 prefetchRunning 状态正确恢复
	if p.prefetchRunning.Load() {
		t.Error("prefetchRunning should be false after doPrefetch completes (even on failure)")
	}
}

// TestBatchPrefetcher_Reset 验证 Reset 清除特定目录的预取记录。
func TestBatchPrefetcher_ResetDir(t *testing.T) {
	bp := NewBatchPrefetcher(true)

	// 触发目录 d 的批量预取
	for i := 0; i < batchPrefetchTriggerThreshold; i++ {
		bp.OnOpen("d", 100)
	}

	// Reset 清除 d 的记录
	bp.Reset("d")

	// 应能重新触发
	for i := 0; i < batchPrefetchTriggerThreshold; i++ {
		triggered := bp.OnOpen("d", 100)
		if i == batchPrefetchTriggerThreshold-1 && !triggered {
			t.Error("should re-trigger after Reset")
		}
	}
}

// TestBatchPrefetcher_Disabled_Edge 禁用时永远不触发（含目录切换）。
func TestBatchPrefetcher_Disabled_Edge(t *testing.T) {
	bp := NewBatchPrefetcher(false)
	for i := 0; i < 50; i++ {
		if bp.OnOpen("d", 100) {
			t.Fatal("disabled BatchPrefetcher should never trigger")
		}
	}
	// 切换目录也不触发
	for i := 0; i < 50; i++ {
		if bp.OnOpen("other", 100) {
			t.Fatal("disabled BatchPrefetcher should never trigger on dir switch")
		}
	}
}

// TestPrefetchAfter_EmptyEntries 空目录 entries 返回空 slice。
func TestPrefetchAfter_EmptyEntries(t *testing.T) {
	out := PrefetchAfter("dir", "dir/a", nil, 0)
	if len(out) != 0 {
		t.Errorf("expected empty, got %v", out)
	}
}

// TestPrefetchAfter_SkipsLargeAndDir 大文件和目录不在预取列表中。
func TestPrefetchAfter_SkipsLargeAndDir(t *testing.T) {
	entries := []api.DirEntry{
		{Name: "big.bin", IsDir: false, Meta: &api.MetaEntry{Size: smallFileThreshold + 1}},
		{Name: "subdir", IsDir: true, Meta: &api.MetaEntry{IsDir: true}},
		{Name: "small.txt", IsDir: false, Meta: &api.MetaEntry{Size: 100}},
		{Name: "opened.txt", IsDir: false, Meta: &api.MetaEntry{Size: 100}},
	}
	out := PrefetchAfter("d", "d/opened.txt", entries, 10)
	if len(out) != 1 || out[0] != "d/small.txt" {
		t.Errorf("expected [d/small.txt], got %v", out)
	}
}

// TestPrefetchAfter_LimitRespected 超出 limit 截断。
func TestPrefetchAfter_LimitRespected(t *testing.T) {
	entries := make([]api.DirEntry, 20)
	for i := range entries {
		entries[i] = api.DirEntry{
			Name:  "f" + string(rune('a'+i)),
			IsDir: false,
			Meta:  &api.MetaEntry{Size: 100},
		}
	}
	out := PrefetchAfter("d", "d/NONE", entries, 3)
	if len(out) != 3 {
		t.Errorf("expected 3 results, got %d", len(out))
	}
}

// --- 辅助类型 ---

// containsDataCache 总是返回指定的 Contains 值。
type containsDataCache struct {
	contains bool
}

func (c *containsDataCache) Get(key, etag string) (string, bool, error) { return "", false, nil }
func (c *containsDataCache) Put(key, etag string, size int64, r io.Reader) (string, error) {
	_, _ = io.Copy(io.Discard, r)
	return "", nil
}
func (c *containsDataCache) Remove(key string) error        { return nil }
func (c *containsDataCache) Contains(key, etag string) bool { return c.contains }
func (c *containsDataCache) Stats() api.CacheStats          { return api.CacheStats{} }

// alwaysFailStore 所有操作都失败的 ObjectStore（测试错误路径）。
type alwaysFailStore struct{}

func (s *alwaysFailStore) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	return nil, io.ErrUnexpectedEOF
}
func (s *alwaysFailStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return nil, io.ErrUnexpectedEOF
}
func (s *alwaysFailStore) Put(ctx context.Context, key string, r io.Reader, size int64) (*api.ObjectMeta, error) {
	return nil, io.ErrUnexpectedEOF
}
func (s *alwaysFailStore) List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]api.ObjectEntry, []string, error) {
	return nil, nil, io.ErrUnexpectedEOF
}
func (s *alwaysFailStore) Copy(ctx context.Context, srcKey, dstKey string) error {
	return io.ErrUnexpectedEOF
}
func (s *alwaysFailStore) Delete(ctx context.Context, key string) error {
	return io.ErrUnexpectedEOF
}
func (s *alwaysFailStore) Bucket() string { return "fail" }

// countingStoreForEdge 用于避免和已有 countingStore 冲突
type countingStoreForEdge struct {
	api.ObjectStore
	headCalls atomic.Int64
}

func (c *countingStoreForEdge) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	c.headCalls.Add(1)
	return c.ObjectStore.Head(ctx, key)
}
