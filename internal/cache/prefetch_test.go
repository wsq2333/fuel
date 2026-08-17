package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// asConcurrentPutter 通过接口断言取出 ConcurrentPutter 能力，验证 INV-7 风格的接口隔离。
func asConcurrentPutter(t *testing.T, c api.DataCache) api.ConcurrentPutter {
	t.Helper()
	cp, ok := c.(api.ConcurrentPutter)
	if !ok {
		t.Fatalf("DataCache %T should implement api.ConcurrentPutter", c)
	}
	return cp
}

// deterministicContent 生成确定性的 size 字节数据（不同位置不同值，便于校验）。
func deterministicContent(size int64) []byte {
	const pattern = "fuel-concurrent-1234567890-"
	buf := make([]byte, size)
	for i := int64(0); i < size; {
		end := i + int64(len(pattern))
		if end > size {
			end = size
		}
		copy(buf[i:end], pattern)
		i = end
	}
	return buf
}

// TestPutConcurrent_BasicCorrectness 验证大文件多 block 并发拉取后字节级一致。
func TestPutConcurrent_BasicCorrectness(t *testing.T) {
	store := objectstore.NewMockStore("test")
	ctx := context.Background()

	size := int64(50 << 20) // 50MB
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "big.bin", bytes.NewReader(data), size)

	c := newTestCache(t, size+1<<20)
	cp := asConcurrentPutter(t, c)

	path, err := cp.PutConcurrent(ctx, "big.bin", "etag-big", size, store, 4, 4<<20)
	if err != nil {
		t.Fatalf("PutConcurrent failed: %v", err)
	}

	if got, _ := os.ReadFile(path); !bytes.Equal(got, data) {
		t.Errorf("content mismatch: got %d bytes, want %d", len(got), len(data))
	}

	if !c.Contains("big.bin", "etag-big") {
		t.Error("Contains should be true after PutConcurrent")
	}
	if gotPath, hit, _ := c.Get("big.bin", "etag-big"); !hit || gotPath != path {
		t.Errorf("Get hit=%v path=%q want %q", hit, gotPath, path)
	}
}

// TestPutConcurrent_SingleBlock 当 size <= blockSize 时只有 1 个 block，仍正确落盘。
func TestPutConcurrent_SingleBlock(t *testing.T) {
	store := objectstore.NewMockStore("test")
	ctx := context.Background()

	size := int64(1 << 20) // 1MB
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "small.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 4<<20)
	cp := asConcurrentPutter(t, c)

	path, err := cp.PutConcurrent(ctx, "small.bin", "e", size, store, 4, 4<<20)
	if err != nil {
		t.Fatalf("PutConcurrent single block failed: %v", err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, data) {
		t.Errorf("content mismatch")
	}
}

// TestPutConcurrent_CustomBlockSize 验证自定义 blockSize 产生正确的分块数与内容。
func TestPutConcurrent_CustomBlockSize(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	ctx := context.Background()

	size := int64(3 << 20) // 3MB
	blockSize := int64(1 << 20)
	wantBlocks := int64(3)
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "custom.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 16<<20)
	cp := asConcurrentPutter(t, c)

	path, err := cp.PutConcurrent(ctx, "custom.bin", "e", size, store, 2, blockSize)
	if err != nil {
		t.Fatalf("PutConcurrent failed: %v", err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, data) {
		t.Errorf("content mismatch")
	}
	if got := store.getCalls.Load(); got != wantBlocks {
		t.Errorf("GET calls = %d, want %d (one per block)", got, wantBlocks)
	}
}

// TestPutConcurrent_ConcurrencyCappedToBlocks 当 concurrency > blocks 时被夹紧。
func TestPutConcurrent_ConcurrencyCappedToBlocks(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	ctx := context.Background()

	size := int64(2 << 20)
	blockSize := int64(1 << 20)
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "cap.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 16<<20)
	cp := asConcurrentPutter(t, c)

	if _, err := cp.PutConcurrent(ctx, "cap.bin", "e", size, store, 16, blockSize); err != nil {
		t.Fatalf("PutConcurrent failed: %v", err)
	}
	if got := store.getCalls.Load(); got != 2 {
		t.Errorf("GET calls = %d, want 2", got)
	}
}

// TestPutConcurrent_DefaultParams concurrency/blockSize <= 0 时取默认值（4 / 4MB）。
func TestPutConcurrent_DefaultParams(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	ctx := context.Background()

	size := int64(20 << 20) // 20MB → 5 blocks @ 4MB
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "def.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 32<<20)
	cp := asConcurrentPutter(t, c)

	path, err := cp.PutConcurrent(ctx, "def.bin", "e", size, store, 0, 0)
	if err != nil {
		t.Fatalf("PutConcurrent default params failed: %v", err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, data) {
		t.Errorf("content mismatch")
	}
	if got := store.getCalls.Load(); got != 5 {
		t.Errorf("GET calls = %d, want 5", got)
	}
}

// TestPutConcurrent_OverMaxFileSize 超过 maxFileSize 应拒绝缓存。
func TestPutConcurrent_OverMaxFileSize(t *testing.T) {
	store := objectstore.NewMockStore("test")
	ctx := context.Background()

	size := int64(100)
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "big.bin", bytes.NewReader(data), size)

	c, err := NewNVMeCache(t.TempDir(), "b", 1<<20, 0.85, 0.70, 10)
	if err != nil {
		t.Fatalf("NewNVMeCache: %v", err)
	}
	cp := asConcurrentPutter(t, c)

	if _, err := cp.PutConcurrent(ctx, "big.bin", "e", size, store, 4, 4<<20); err == nil {
		t.Error("expected error for oversized file")
	}
	if c.Contains("big.bin", "e") {
		t.Error("oversized file should not be cached")
	}
}

// TestPutConcurrent_InvalidKey 非法 key 应返回错误并不写入。
func TestPutConcurrent_InvalidKey(t *testing.T) {
	store := objectstore.NewMockStore("test")
	c := newTestCache(t, 1<<20)
	cp := asConcurrentPutter(t, c)

	ctx := context.Background()
	bad := []string{"", "../escape", "/abs/path", "a/../../b"}
	for _, k := range bad {
		if _, err := cp.PutConcurrent(ctx, k, "e", 100, store, 4, 4<<20); err == nil {
			t.Errorf("PutConcurrent(%q) should reject invalid key", k)
		}
	}
}

// TestPutConcurrent_GetFailureAborts 任意 block GET 失败 → 整体失败、临时文件清理、索引未污染。
func TestPutConcurrent_GetFailureAborts(t *testing.T) {
	store := &failingStore{
		ObjectStore: objectstore.NewMockStore("test"),
		failFrom:   2, // 第 3 次 GET（block index 2）起失败
	}
	ctx := context.Background()

	size := int64(20 << 20) // 5 blocks @ 4MB
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "fail.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 32<<20)
	cp := asConcurrentPutter(t, c)

	if _, err := cp.PutConcurrent(ctx, "fail.bin", "e", size, store, 4, 4<<20); err == nil {
		t.Fatal("expected error when a block GET fails")
	}

	if c.Contains("fail.bin", "e") {
		t.Error("index should not contain key after failure")
	}

	// 缓存目录不应残留 .fuel-* 临时文件
	if leaks := countOrphanTemps(t, c); leaks != 0 {
		t.Errorf("expected no orphan tmp files after failure, got %d", leaks)
	}
}

// TestPutConcurrent_ShortReadAborts 短读（reader 返回字节数 < length）应被识别为错误。
func TestPutConcurrent_ShortReadAborts(t *testing.T) {
	store := &shortReadStore{
		ObjectStore: objectstore.NewMockStore("test"),
		// 每次返回实际字节数 - 1，触发 short block 校验
	}
	ctx := context.Background()

	size := int64(8 << 20) // 2 blocks @ 4MB
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "short.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 16<<20)
	cp := asConcurrentPutter(t, c)

	if _, err := cp.PutConcurrent(ctx, "short.bin", "e", size, store, 2, 4<<20); err == nil {
		t.Fatal("expected error on short read")
	}
	if c.Contains("short.bin", "e") {
		t.Error("index should not contain key after short read")
	}
}

// TestPutConcurrent_KeyMissing 在对象存储中不存在该 key 时整体失败。
func TestPutConcurrent_KeyMissing(t *testing.T) {
	store := objectstore.NewMockStore("test")
	c := newTestCache(t, 16<<20)
	cp := asConcurrentPutter(t, c)

	if _, err := cp.PutConcurrent(context.Background(), "ghost", "e", 4<<20, store, 2, 4<<20); err == nil {
		t.Fatal("expected error for missing object")
	}
}

// TestPutConcurrent_ConcurrentCalls 多 goroutine 同时调用 PutConcurrent 写同 key
// 验证不产生 data race（受 errgroup + 文件 rename 的原子性保护）。
func TestPutConcurrent_ConcurrentCalls(t *testing.T) {
	store := &countingStore{ObjectStore: objectstore.NewMockStore("test")}
	ctx := context.Background()

	size := int64(4 << 20) // 1 block
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "race.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 64<<20)
	cp := asConcurrentPutter(t, c)

	const callers = 8
	errs := make([]error, callers)
	done := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			_, errs[i] = cp.PutConcurrent(ctx, "race.bin", "e", size, store, 4, 1<<20)
		}(i)
	}
	for i := 0; i < callers; i++ {
		<-done
	}

	ok := 0
	for _, e := range errs {
		if e == nil {
			ok++
		}
	}
	if ok == 0 {
		t.Fatalf("at least one PutConcurrent should succeed, errs=%v", errs)
	}
	if !c.Contains("race.bin", "e") {
		t.Error("cache should contain key after concurrent calls")
	}
	if got, _ := os.ReadFile(filepath.Join(c.(*nvmeCache).dir, c.(*nvmeCache).bucket, "race.bin")); !bytes.Equal(got, data) {
		t.Errorf("content mismatch after concurrent calls")
	}
}

// TestPutConcurrent_ContextCancel ctx 取消时已启动的 block 应中止并返回错误。
func TestPutConcurrent_ContextCancel(t *testing.T) {
	store := &slowStore{
		ObjectStore: objectstore.NewMockStore("test"),
		delay:       100 * time.Millisecond,
	}
	ctx := context.Background()

	size := int64(40 << 20) // 10 blocks @ 4MB
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "cancel.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 64<<20)
	cp := asConcurrentPutter(t, c)

	cctx, cancel := context.WithCancel(ctx)
	// 启动一个 goroutine 等待 block 拉取开始后取消
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	if _, err := cp.PutConcurrent(cctx, "cancel.bin", "e", size, store, 4, 4<<20); err == nil {
		t.Fatal("expected error on context cancel")
	}
	if c.Contains("cancel.bin", "e") {
		t.Error("index should not contain key after cancel")
	}
	if leaks := countOrphanTemps(t, c); leaks != 0 {
		t.Errorf("expected no orphan tmp files after cancel, got %d", leaks)
	}
}

// TestPutConcurrent_EtagMismatchOnGet PutConcurrent 后用错 etag 调 Get 应 miss。
// 注意：nvmeCache.Get 在 ETag 不匹配时会剔除缓存条目（INV-9），所以错 etag 的 Get
// 之后再 Contains 正确 etag 必然为 false —— 这里只验证 mismatch 时返回 miss。
func TestPutConcurrent_EtagMismatchOnGet(t *testing.T) {
	store := objectstore.NewMockStore("test")
	ctx := context.Background()

	size := int64(8 << 20)
	data := deterministicContent(size)
	_, _ = store.Put(ctx, "etag.bin", bytes.NewReader(data), size)

	c := newTestCache(t, 16<<20)
	cp := asConcurrentPutter(t, c)

	path, err := cp.PutConcurrent(ctx, "etag.bin", "e1", size, store, 2, 4<<20)
	if err != nil {
		t.Fatalf("PutConcurrent failed: %v", err)
	}
	if _, hit, _ := c.Get("etag.bin", "e2"); hit {
		t.Error("Get with mismatched etag should miss")
	}
	// 文件仍应在磁盘上（Get 在 mismatch 时会 os.Remove —— 验证已被剔除）
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("cache file should be removed on etag mismatch, got err=%v", err)
	}
}

// failingStore 在第 failFrom 次 GET 起返回错误，用于注入失败。
type failingStore struct {
	api.ObjectStore
	getCalls  atomic.Int64
	failFrom  int64
}

func (f *failingStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	n := f.getCalls.Add(1)
	if n >= f.failFrom {
		return nil, fmt.Errorf("injected GET failure (call %d)", n)
	}
	return f.ObjectStore.Get(ctx, key, offset, length)
}

// shortReadStore 每次返回比请求少 1 字节，触发 short block 校验。
type shortReadStore struct {
	api.ObjectStore
}

func (s *shortReadStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if length > 1 {
		length--
	}
	return s.ObjectStore.Get(ctx, key, offset, length)
}

// slowStore 在 Get 返回前 sleep delay，用于测试 context 取消。
type slowStore struct {
	api.ObjectStore
	delay time.Duration
}

func (s *slowStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.ObjectStore.Get(ctx, key, offset, length)
}

// countOrphanTemps 统计缓存目录中残留的 .fuel-* 临时文件。
func countOrphanTemps(t *testing.T, c api.DataCache) int {
	t.Helper()
	nc, ok := c.(*nvmeCache)
	if !ok {
		t.Fatalf("expected *nvmeCache, got %T", c)
	}
	root := filepath.Join(nc.dir, nc.bucket)
	count := 0
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), tmpFilePrefix) {
			count++
		}
		return nil
	})
	return count
}

// --- 4.3 BatchPrefetcher 测试 ---

// TestBatchPrefetcher_TriggerOnThreshold 同目录连续 3 次小文件 Open 触发批量预取。
func TestBatchPrefetcher_TriggerOnThreshold(t *testing.T) {
	bp := NewBatchPrefetcher(true)
	for i := 0; i < batchPrefetchTriggerThreshold-1; i++ {
		if bp.OnOpen("d", 100) {
			t.Fatalf("OnOpen #%d should not trigger yet", i)
		}
	}
	if !bp.OnOpen("d", 100) {
		t.Error("OnOpen at threshold should trigger")
	}
}

// TestBatchPrefetcher_OncePerDir 同目录仅触发一次，后续 Open 不再触发。
func TestBatchPrefetcher_OncePerDir(t *testing.T) {
	bp := NewBatchPrefetcher(true)
	for i := 0; i < batchPrefetchTriggerThreshold; i++ {
		bp.OnOpen("d", 100)
	}
	for i := 0; i < 3; i++ {
		if bp.OnOpen("d", 100) {
			t.Error("should not trigger again for same dir")
		}
	}
}

// TestBatchPrefetcher_DirChangeResetsConsec 切换目录重置连续计数。
func TestBatchPrefetcher_DirChangeResetsConsec(t *testing.T) {
	bp := NewBatchPrefetcher(true)
	bp.OnOpen("a", 100)
	bp.OnOpen("a", 100)
	// 切到 b，计数从 1 开始
	if bp.OnOpen("b", 100) {
		t.Error("dir switch should reset consec, no trigger")
	}
	// 回 a，consec 也从 1 开始
	if bp.OnOpen("a", 100) {
		t.Error("dir switch back should reset consec, no trigger")
	}
}

// TestBatchPrefetcher_LargeFileSkipped 大文件不计入连续计数也不触发。
func TestBatchPrefetcher_LargeFileSkipped(t *testing.T) {
	bp := NewBatchPrefetcher(true)
	large := int64(smallFileThreshold + 1)
	for i := 0; i < 5; i++ {
		if bp.OnOpen("d", large) {
			t.Errorf("large file should not trigger (size=%d)", large)
		}
	}
	// 大文件穿插也不污染小文件计数
	bp.OnOpen("d", 100)
	if bp.OnOpen("d", large) {
		t.Error("large file interleave should not trigger")
	}
	if bp.OnOpen("d", 100) {
		t.Error("large file should not advance consec counter")
	}
}

// TestBatchPrefetcher_Disabled 禁用时任何 Open 不触发。
func TestBatchPrefetcher_Disabled(t *testing.T) {
	bp := NewBatchPrefetcher(false)
	for i := 0; i < 10; i++ {
		if bp.OnOpen("d", 100) {
			t.Error("disabled prefetcher should never trigger")
		}
	}
}

// TestBatchPrefetcher_Reset Reset 清除触发记录，同目录可再次触发。
func TestBatchPrefetcher_Reset(t *testing.T) {
	bp := NewBatchPrefetcher(true)
	for i := 0; i < batchPrefetchTriggerThreshold; i++ {
		bp.OnOpen("d", 100)
	}
	bp.Reset("d")
	// 重新计数到阈值
	for i := 0; i < batchPrefetchTriggerThreshold-1; i++ {
		if bp.OnOpen("d", 100) {
			t.Fatal("after Reset, count restarts, no trigger")
		}
	}
	if !bp.OnOpen("d", 100) {
		t.Error("after Reset and reaching threshold again, should trigger")
	}
}

// TestPrefetchAfter_Filters 过滤已打开/目录项/大文件，仅返回小文件。
func TestPrefetchAfter_Filters(t *testing.T) {
	entries := []api.DirEntry{
		{Name: "a", Meta: &api.MetaEntry{Size: 100}},
		{Name: "b", Meta: &api.MetaEntry{Size: 200}},
		{Name: "big", Meta: &api.MetaEntry{Size: smallFileThreshold + 1}},
		{Name: "sub", IsDir: true, Meta: &api.MetaEntry{}},
		{Name: "c", Meta: nil},
		{Name: "opened", Meta: &api.MetaEntry{Size: 50}},
		{Name: "d", Meta: &api.MetaEntry{Size: 300}},
	}
	got := PrefetchAfter("", "opened", entries, 0)
	want := []string{"a", "b", "d"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("PrefetchAfter got %v, want %v", got, want)
	}
}

// TestPrefetchAfter_PathJoin 非空 dirPath 正确拼接 key。
func TestPrefetchAfter_PathJoin(t *testing.T) {
	entries := []api.DirEntry{
		{Name: "x", Meta: &api.MetaEntry{Size: 10}},
	}
	got := PrefetchAfter("d1/d2", "d1/d2/other", entries, 0)
	if len(got) != 1 || got[0] != "d1/d2/x" {
		t.Errorf("expected [d1/d2/x], got %v", got)
	}
}

// TestPrefetchAfter_Limit 限制返回数量。
func TestPrefetchAfter_Limit(t *testing.T) {
	entries := make([]api.DirEntry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, api.DirEntry{
			Name: fmt.Sprintf("f%d", i),
			Meta: &api.MetaEntry{Size: 10},
		})
	}
	got := PrefetchAfter("", "other", entries, 3)
	if len(got) != 3 {
		t.Errorf("expected 3 entries, got %v", got)
	}
}
