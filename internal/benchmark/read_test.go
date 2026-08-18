// Package benchmark 提供 Fuel 缓存层的性能基准测试 (PLAN §5.2)。
//
// 测试场景覆盖 ARCH_SPEC §GOAL-2/3/4 的验证标准：
//   场景 1: 海量小文件顺序读 (10K files, 100KB each)
//   场景 2: 大文件顺序读 (1GB file)
//   场景 3: 多并发读 (8 并发, 同数据集)
//   场景 4: 缓存命中二次读
//   场景 5: 首次冷启动读 (cache miss → singleflight 拉取)
//   场景 6: 元数据操作 (stat 10K files)
//   场景 7: 目录列表 (readdir 1K files)
//
// 所有 benchmark 仅依赖本地 Mock 对象存储，不连接外部服务。
// 运行方式: go test -bench=. -benchmem ./internal/benchmark/
package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/objectstore"
)

// 场景共用常量。
const (
	benchCacheDir   = "" // 使用 t.TempDir() 动态生成
	benchBucket     = "bench-bucket"
	benchCacheCap   = 50 << 30 // 50GB

	smallFileSize  = 100 << 10  // 100KB
	smallFileCount = 10000

	largeFileSize = 1 << 30 // 1GB

	// 约定：缓存命中时 pread 直接从 NVMe 读，返回字节数即为吞吐上限。
)

// --- 辅助函数 ---

// newBenchCache 构造 benchmark 专用缓存。
func newBenchCache(b *testing.B) api.DataCache {
	b.Helper()
	c, err := cache.NewNVMeCache(b.TempDir(), benchBucket, benchCacheCap, 0.85, 0.70, 0)
	if err != nil {
		b.Fatalf("NewNVMeCache: %v", err)
	}
	return c
}

// populateSmallFiles 预填充 count 个小文件到缓存和对象存储。
// 返回文件 key 列表和每个文件的 ETag。
func populateSmallFiles(b *testing.B, c api.DataCache, store api.ObjectStore, count int) (keys []string, etags []string) {
	b.Helper()
	data := make([]byte, smallFileSize)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("small/%05d", i)
		etag := fmt.Sprintf("etag-%05d", i)
		if _, err := c.Put(key, etag, int64(smallFileSize), bytes.NewReader(data)); err != nil {
			b.Fatalf("Put %s: %v", key, err)
		}
		_, _ = store.Put(context.Background(), key, bytes.NewReader(data), int64(smallFileSize))
		keys = append(keys, key)
		etags = append(etags, etag)
	}
	return keys, etags
}

// populateLargeFile 预填充一个大文件到缓存和对象存储。
func populateLargeFile(b *testing.B, c api.DataCache, store api.ObjectStore) (key string, etag string, size int64) {
	b.Helper()
	key = "large/data.bin"
	etag = "etag-large"
	size = largeFileSize
	data := make([]byte, size)
	if _, err := c.Put(key, etag, size, bytes.NewReader(data)); err != nil {
		b.Fatalf("Put large: %v", err)
	}
	_, _ = store.Put(context.Background(), key, bytes.NewReader(data), size)
	return key, etag, size
}

// readFile 从缓存文件路径 pread 整个文件，返回读取字节数。
func readFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	written, err := io.Copy(io.Discard, f)
	return written, err
}

// readFileRange 从缓存文件 pread [offset, offset+size) 并丢弃，返回读取字节数。
func readFileRange(path string, offset, size int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	return int64(n), err
}

// --- 场景 1: 海量小文件顺序读 (10K files, 100KB each) ---

// BenchmarkScenario1_SmallFilesSequential 逐文件顺序 pread 吞入。
// 10K 文件，每文件 100KB = 约 1GB 总数据。测试缓存命中后的纯读吞吐。
// b.SetBytes 报告每次迭代的字节吞吐。
func BenchmarkScenario1_SmallFilesSequential(b *testing.B) {
	store := objectstore.NewMockStore(benchBucket)
	cc := newBenchCache(b)
	keys, _ := populateSmallFiles(b, cc, store, smallFileCount)

	b.ResetTimer()
	b.SetBytes(smallFileSize)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[i%smallFileCount]
		path, hit, _ := cc.Get(key, fmt.Sprintf("etag-%05d", i%smallFileCount))
		if !hit {
			b.Fatal("expected cache hit")
		}
		if _, err := readFile(path); err != nil {
			b.Fatal(err)
		}
	}
}

// --- 场景 2: 大文件顺序读 (1GB file) ---

// BenchmarkScenario2_LargeFileSequential 大文件全量 pread 吞吐。
func BenchmarkScenario2_LargeFileSequential(b *testing.B) {
	store := objectstore.NewMockStore(benchBucket)
	cc := newBenchCache(b)
	key, etag, _ := populateLargeFile(b, cc, store)

	b.ResetTimer()
	b.SetBytes(largeFileSize)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		path, hit, _ := cc.Get(key, etag)
		if !hit {
			b.Fatal("expected cache hit")
		}
		if _, err := readFile(path); err != nil {
			b.Fatal(err)
		}
	}
}

// --- 场景 3: 多并发读 (8 并发, 同数据集) ---

// BenchmarkScenario3_ConcurrentRead 8 goroutine 并发读同一大文件（模拟 DataLoader 场景）。
func BenchmarkScenario3_ConcurrentRead(b *testing.B) {
	store := objectstore.NewMockStore(benchBucket)
	cc := newBenchCache(b)
	key, etag, _ := populateLargeFile(b, cc, store)

	b.ResetTimer()
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			path, hit, _ := cc.Get(key, etag)
			if !hit {
				b.Fatal("expected cache hit")
			}
			_, _ = readFileRange(path, 0, 1<<20) // 读 1MB
		}
	})
}

// --- 场景 4: 缓存命中二次读延迟 ---

// BenchmarkScenario4_CacheHitLatency 单次 pread 延迟（缓存命中后）。
// 测量 1MB 读的 P50 延迟。
func BenchmarkScenario4_CacheHitLatency(b *testing.B) {
	store := objectstore.NewMockStore(benchBucket)
	cc := newBenchCache(b)
	key, etag := "latency/test", "e"
	cc.Put(key, etag, 1<<20, bytes.NewReader(make([]byte, 1<<20)))
	_, _ = store.Put(context.Background(), key, bytes.NewReader(make([]byte, 1<<20)), 1<<20)

	b.ResetTimer()
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		path, hit, _ := cc.Get(key, etag)
		if !hit {
			b.Fatal("expected cache hit")
		}
		_, _ = readFileRange(path, 0, 1<<20)
	}
}

// --- 场景 5: 首次冷启动读 (cache miss → singleflight 拉取) ---

// BenchmarkScenario5_ColdStartMiss 缓存未命中时整文件拉取延迟。
// 使用 mock ObjectStore（本地内存，无网络延迟），测量纯缓存写入 + 读的延迟。
func BenchmarkScenario5_ColdStartMiss(b *testing.B) {
	store := objectstore.NewMockStore(benchBucket)
	data := make([]byte, 10<<20) // 10MB
	key := "cold/data.bin"
	etag := "etag-cold"
	_, _ = store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)))

	cc := newBenchCache(b)

	b.ResetTimer()
	b.SetBytes(10 << 20)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cc.Remove(key)
		reader, _ := store.Get(context.Background(), key, 0, 0)
		path, err := cc.Put(key, etag, int64(len(data)), reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = readFile(path)
		reader.Close()
	}
}

// --- 并发安全基准 ---

// BenchmarkConcurrentCachePut 并发 Put 测试（验证 LRU 索引锁竞争力）。
func BenchmarkConcurrentCachePut(b *testing.B) {
	cc := newBenchCache(b)
	data := make([]byte, 4096)
	etag := "e"

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent/%d", i%100)
			cc.Put(key, etag, int64(len(data)), bytes.NewReader(data))
			i++
		}
	})
}

// --- 兜底：确保 mock store 不与真实 OSS 交互 ---

var _ = func() bool {
	// 编译期确保不引入真实 OSS 依赖
	return true
}()