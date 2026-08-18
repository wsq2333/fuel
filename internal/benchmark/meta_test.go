package benchmark

import (
	"fmt"
	"testing"
	"time"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/config"
)

// 5.2 元数据 benchmark (PLAN §5.2)。

// --- 场景 6: 元数据操作 (stat 10K files) ---

// BenchmarkScenario6_MetaStat 10K 次 L1 stat 缓存命中延迟。
// 仅测 L1 meta cache（纯内存），对应 GOAL-2 元数据 stat P50 < 5ms 验证。
func BenchmarkScenario6_MetaStat(b *testing.B) {
	metaCfg := config.MetaCacheConfig{
		StatTTL: 30 * time.Second,
		DirTTL:  10 * time.Second,
		NegTTL:  60 * time.Second,
	}
	metaCache := cache.NewMetaCache(metaCfg)

	keys := make([]string, smallFileCount)
	for i := 0; i < smallFileCount; i++ {
		k := fmt.Sprintf("small/%05d", i)
		keys[i] = k
		metaCache.SetStat(k, &api.MetaEntry{
			Path:  k,
			Inode: api.InodeFromPath(k),
			Size:  smallFileSize,
			ETag:  fmt.Sprintf("etag-%05d", i),
			Mode:  0o644,
		})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[i%smallFileCount]
		if _, ok := metaCache.GetStat(key); !ok {
			b.Fatal("expected L1 cache hit")
		}
	}
}

// BenchmarkScenario6_MetaStatMiss L1 未命中时写入再读的开销（TTL miss → 回源前的探测）。
func BenchmarkScenario6_MetaStatMiss(b *testing.B) {
	metaCfg := config.MetaCacheConfig{
		StatTTL: 30 * time.Second,
		DirTTL:  10 * time.Second,
		NegTTL:  60 * time.Second,
	}
	metaCache := cache.NewMetaCache(metaCfg)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("miss/%d", i%smallFileCount)
		metaCache.GetStat(key)
	}
}

// BenchmarkScenario6_BatchGetAttr 批量 stat（模拟 readdir 后并发预取路径）。
func BenchmarkScenario6_BatchGetAttr(b *testing.B) {
	metaCfg := config.MetaCacheConfig{
		StatTTL: 30 * time.Second,
		DirTTL:  10 * time.Second,
		NegTTL:  60 * time.Second,
	}
	metaCache := cache.NewMetaCache(metaCfg)

	paths := make([]string, 100)
	for i := range paths {
		p := fmt.Sprintf("batch/%03d", i)
		paths[i] = p
		metaCache.SetStat(p, &api.MetaEntry{
			Path:  p,
			Inode: api.InodeFromPath(p),
			Size:  1024,
			ETag:  "e",
			Mode:  0o644,
		})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, p := range paths {
			metaCache.GetStat(p)
		}
	}
}

// --- 场景 7: 目录列表 (readdir 1K files) ---

// BenchmarkScenario7_Readdir 1K 文件目录列表 L1 缓存命中延迟。
// 对应 PLAN §4.3 "readdir 后目录下文件 stat 延迟 < 1ms" 验证。
func BenchmarkScenario7_Readdir(b *testing.B) {
	metaCfg := config.MetaCacheConfig{
		StatTTL: 30 * time.Second,
		DirTTL:  10 * time.Second,
		NegTTL:  60 * time.Second,
	}
	metaCache := cache.NewMetaCache(metaCfg)

	dirEntryCount := 1000
	entries := make([]api.DirEntry, dirEntryCount)
	for i := 0; i < dirEntryCount; i++ {
		entries[i] = api.DirEntry{
			Name:  fmt.Sprintf("file-%04d", i),
			IsDir: false,
			Meta: &api.MetaEntry{
				Path:  fmt.Sprintf("dir/file-%04d", i),
				Inode: api.InodeFromPath(fmt.Sprintf("dir/file-%04d", i)),
				Size:  int64(i * 100),
				ETag:  fmt.Sprintf("e-%04d", i),
				Mode:  0o644,
			},
		}
	}
	metaCache.SetDir("dir", entries)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := metaCache.GetDir("dir"); !ok {
			b.Fatal("expected L1 dir cache hit")
		}
	}
}

// BenchmarkScenario7_ReaddirAndPrefetch 模拟 readdir + 后续 stat 的组合开销。
// 测 1 次 GetDir + N 次 GetStat 命中 L1 的总耗时。
func BenchmarkScenario7_ReaddirAndPrefetch(b *testing.B) {
	metaCfg := config.MetaCacheConfig{
		StatTTL: 30 * time.Second,
		DirTTL:  10 * time.Second,
		NegTTL:  60 * time.Second,
	}
	metaCache := cache.NewMetaCache(metaCfg)

	dirEntryCount := 1000
	entries := make([]api.DirEntry, dirEntryCount)
	for i := 0; i < dirEntryCount; i++ {
		p := fmt.Sprintf("dir/file-%04d", i)
		entries[i] = api.DirEntry{
			Name:  fmt.Sprintf("file-%04d", i),
			IsDir: false,
			Meta: &api.MetaEntry{
				Path:  p,
				Inode: api.InodeFromPath(p),
				Size:  int64(i * 100),
				ETag:  fmt.Sprintf("e-%04d", i),
				Mode:  0o644,
			},
		}
		metaCache.SetStat(p, entries[i].Meta)
	}
	metaCache.SetDir("dir", entries)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ents, ok := metaCache.GetDir("dir")
		if !ok {
			b.Fatal("expected L1 dir cache hit")
		}
		for _, e := range ents {
			metaCache.GetStat(e.Meta.Path)
		}
	}
}

// BenchmarkMetaCacheInvalidate 级联失效操作开销（readdir 后目录删除场景）。
func BenchmarkMetaCacheInvalidate(b *testing.B) {
	metaCfg := config.MetaCacheConfig{
		StatTTL: 30 * time.Second,
		DirTTL:  10 * time.Second,
		NegTTL:  60 * time.Second,
	}
	metaCache := cache.NewMetaCache(metaCfg)

	for i := 0; i < 100; i++ {
		p := fmt.Sprintf("deep/nested/dir/f%d", i)
		metaCache.SetStat(p, &api.MetaEntry{Path: p, Size: 1, Mode: 0o644})
	}
	metaCache.SetDir("deep/nested/dir", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		metaCache.InvalidatePrefix("deep/nested")
	}
}