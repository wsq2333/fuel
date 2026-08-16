package cache

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"fuel/api"
)

// newLRUCache 构造容量 1000 字节、高水位 0.85(850)、低水位 0.70(700) 的缓存。
func newLRUCache(t *testing.T) api.DataCache {
	t.Helper()
	c, err := NewNVMeCache(t.TempDir(), "b", 1000, 0.85, 0.70, 0)
	if err != nil {
		t.Fatalf("NewNVMeCache: %v", err)
	}
	return c
}

func putN(t *testing.T, c api.DataCache, key string, size int) {
	t.Helper()
	if _, err := c.Put(key, "etag-"+key, int64(size), bytes.NewReader(make([]byte, size))); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

// TestEviction_Triggers 验证写入超过高水位时触发淘汰。
func TestEviction_Triggers(t *testing.T) {
	c := newLRUCache(t)

	// 写 4 个 200 字节文件 = 800 (< 850 高水位)
	for i := 0; i < 4; i++ {
		putN(t, c, fmt.Sprintf("f%d", i), 200)
	}
	if got := c.Stats().UsedBytes; got != 800 {
		t.Fatalf("expected used 800, got %d", got)
	}

	// 再写 200 → 1000 > 850，触发淘汰到 <= 700-incoming
	putN(t, c, "f4", 200)

	s := c.Stats()
	if s.UsedBytes > 700 {
		t.Errorf("after eviction used should <= 700, got %d", s.UsedBytes)
	}
	if s.EvictionCount == 0 {
		t.Error("expected some evictions")
	}
	// 新写入的必须保留
	if !c.Contains("f4", "etag-f4") {
		t.Error("newly written f4 must be kept")
	}
}

// TestEviction_LRUOrder 验证淘汰顺序是最久未访问优先。
func TestEviction_LRUOrder(t *testing.T) {
	c := newLRUCache(t)

	putN(t, c, "old1", 200)
	putN(t, c, "old2", 200)
	putN(t, c, "old3", 200)

	// 访问 old1 使其成为最近使用
	if _, hit, _ := c.Get("old1", "etag-old1"); !hit {
		t.Fatal("old1 should hit")
	}

	// 写入触发淘汰：used 600+400(new)=1000 > 850 → 淘汰到 <=700-400=300
	// 需淘汰 old2, old3（old1 刚访问过，保留）
	putN(t, c, "new", 400)

	if c.Contains("old1", "etag-old1") {
		// old1 是最近访问，应被保留
	} else {
		t.Error("old1 (recently accessed) should be kept")
	}
	if c.Contains("old2", "etag-old2") {
		t.Error("old2 (LRU) should be evicted")
	}
	if c.Contains("old3", "etag-old3") {
		t.Error("old3 (LRU) should be evicted")
	}
	if !c.Contains("new", "etag-new") {
		t.Error("new entry must be kept")
	}
}

// TestEviction_RemovesFiles 验证淘汰时磁盘文件被删除。
func TestEviction_RemovesFiles(t *testing.T) {
	c := newLRUCache(t)

	p1, _ := c.Put("victim", "e", 600, bytes.NewReader(make([]byte, 600)))
	putN(t, c, "other", 200)
	// used=800, 再写 400 → 1200>850 → 淘汰到 <=700-400=300，victim(600) 被淘汰
	putN(t, c, "big", 400)

	if _, err := os.Stat(p1); !os.IsNotExist(err) {
		t.Error("evicted file should be removed from disk")
	}
}

// TestEviction_ManySmallFiles 验证大量小文件场景淘汰计数正确。
func TestEviction_ManySmallFiles(t *testing.T) {
	c := newLRUCache(t)

	// 每个 100 字节，低水位 700，高水位 850
	// 写到第 9 个时 900>850 触发淘汰
	for i := 0; i < 20; i++ {
		putN(t, c, fmt.Sprintf("k%02d", i), 100)
	}
	s := c.Stats()
	if s.UsedBytes > 850 {
		t.Errorf("used %d exceeds high watermark 850", s.UsedBytes)
	}
	if s.EvictionCount == 0 {
		t.Error("expected evictions with many small files")
	}
	if s.EntryCount > 8 {
		t.Errorf("entry count %d too high, expected <=8 after eviction", s.EntryCount)
	}
	// used + 统计一致
	if s.UsedBytes != s.EntryCount*100 {
		t.Errorf("used %d != entries*100 (%d)", s.UsedBytes, s.EntryCount*100)
	}
}

// TestEviction_EvictForBelowLowWatermark 验证淘汰后降到低水位附近。
func TestEviction_EvictForBelowLowWatermark(t *testing.T) {
	c := newLRUCache(t)
	for i := 0; i < 4; i++ {
		putN(t, c, fmt.Sprintf("f%d", i), 200) // 800
	}
	// 写 300 → 1100 > 850 → evictFor(300): target = 700-300=400 → 淘汰到 used<=400
	// 淘汰 f0,f1,f2 (600) → used=200 (f3)，再写 300 → 500
	putN(t, c, "new", 300)
	s := c.Stats()
	// used = 剩余未被淘汰的 + new(300)
	if s.UsedBytes > 700 {
		t.Errorf("used %d should be <= low watermark 700 after eviction+write", s.UsedBytes)
	}
}
