package cache

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fuel/api"
)

// newTestCache 构造一个小容量测试缓存。
func newTestCache(t *testing.T, capacity int64) api.DataCache {
	t.Helper()
	c, err := NewNVMeCache(t.TempDir(), "test-bucket", capacity, 0.85, 0.70, 0)
	if err != nil {
		t.Fatalf("NewNVMeCache failed: %v", err)
	}
	return c
}

func TestNVMeCache_PutAndGet(t *testing.T) {
	c := newTestCache(t, 1<<20)

	path, err := c.Put("a/b.txt", "etag1", 5, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 路径映射 INV-2: {dir}/{bucket}/{key}
	if !strings.HasSuffix(filepath.ToSlash(path), "/test-bucket/a/b.txt") {
		t.Errorf("cache path should be {dir}/{bucket}/{key}, got %q", path)
	}

	// 字节镜像: 缓存文件可被外部直接读
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}

	// Get 命中
	gotPath, hit, err := c.Get("a/b.txt", "etag1")
	if err != nil || !hit {
		t.Fatalf("Get failed: err=%v hit=%v", err, hit)
	}
	if gotPath != path {
		t.Errorf("Get path %q != Put path %q", gotPath, path)
	}
}

func TestNVMeCache_PutCreatesSubdirs(t *testing.T) {
	c := newTestCache(t, 1<<20)
	path, err := c.Put("deep/nested/dir/file.txt", "e", 3, bytes.NewReader([]byte("abc")))
	if err != nil {
		t.Fatalf("Put nested failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("nested cache file not created: %v", err)
	}
}

func TestNVMeCache_ETagMismatch(t *testing.T) {
	c := newTestCache(t, 1<<20)
	path, _ := c.Put("f.txt", "etag-old", 5, bytes.NewReader([]byte("hello")))

	// ETag 不匹配 → miss，且缓存被删除
	_, hit, err := c.Get("f.txt", "etag-new")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if hit {
		t.Error("expected miss on etag mismatch")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("stale cache file should be removed on etag mismatch")
	}
	// 后续 Get 仍 miss（索引已清）
	if _, hit, _ := c.Get("f.txt", "etag-old"); hit {
		t.Error("expected miss after eviction by etag mismatch")
	}
}

func TestNVMeCache_GetMissNoFile(t *testing.T) {
	c := newTestCache(t, 1<<20)
	_, hit, err := c.Get("nonexistent", "e")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if hit {
		t.Error("expected miss for nonexistent key")
	}
}

func TestNVMeCache_GetFileDeletedExternally(t *testing.T) {
	c := newTestCache(t, 1<<20)
	path, _ := c.Put("f.txt", "e", 5, bytes.NewReader([]byte("hello")))
	// 外部删除缓存文件
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	// Get 应检测文件丢失 → miss + 清索引
	if _, hit, _ := c.Get("f.txt", "e"); hit {
		t.Error("expected miss when cache file deleted externally")
	}
	// 索引已清 → Contains false
	if c.Contains("f.txt", "e") {
		t.Error("Contains should be false after external deletion")
	}
}

func TestNVMeCache_Remove(t *testing.T) {
	c := newTestCache(t, 1<<20)
	path, _ := c.Put("f.txt", "e", 5, bytes.NewReader([]byte("hello")))
	if err := c.Remove("f.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("cache file should be removed")
	}
	if c.Contains("f.txt", "e") {
		t.Error("Contains should be false after Remove")
	}
	// Remove 不存在的 key 不报错
	if err := c.Remove("ghost"); err != nil {
		t.Errorf("Remove nonexistent should not error, got %v", err)
	}
}

func TestNVMeCache_Contains(t *testing.T) {
	c := newTestCache(t, 1<<20)
	c.Put("f.txt", "e1", 5, bytes.NewReader([]byte("hello")))

	if !c.Contains("f.txt", "e1") {
		t.Error("Contains should be true for matching etag")
	}
	if c.Contains("f.txt", "e2") {
		t.Error("Contains should be false for mismatched etag")
	}
	if c.Contains("ghost", "e1") {
		t.Error("Contains should be false for missing key")
	}
}

func TestNVMeCache_Stats(t *testing.T) {
	c := newTestCache(t, 1<<20)
	c.Put("a", "e", 5, bytes.NewReader([]byte("hello")))
	c.Put("b", "e", 5, bytes.NewReader([]byte("world")))

	c.Get("a", "e")   // hit
	c.Get("a", "e")   // hit
	c.Get("ghost", "e") // miss

	s := c.Stats()
	if s.HitCount != 2 {
		t.Errorf("expected 2 hits, got %d", s.HitCount)
	}
	if s.MissCount != 1 {
		t.Errorf("expected 1 miss, got %d", s.MissCount)
	}
	if s.UsedBytes != 10 {
		t.Errorf("expected used 10, got %d", s.UsedBytes)
	}
	if s.CapacityBytes != 1<<20 {
		t.Errorf("expected capacity %d, got %d", int64(1<<20), s.CapacityBytes)
	}
	if s.EntryCount != 2 {
		t.Errorf("expected 2 entries, got %d", s.EntryCount)
	}
}

func TestNVMeCache_MaxFileSize(t *testing.T) {
	c, err := NewNVMeCache(t.TempDir(), "b", 1<<20, 0.85, 0.70, 10)
	if err != nil {
		t.Fatalf("NewNVMeCache: %v", err)
	}
	// 超过 maxFileSize=10 → 不缓存，返回错误
	if _, err := c.Put("big", "e", 100, bytes.NewReader(make([]byte, 100))); err == nil {
		t.Error("expected error for oversized file")
	}
	if c.Contains("big", "e") {
		t.Error("oversized file should not be cached")
	}
}

func TestNVMeCache_InvalidKey(t *testing.T) {
	c := newTestCache(t, 1<<20)
	bad := []string{"", "../escape", "/abs/path", "a/../../b"}
	for _, k := range bad {
		if _, err := c.Put(k, "e", 1, bytes.NewReader([]byte("x"))); err == nil {
			t.Errorf("Put(%q) should reject invalid key", k)
		}
		if _, hit, _ := c.Get(k, "e"); hit {
			t.Errorf("Get(%q) should not hit invalid key", k)
		}
		if c.Contains(k, "e") {
			t.Errorf("Contains(%q) should be false for invalid key", k)
		}
		if err := c.Remove(k); err == nil {
			t.Errorf("Remove(%q) should reject invalid key", k)
		}
	}
}

func TestNewNVMeCache_InvalidArgs(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name                     string
		dir, bucket              string
		capacity                 int64
		high, low                float64
	}{
		{"empty dir", "", "b", 1000, 0.85, 0.70},
		{"zero capacity", dir, "b", 0, 0.85, 0.70},
		{"negative capacity", dir, "b", -1, 0.85, 0.70},
		{"high > 1", dir, "b", 1000, 1.5, 0.70},
		{"low >= high", dir, "b", 1000, 0.70, 0.70},
		{"low zero", dir, "b", 1000, 0.85, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewNVMeCache(tc.dir, tc.bucket, tc.capacity, tc.high, tc.low, 0); err == nil {
				t.Errorf("expected error for %s", tc.name)
			}
		})
	}
}

// TestNVMeCache_PutOverwrite 验证同 key 重复 Put 覆盖且 used 不重复累计。
func TestNVMeCache_PutOverwrite(t *testing.T) {
	c := newTestCache(t, 1<<20)
	c.Put("f", "e1", 5, bytes.NewReader([]byte("hello")))
	c.Put("f", "e2", 3, bytes.NewReader([]byte("abc")))

	s := c.Stats()
	if s.UsedBytes != 3 {
		t.Errorf("overwrite should replace size: expected 3, got %d", s.UsedBytes)
	}
	if s.EntryCount != 1 {
		t.Errorf("expected 1 entry, got %d", s.EntryCount)
	}
	// 旧 etag 不再命中
	if c.Contains("f", "e1") {
		t.Error("old etag should not be contained after overwrite")
	}
	if !c.Contains("f", "e2") {
		t.Error("new etag should be contained after overwrite")
	}
}

// TestNVMeCache_LargeStream 验证大文件流式写入（不一次性加载内存）。
func TestNVMeCache_LargeStream(t *testing.T) {
	c := newTestCache(t, 10<<20)
	size := int64(2 << 20) // 2MB
	content := make([]byte, size)
	for i := range content {
		content[i] = byte('a' + i%26)
	}
	path, err := c.Put("bigfile", "etag-big", size, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put large failed: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != size {
		t.Errorf("expected size %d, got %d", size, fi.Size())
	}
	rc, _, _ := c.Get("bigfile", "etag-big")
	got, _ := io.ReadAll(mustOpen(t, rc))
	if !bytes.Equal(got, content) {
		t.Error("large file content mismatch")
	}
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestNVMeCache_Concurrent 验证并发 Put/Get/Remove 无 data race 且统计一致。
// 模拟 PyTorch DataLoader 多 worker 并发访问 (IMPL_DESIGN §7.1)。
func TestNVMeCache_Concurrent(t *testing.T) {
	c := newTestCache(t, 1<<20)
	const workers = 16
	const keysPerWorker = 20

	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < keysPerWorker; i++ {
				key := fmt.Sprintf("w%d/f%d", w, i%5) // 故意让 key 重叠
				data := []byte(fmt.Sprintf("data-%d-%d", w, i))
				if _, err := c.Put(key, "e", int64(len(data)), bytes.NewReader(data)); err != nil {
					continue // 可能因淘汰失败，允许
				}
				_, _, _ = c.Get(key, "e")
				_ = c.Contains(key, "e")
				if i%7 == 0 {
					_ = c.Remove(key)
				}
				_ = c.Stats()
			}
		}(w)
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	// 最终统计应自洽：used <= capacity，entry 数与索引一致
	s := c.Stats()
	if s.UsedBytes > s.CapacityBytes {
		t.Errorf("used %d exceeds capacity %d", s.UsedBytes, s.CapacityBytes)
	}
	if s.UsedBytes < 0 || s.EntryCount < 0 {
		t.Errorf("negative stats: %+v", s)
	}
}
