package cache

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestNVMeCache_Get_InvalidKey 非法 key 返回错误。
func TestNVMeCache_Get_InvalidKey(t *testing.T) {
	c := newTestCache(t, 1<<20)
	badKeys := []string{"", "../escape", "/absolute", "a/../../b"}
	for _, k := range badKeys {
		_, _, err := c.Get(k, "e")
		if err == nil {
			t.Errorf("Get(%q) should return error for invalid key", k)
		}
	}
}

// TestNVMeCache_Put_InvalidKey 非法 key 返回错误。
func TestNVMeCache_Put_InvalidKey(t *testing.T) {
	c := newTestCache(t, 1<<20)
	badKeys := []string{"", "../escape", "/absolute"}
	for _, k := range badKeys {
		_, err := c.Put(k, "e", 5, bytes.NewReader([]byte("hello")))
		if err == nil {
			t.Errorf("Put(%q) should return error for invalid key", k)
		}
	}
}

// TestNVMeCache_Contains_EtagMismatch Contains 在 ETag 不匹配时返回 false。
func TestNVMeCache_Contains_EtagMismatch(t *testing.T) {
	c := newTestCache(t, 1<<20)
	_, _ = c.Put("a.txt", "etag1", 5, bytes.NewReader([]byte("hello")))

	if c.Contains("a.txt", "etag-wrong") {
		t.Error("Contains with wrong etag should return false")
	}
	if !c.Contains("a.txt", "etag1") {
		t.Error("Contains with correct etag should return true")
	}
}

// TestNVMeCache_Put_OverwriteSameKey 同 key 多次 Put 覆盖旧数据。
func TestNVMeCache_Put_OverwriteSameKey(t *testing.T) {
	c := newTestCache(t, 1<<20)

	_, err := c.Put("file.txt", "e1", 5, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	_, err = c.Put("file.txt", "e2", 5, bytes.NewReader([]byte("world")))
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}

	// 新 etag hit
	path, hit, _ := c.Get("file.txt", "e2")
	if !hit {
		t.Fatal("new etag should hit after overwrite")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "world" {
		t.Errorf("content = %q, want 'world'", data)
	}

	// Contains 对旧 etag 返回 false（index 里存的是新 etag）
	if c.Contains("file.txt", "e1") {
		t.Error("old etag should not match after overwrite")
	}
}

// TestNVMeCache_Remove_NonExistent 删除不存在的 key 不报错。
func TestNVMeCache_Remove_NonExistent(t *testing.T) {
	c := newTestCache(t, 1<<20)
	if err := c.Remove("ghost"); err != nil {
		t.Errorf("Remove nonexistent should not error: %v", err)
	}
}

// TestNVMeCache_Put_MaxFileSize 超 maxFileSize 的文件不缓存。
func TestNVMeCache_Put_MaxFileSize(t *testing.T) {
	c, err := NewNVMeCache(t.TempDir(), "b", 1<<30, 0.85, 0.70, 100) // max 100 bytes
	if err != nil {
		t.Fatal(err)
	}
	_, putErr := c.Put("big.bin", "e", 200, bytes.NewReader(bytes.Repeat([]byte("x"), 200)))
	if putErr == nil {
		t.Error("Put should reject file exceeding maxFileSize")
	}
	if c.Contains("big.bin", "e") {
		t.Error("oversized file should not be cached")
	}
}

// TestNVMeCache_Stats_Accurate 验证 Stats 准确反映命中/未命中。
func TestNVMeCache_Stats_Accurate(t *testing.T) {
	c := newTestCache(t, 1<<20)
	_, _ = c.Put("a", "e", 5, bytes.NewReader([]byte("hello")))

	// hit
	c.Get("a", "e")
	// miss
	c.Get("b", "e")
	c.Get("c", "e")

	s := c.Stats()
	if s.HitCount != 1 {
		t.Errorf("hits = %d, want 1", s.HitCount)
	}
	if s.MissCount != 2 {
		t.Errorf("misses = %d, want 2", s.MissCount)
	}
}

// TestNVMeCache_ConcurrentPutGet 并发 Put 和 Get 不 panic。
func TestNVMeCache_ConcurrentPutGet(t *testing.T) {
	c := newTestCache(t, 10<<20)
	const n = 20
	var wg sync.WaitGroup
	wg.Add(n * 2)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			key := "file" + string(rune('a'+idx%10))
			data := bytes.Repeat([]byte{byte(idx)}, 1000)
			_, _ = c.Put(key, "e", int64(len(data)), bytes.NewReader(data))
		}(i)
		go func(idx int) {
			defer wg.Done()
			key := "file" + string(rune('a'+idx%10))
			c.Get(key, "e")
		}(i)
	}
	wg.Wait()
}

// TestNVMeCache_Get_FileDeletedExternally 文件被外部删除时返回 miss 并清理索引。
func TestNVMeCache_Get_FileDeletedExternally(t *testing.T) {
	c := newTestCache(t, 1<<20)
	path, _ := c.Put("ext.txt", "e", 5, bytes.NewReader([]byte("hello")))

	// 外部删除文件
	_ = os.Remove(path)

	_, hit, err := c.Get("ext.txt", "e")
	if err != nil {
		t.Fatalf("Get after external delete: %v", err)
	}
	if hit {
		t.Error("should miss after external file deletion")
	}
	// 索引已清理
	if c.Contains("ext.txt", "e") {
		t.Error("Contains should be false after external deletion + Get")
	}
}

// TestNVMeCache_Put_ZeroSize 零字节文件可以正常缓存。
func TestNVMeCache_Put_ZeroSize(t *testing.T) {
	c := newTestCache(t, 1<<20)
	path, err := c.Put("empty", "e", 0, bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("Put zero-size: %v", err)
	}
	if path == "" {
		t.Error("should return a valid path for zero-size file")
	}
	localPath, hit, _ := c.Get("empty", "e")
	if !hit {
		t.Error("zero-size file should be cached")
	}
	data, _ := os.ReadFile(localPath)
	if len(data) != 0 {
		t.Errorf("cached zero-size file should be empty, got %d bytes", len(data))
	}
}

// TestNVMeCache_Eviction_TriggersOnHighWatermark 写入超高水位触发淘汰。
func TestNVMeCache_Eviction_TriggersOnHighWatermark(t *testing.T) {
	// capacity=1000, highWatermark=0.80=800, lowWatermark=0.50=500
	c, err := NewNVMeCache(t.TempDir(), "b", 1000, 0.80, 0.50, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 写入 3 个 300 字节文件 → 第 3 个 (300+300+300=900>800) 触发淘汰
	for i := 0; i < 3; i++ {
		key := "f" + string(rune('a'+i))
		data := bytes.Repeat([]byte{byte(i)}, 300)
		if _, err := c.Put(key, "e", 300, bytes.NewReader(data)); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	// 最旧的 fa 应被淘汰
	if c.Contains("fa", "e") {
		t.Error("oldest entry 'fa' should have been evicted")
	}
	// 最新的 fc 仍在
	if !c.Contains("fc", "e") {
		t.Error("newest entry 'fc' should still be cached")
	}
}

// TestNVMeCache_Put_DeepNestedKey 深层嵌套 key 正确创建目录。
func TestNVMeCache_Put_DeepNestedKey(t *testing.T) {
	c := newTestCache(t, 1<<20)
	key := "a/b/c/d/e/f/deep.txt"
	_, err := c.Put(key, "e", 5, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Put deep nested: %v", err)
	}
	path, hit, _ := c.Get(key, "e")
	if !hit {
		t.Fatal("deep nested should hit")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "hello" {
		t.Errorf("content = %q, want 'hello'", data)
	}
}

// TestNVMeCache_CleanOrphanTemps 启动时清理残留的 .fuel-* 临时文件。
func TestNVMeCache_CleanOrphanTemps(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "b")
	_ = os.MkdirAll(root, 0o755)

	// 制造残留临时文件
	orphanPath := filepath.Join(root, ".fuel-orphan123")
	_ = os.WriteFile(orphanPath, []byte("garbage"), 0o644)

	// NewNVMeCache 应在构造时清理
	_, err := NewNVMeCache(dir, "b", 1<<20, 0.85, 0.70, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, statErr := os.Stat(orphanPath); !os.IsNotExist(statErr) {
		t.Error("orphan temp file should be cleaned on startup")
	}
}

// TestNVMeCache_Put_ReaderError 读 reader 失败时不污染索引。
func TestNVMeCache_Put_ReaderError(t *testing.T) {
	c := newTestCache(t, 1<<20)

	// 制造一个读到一半就失败的 reader
	failReader := &failAfterReader{data: []byte("partial"), failAt: 3}
	_, err := c.Put("broken.txt", "e", 7, failReader)
	if err == nil {
		t.Fatal("Put with failing reader should return error")
	}
	if c.Contains("broken.txt", "e") {
		t.Error("index should not contain key after reader failure")
	}
}

// TestNVMeCache_Verify_CorruptedFile Verify 检测并剔除损坏文件。
func TestNVMeCache_Verify_CorruptedFile(t *testing.T) {
	c := newTestCache(t, 1<<20)
	data := []byte("original content")
	etag := computeMD5(data)

	path, _ := c.Put("verify.txt", etag, int64(len(data)), bytes.NewReader(data))

	// 篡改缓存文件内容
	_ = os.WriteFile(path, []byte("corrupted!!!!"), 0o644)

	// CacheVerifier 接口断言
	verifier, ok := c.(interface {
		Verify() interface {
		}
	})
	if !ok {
		t.Skip("DataCache does not implement Verify()")
	}
	_ = verifier

	// 直接调用 nvmeCache.Verify
	nc := c.(*nvmeCache)
	result := nc.Verify()
	if len(result.Corrupted) == 0 {
		t.Error("Verify should detect corrupted file")
	}
	if c.Contains("verify.txt", etag) {
		t.Error("corrupted file should be removed from index")
	}
}

// TestNVMeCache_Contains_InvalidKey 非法 key 返回 false。
func TestNVMeCache_Contains_InvalidKey(t *testing.T) {
	c := newTestCache(t, 1<<20)
	if c.Contains("", "e") {
		t.Error("empty key should return false")
	}
	if c.Contains("../escape", "e") {
		t.Error("path traversal key should return false")
	}
}

// --- 辅助 ---

type failAfterReader struct {
	data   []byte
	failAt int
	pos    int
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAt {
		return 0, io.ErrUnexpectedEOF
	}
	remaining := r.failAt - r.pos
	if remaining > len(p) {
		remaining = len(p)
	}
	if r.pos+remaining > len(r.data) {
		remaining = len(r.data) - r.pos
	}
	if remaining <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p[:remaining], r.data[r.pos:r.pos+remaining])
	r.pos += n
	return n, nil
}

func computeMD5(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}
