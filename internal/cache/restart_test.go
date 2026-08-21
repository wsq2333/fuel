package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// newCacheAt 在指定目录构造缓存（便于同一目录重复构造以模拟重启）。
func newCacheAt(t *testing.T, dir string) *nvmeCache {
	t.Helper()
	c, err := NewNVMeCache(dir, "b", 1<<20, 0.85, 0.70, 0)
	if err != nil {
		t.Fatalf("NewNVMeCache: %v", err)
	}
	return c.(*nvmeCache)
}

// TestCleanOrphanTemps_OnRestart 模拟崩溃后重启：孤儿临时文件被清理，正常缓存文件保留。
func TestCleanOrphanTemps_OnRestart(t *testing.T) {
	dir := t.TempDir()

	// 第一次启动，写入一个正常缓存文件
	c1 := newCacheAt(t, dir)
	goodPath, err := c1.Put("train/f1.jpg", "etag1", 5, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// 模拟崩溃：留下两个半成品临时文件（一个根级，一个嵌套）
	orphan1 := filepath.Join(dir, "b", ".fuel-111222")
	orphan2 := filepath.Join(dir, "b", "train", ".fuel-333444")
	if err := os.WriteFile(orphan1, []byte("partial"), 0o644); err != nil {
		t.Fatalf("seed orphan1: %v", err)
	}
	if err := os.WriteFile(orphan2, []byte("partial"), 0o644); err != nil {
		t.Fatalf("seed orphan2: %v", err)
	}

	// 重启（同目录重新构造）→ 触发 cleanOrphanTemps
	c2 := newCacheAt(t, dir)

	// 孤儿临时文件被清理
	if _, err := os.Stat(orphan1); !os.IsNotExist(err) {
		t.Errorf("orphan temp at root should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(orphan2); !os.IsNotExist(err) {
		t.Errorf("orphan temp in subdir should be removed, stat err=%v", err)
	}

	// 正常缓存文件保留（不被误删）
	if _, err := os.Stat(goodPath); err != nil {
		t.Errorf("good cache file should be kept, stat err=%v", err)
	}
	data, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatalf("read good file: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("good file content corrupted: %q", string(data))
	}
	_ = c2
}

// TestCleanOrphanTemps_NoOrphans 无孤儿时正常返回，不影响已有文件。
func TestCleanOrphanTemps_NoOrphans(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	// 无孤儿，已有正常文件
	if _, err := c.Put("a.txt", "e", 3, bytes.NewReader([]byte("abc"))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	removed := c.cleanOrphanTemps()
	if removed != 0 {
		t.Errorf("expected 0 removed, got %d", removed)
	}
	// 正常文件仍在
	if !c.Contains("a.txt", "e") {
		t.Error("normal file should still be present")
	}
}

// TestCleanOrphanTemps_EmptyDir 空缓存目录不报错。
func TestCleanOrphanTemps_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)
	if removed := c.cleanOrphanTemps(); removed != 0 {
		t.Errorf("expected 0 removed on empty dir, got %d", removed)
	}
}

// TestCleanOrphanTemps_OnlyTemps 全是孤儿时全部清理并返回正确计数。
func TestCleanOrphanTemps_OnlyTemps(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "b")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	names := []string{".fuel-aaa", ".fuel-bbb", ".fuel-ccc"}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(root, n), []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}
	// 混入一个非临时文件（以 .fuel 开头但不带 '-'，不应匹配）
	nonTemp := filepath.Join(root, ".fuelx")
	if err := os.WriteFile(nonTemp, []byte("keep"), 0o644); err != nil {
		t.Fatalf("seed nonTemp: %v", err)
	}

	c := newCacheAt(t, dir) // 构造时已清理
	_ = c

	for _, n := range names {
		if _, err := os.Stat(filepath.Join(root, n)); !os.IsNotExist(err) {
			t.Errorf("temp %s should be removed", n)
		}
	}
	// .fuelx 不以 ".fuel-" 为前缀，应保留
	if _, err := os.Stat(nonTemp); err != nil {
		t.Errorf(".fuelx should be kept (not matching prefix .fuel-), stat err=%v", err)
	}
}

// TestScanRebuild_OnRestart 模拟进程重启：存量缓存文件被扫描登记进索引，
// 空间恢复记账（参与 LRU 淘汰）；ETag 未知的条目 Get 必然 miss（INV-9：
// 不返回无法证明正确的数据），miss 后剔除，重新 Put 后恢复命中。
func TestScanRebuild_OnRestart(t *testing.T) {
	dir := t.TempDir()

	// 第一次运行：写入两个缓存文件
	c1 := newCacheAt(t, dir)
	if _, err := c1.Put("train/f1.jpg", "etag1", 5, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("Put f1: %v", err)
	}
	if _, err := c1.Put("train/f2.jpg", "etag2", 6, bytes.NewReader([]byte("world!"))); err != nil {
		t.Fatalf("Put f2: %v", err)
	}

	// 崩溃 → 同目录重新构造（触发扫描重建）
	c2 := newCacheAt(t, dir)

	// 空间记账恢复：两个文件的字节数计入 used
	stats := c2.Stats()
	if stats.EntryCount != 2 {
		t.Errorf("rebuilt entry count = %d, want 2", stats.EntryCount)
	}
	if stats.UsedBytes != 11 {
		t.Errorf("rebuilt used bytes = %d, want 11", stats.UsedBytes)
	}

	// ETag 未知 → Get miss（INV-9：不可证明正确不返回），且条目被剔除
	if _, hit, err := c2.Get("train/f1.jpg", "etag1"); err != nil || hit {
		t.Errorf("Get rebuilt entry = hit %v err %v, want miss (unknown etag)", hit, err)
	}
	if _, err := os.Stat(c2.localPath("train/f1.jpg")); !os.IsNotExist(err) {
		t.Error("unverifiable rebuilt file should be removed on miss")
	}

	// miss 后回源重拉（Put）→ 恢复正常命中
	if _, err := c2.Put("train/f1.jpg", "etag1", 5, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("re-Put: %v", err)
	}
	if _, hit, _ := c2.Get("train/f1.jpg", "etag1"); !hit {
		t.Error("Get after re-Put should hit")
	}
	// f2 未被访问，仍登记在索引中（可被淘汰，不泄漏）
	if !c2.Contains("train/f2.jpg", "") {
		t.Error("untouched rebuilt entry should remain in index")
	}
}
