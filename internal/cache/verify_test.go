package cache

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"os"
	"testing"
)

// md5Of 计算内容的 MD5 hex（模拟 OSS 整文件上传的 ETag）。
func md5Of(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

func TestVerify_GoodFile(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	content := []byte("consistent data")
	if _, err := c.Put("good.txt", md5Of(content), int64(len(content)), bytes.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	res := c.Verify()
	if res.Checked != 1 {
		t.Errorf("expected 1 checked, got %d", res.Checked)
	}
	if len(res.Corrupted) != 0 {
		t.Errorf("expected no corruption, got %v", res.Corrupted)
	}
	if !c.Contains("good.txt", md5Of(content)) {
		t.Error("good file should remain cached")
	}
}

func TestVerify_DetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	content := []byte("original-bytes")
	etag := md5Of(content)
	path, err := c.Put("victim.bin", etag, int64(len(content)), bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// 模拟磁盘损坏/bit翻转：改内容但保留原 ETag 在索引
	if err := os.WriteFile(path, []byte("CORRUPTEDXXXX"), 0o644); err != nil {
		t.Fatalf("corrupt file: %v", err)
	}

	res := c.Verify()
	if res.Checked != 1 {
		t.Errorf("expected 1 checked, got %d", res.Checked)
	}
	if len(res.Corrupted) != 1 || res.Corrupted[0] != "victim.bin" {
		t.Errorf("expected victim.bin corrupted, got %v", res.Corrupted)
	}
	// 坏文件被剔除：索引 + 磁盘都清
	if c.Contains("victim.bin", etag) {
		t.Error("corrupted file should be removed from index")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("corrupted file should be removed from disk")
	}
}

func TestVerify_MissingFile(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	content := []byte("will be deleted")
	etag := md5Of(content)
	path, _ := c.Put("gone.txt", etag, int64(len(content)), bytes.NewReader(content))
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}

	res := c.Verify()
	if len(res.Missing) != 1 || res.Missing[0] != "gone.txt" {
		t.Errorf("expected gone.txt missing, got %v", res.Missing)
	}
	if len(res.Corrupted) != 0 {
		t.Errorf("missing should not count as corrupted, got %v", res.Corrupted)
	}
	if c.Contains("gone.txt", etag) {
		t.Error("missing file should be removed from index")
	}
}

func TestVerify_SkipsMultipartETag(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	// Multipart 上传的 ETag 含 "-"，不是内容 MD5 → 跳过
	content := []byte("multipart object")
	multipartETag := md5Of(content) + "-3"
	if _, err := c.Put("mp.bin", multipartETag, int64(len(content)), bytes.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	res := c.Verify()
	if res.Skipped != 1 {
		t.Errorf("expected 1 skipped, got %d", res.Skipped)
	}
	if res.Checked != 0 {
		t.Errorf("multipart etag should not be content-checked, got checked=%d", res.Checked)
	}
	// 跳过的文件保留（不误删）
	if !c.Contains("mp.bin", multipartETag) {
		t.Error("multipart file should remain cached")
	}
}

func TestVerify_SkipsNonMD5ETag(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	// 非 MD5 格式 ETag（如 mock 的 etag-key-len）→ 跳过
	content := []byte("x")
	if _, err := c.Put("odd.txt", "etag-not-md5", int64(len(content)), bytes.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	res := c.Verify()
	if res.Skipped != 1 {
		t.Errorf("expected 1 skipped for non-md5 etag, got %d", res.Skipped)
	}
}

func TestVerify_Mixed(t *testing.T) {
	dir := t.TempDir()
	c := newCacheAt(t, dir)

	// 好文件
	good := []byte("good")
	c.Put("good", md5Of(good), int64(len(good)), bytes.NewReader(good))
	// 坏文件
	bad := []byte("bad")
	badPath, _ := c.Put("bad", md5Of(bad), int64(len(bad)), bytes.NewReader(bad))
	os.WriteFile(badPath, []byte("XXX"), 0o644)
	// multipart 跳过
	mp := []byte("mp")
	c.Put("mp", md5Of(mp)+"-2", int64(len(mp)), bytes.NewReader(mp))

	res := c.Verify()
	if res.Checked != 2 {
		t.Errorf("expected 2 checked (good+bad), got %d", res.Checked)
	}
	if len(res.Corrupted) != 1 || res.Corrupted[0] != "bad" {
		t.Errorf("expected only bad corrupted, got %v", res.Corrupted)
	}
	if res.Skipped != 1 {
		t.Errorf("expected 1 skipped, got %d", res.Skipped)
	}
	if !c.Contains("good", md5Of(good)) {
		t.Error("good should remain")
	}
}

func TestIsContentMD5ETag(t *testing.T) {
	cases := []struct {
		etag string
		want bool
	}{
		{"1e02a5a4bff5dad7cd2af184c8899678", true},  // 32 hex
		{"d41d8cd98f00b204e9800998ecf8427e", true},  // empty content md5
		{"1e02a5a4bff5dad7cd2af184c8899678-3", false}, // multipart
		{"etag-abc", false},                          // 非 hex
		{"short", false},                             // 太短
		{"1E02A5A4BFF5DAD7CD2AF184C8899678", true},  // 大写 hex 也合法
	}
	for _, c := range cases {
		if got := isContentMD5ETag(c.etag); got != c.want {
			t.Errorf("isContentMD5ETag(%q) = %v, want %v", c.etag, got, c.want)
		}
	}
}
