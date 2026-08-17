package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"syscall"
	"testing"

	"fuel/api"
	"fuel/internal/config"
)

// TestMockStore_Contract 验证 Mock 实现满足 ObjectStore 接口契约。
func TestMockStore_Contract(t *testing.T) {
	var _ api.ObjectStore = (*mockStore)(nil)

	store := NewMockStore("test-bucket")
	ctx := context.Background()

	// Put
	om, err := store.Put(ctx, "a/b.txt", bytes.NewReader([]byte("hello")), 5)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if om.Size != 5 {
		t.Errorf("expected size 5, got %d", om.Size)
	}
	if om.ETag == "" {
		t.Error("expected non-empty etag")
	}

	// Head
	hm, err := store.Head(ctx, "a/b.txt")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if hm.Size != 5 || hm.ETag != om.ETag {
		t.Errorf("Head mismatch: got %+v want size=5 etag=%s", hm, om.ETag)
	}

	// Get 全量
	rc, err := store.Get(ctx, "a/b.txt", 0, 0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}

	// Get Range
	rc, err = store.Get(ctx, "a/b.txt", 1, 3)
	if err != nil {
		t.Fatalf("Get range failed: %v", err)
	}
	data, _ = io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "ell" {
		t.Errorf("expected 'ell', got %q", string(data))
	}

	// List
	entries, prefixes, err := store.List(ctx, "a/", "/", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Key != "a/b.txt" {
		t.Errorf("expected 1 entry a/b.txt, got %+v", entries)
	}
	if len(prefixes) != 0 {
		t.Errorf("expected 0 prefixes, got %+v", prefixes)
	}

	// Copy
	if err := store.Copy(ctx, "a/b.txt", "a/c.txt"); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	if _, err := store.Head(ctx, "a/c.txt"); err != nil {
		t.Fatalf("Head copied object failed: %v", err)
	}

	// Delete
	if err := store.Delete(ctx, "a/c.txt"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, err := store.Head(ctx, "a/c.txt"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("expected ENOENT after delete, got %v", err)
	}

	// Bucket
	if store.Bucket() != "test-bucket" {
		t.Errorf("expected bucket test-bucket, got %q", store.Bucket())
	}
}

// TestMockStore_NotFound 验证不存在对象返回 ENOENT（Delete 除外：OSS Delete 是幂等的，不存在时不报错）。
func TestMockStore_NotFound(t *testing.T) {
	store := NewMockStore("b")
	ctx := context.Background()

	if _, err := store.Head(ctx, "missing"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("Head: expected ENOENT, got %v", err)
	}
	if _, err := store.Get(ctx, "missing", 0, 0); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("Get: expected ENOENT, got %v", err)
	}
	// Delete 是幂等的（与真实 OSS 行为一致）：删除不存在的对象不报错
	if err := store.Delete(ctx, "missing"); err != nil {
		t.Errorf("Delete: expected nil (idempotent), got %v", err)
	}
	if err := store.Copy(ctx, "missing", "dst"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("Copy: expected ENOENT, got %v", err)
	}
}

// TestMockStore_ListDelimiter 验证 List 的 delimiter 子目录分组。
func TestMockStore_ListDelimiter(t *testing.T) {
	store := NewMockStore("b")
	ctx := context.Background()

	keys := []string{"dir/f1.txt", "dir/f2.txt", "dir/sub/g1.txt", "top.txt"}
	for _, k := range keys {
		if _, err := store.Put(ctx, k, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	// 不带 delimiter: 列出 dir/ 下所有对象
	entries, _, err := store.List(ctx, "dir/", "", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries under dir/, got %d", len(entries))
	}

	// 带 delimiter: dir/ 下的直接子项 = f1/f2 + 子目录 sub/
	entries, prefixes, err := store.List(ctx, "dir/", "/", 0)
	if err != nil {
		t.Fatalf("List with delimiter failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 file entries, got %d: %+v", len(entries), entries)
	}
	if len(prefixes) != 1 || prefixes[0] != "dir/sub/" {
		t.Errorf("expected prefix dir/sub/, got %+v", prefixes)
	}
}

// TestNewObjectStore_Unsupported 验证工厂对未知后端报错。
func TestNewObjectStore_Unsupported(t *testing.T) {
	cfg := &config.Config{}
	cfg.Storage.Type = "unknown-backend"
	if _, err := NewObjectStore(cfg); err == nil {
		t.Fatal("expected error for unsupported backend, got nil")
	}
}

// TestNewObjectStore_OSS 验证 OSS 后端已注册并可构造。
func TestNewObjectStore_OSS(t *testing.T) {
	cfg := &config.Config{}
	cfg.Storage.Type = "oss"
	cfg.Storage.Bucket = "test-bucket"
	cfg.Storage.OSS.Endpoint = "oss-cn-test.aliyuncs.com"
	cfg.Storage.AccessKey = "ak"
	cfg.Storage.AccessSecret = "sk"

	store, err := NewObjectStore(cfg)
	if err != nil {
		t.Fatalf("NewObjectStore(oss) failed: %v", err)
	}
	if store.Bucket() != "test-bucket" {
		t.Errorf("expected bucket test-bucket, got %q", store.Bucket())
	}
}

// TestMapError 验证 OSS 错误到 POSIX errno 的映射。
func TestMapError(t *testing.T) {
	if err := mapError("k", nil); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

// TestTrimETag 验证 ETag 引号去除。
func TestTrimETag(t *testing.T) {
	cases := map[string]string{
		`"abc123"`: "abc123",
		`abc123`:   "abc123",
		`""`:       "",
	}
	for in, want := range cases {
		if got := trimETag(in); got != want {
			t.Errorf("trimETag(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestMetaFromHeader 验证从 HEAD 响应头解析 ObjectMeta。
func TestMetaFromHeader(t *testing.T) {
	header := map[string][]string{
		"Content-Length": {"1024"},
		"Etag":           {`"deadbeef"`},
		"Content-Type":   {"application/octet-stream"},
		"Last-Modified":  {"Mon, 02 Jan 2006 15:04:05 GMT"},
	}
	om, err := metaFromHeader("a/b.txt", header)
	if err != nil {
		t.Fatalf("metaFromHeader failed: %v", err)
	}
	if om.Key != "a/b.txt" {
		t.Errorf("expected key a/b.txt, got %q", om.Key)
	}
	if om.Size != 1024 {
		t.Errorf("expected size 1024, got %d", om.Size)
	}
	if om.ETag != "deadbeef" {
		t.Errorf("expected etag deadbeef, got %q", om.ETag)
	}
	if om.ContentType != "application/octet-stream" {
		t.Errorf("expected content type, got %q", om.ContentType)
	}
	if om.LastModified.IsZero() {
		t.Error("expected non-zero LastModified")
	}
}

// TestMetaFromHeader_BadSize 验证非法 Content-Length 报错。
func TestMetaFromHeader_BadSize(t *testing.T) {
	header := map[string][]string{"Content-Length": {"not-a-number"}}
	if _, err := metaFromHeader("k", header); err == nil {
		t.Fatal("expected error for bad content-length, got nil")
	}
}
