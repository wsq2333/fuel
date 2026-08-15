//go:build integration

package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"fuel/api"
	"fuel/internal/config"
)

// 集成测试连接真实 OSS。需要环境变量:
//   OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET  (或 FUEL_STORAGE_ACCESS_KEY/SECRET)
//   FUEL_TEST_OSS_ENDPOINT                     (必填)
//   FUEL_TEST_OSS_BUCKET                       (必填)
func integrationStore(t *testing.T) api.ObjectStore {
	t.Helper()
	endpoint := os.Getenv("FUEL_TEST_OSS_ENDPOINT")
	bucket := os.Getenv("FUEL_TEST_OSS_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("skip integration test: FUEL_TEST_OSS_ENDPOINT / FUEL_TEST_OSS_BUCKET not set")
	}

	cfg := &config.Config{}
	cfg.Storage.Type = "oss"
	cfg.Storage.Bucket = bucket
	cfg.Storage.OSS.Endpoint = endpoint
	cfg.Storage.AccessKey = os.Getenv("OSS_ACCESS_KEY_ID")
	cfg.Storage.AccessSecret = os.Getenv("OSS_ACCESS_KEY_SECRET")

	store, err := NewObjectStore(cfg)
	if err != nil {
		t.Fatalf("NewObjectStore failed: %v", err)
	}
	return store
}

func TestIntegration_OSS(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	key := fmt.Sprintf("fuel-it/%d/test.txt", time.Now().UnixNano())
	content := []byte("fuel integration test content")

	// Put
	om, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if om.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), om.Size)
	}
	defer func() { _ = store.Delete(context.Background(), key) }()

	// Head
	hm, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if hm.Size != int64(len(content)) {
		t.Errorf("Head size mismatch: got %d", hm.Size)
	}

	// Get
	rc, err := store.Get(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(data, content) {
		t.Errorf("Get content mismatch: got %q", string(data))
	}

	// Get Range
	rc, err = store.Get(ctx, key, 0, 4)
	if err != nil {
		t.Fatalf("Get range failed: %v", err)
	}
	data, _ = io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "fuel" {
		t.Errorf("Get range expected 'fuel', got %q", string(data))
	}

	// Copy
	dst := key + ".copy"
	if err := store.Copy(ctx, key, dst); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	defer func() { _ = store.Delete(context.Background(), dst) }()
	if _, err := store.Head(ctx, dst); err != nil {
		t.Fatalf("Head copied object failed: %v", err)
	}

	// List
	entries, _, err := store.List(ctx, key[:len("fuel-it/")], "", 100)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(entries) == 0 {
		t.Error("expected at least 1 entry from List")
	}

	// Delete
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, err := store.Head(ctx, key); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("expected ENOENT after delete, got %v", err)
	}
}
