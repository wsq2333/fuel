package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "fuel-config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoad_ValidYAML(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: test-bucket
  oss:
    endpoint: oss-cn-test.aliyuncs.com
cache:
  dir: /tmp/cache
  capacity: 1000000
fuse:
  mountPoint: /fuel/test-bucket
`
	path := writeTempConfig(t, yaml)
	t.Setenv("OSS_ACCESS_KEY_ID", "ak-123")
	t.Setenv("OSS_ACCESS_KEY_SECRET", "sk-456")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Storage.Type != "oss" {
		t.Errorf("expected storage.type oss, got %q", cfg.Storage.Type)
	}
	if cfg.Storage.Bucket != "test-bucket" {
		t.Errorf("expected bucket test-bucket, got %q", cfg.Storage.Bucket)
	}
	if cfg.Storage.OSS.Endpoint != "oss-cn-test.aliyuncs.com" {
		t.Errorf("expected endpoint, got %q", cfg.Storage.OSS.Endpoint)
	}
	if cfg.Storage.AccessKey != "ak-123" {
		t.Errorf("expected AccessKey ak-123, got %q", cfg.Storage.AccessKey)
	}
	if cfg.Storage.AccessSecret != "sk-456" {
		t.Errorf("expected AccessSecret sk-456, got %q", cfg.Storage.AccessSecret)
	}
	if cfg.Cache.Dir != "/tmp/cache" {
		t.Errorf("expected cache.dir /tmp/cache, got %q", cfg.Cache.Dir)
	}
	if cfg.Cache.Capacity != 1000000 {
		t.Errorf("expected capacity 1000000, got %d", cfg.Cache.Capacity)
	}
	if cfg.Fuse.MountPoint != "/fuel/test-bucket" {
		t.Errorf("expected mountPoint, got %q", cfg.Fuse.MountPoint)
	}
}

func TestLoad_Defaults(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: b
  oss:
    endpoint: e
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Metadata.Engine != "direct" {
		t.Errorf("expected default engine direct, got %q", cfg.Metadata.Engine)
	}
	if cfg.Metadata.Cache.StatTTL != 30*time.Second {
		t.Errorf("expected default statTTL 30s, got %v", cfg.Metadata.Cache.StatTTL)
	}
	if cfg.Cache.HighWatermark != 0.85 {
		t.Errorf("expected default highWatermark 0.85, got %v", cfg.Cache.HighWatermark)
	}
	if cfg.Monitor.MetricsAddr != ":49999" {
		t.Errorf("expected default metricsAddr :49999, got %q", cfg.Monitor.MetricsAddr)
	}
}

func TestLoad_FuelStorageEnvFallback(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: b
  oss:
    endpoint: e
`
	path := writeTempConfig(t, yaml)
	t.Setenv("FUEL_STORAGE_ACCESS_KEY", "fuel-ak")
	t.Setenv("FUEL_STORAGE_ACCESS_SECRET", "fuel-sk")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Storage.AccessKey != "fuel-ak" {
		t.Errorf("expected fallback FUEL_STORAGE_ACCESS_KEY, got %q", cfg.Storage.AccessKey)
	}
	if cfg.Storage.AccessSecret != "fuel-sk" {
		t.Errorf("expected fallback FUEL_STORAGE_ACCESS_SECRET, got %q", cfg.Storage.AccessSecret)
	}
}

func TestLoad_OSSEnvTakesPrecedence(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: b
  oss:
    endpoint: e
`
	path := writeTempConfig(t, yaml)
	t.Setenv("OSS_ACCESS_KEY_ID", "oss-ak")
	t.Setenv("FUEL_STORAGE_ACCESS_KEY", "fuel-ak")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Storage.AccessKey != "oss-ak" {
		t.Errorf("expected OSS_ACCESS_KEY_ID to take precedence, got %q", cfg.Storage.AccessKey)
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	if _, err := Load("/nonexistent/fuel-config.yaml"); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := writeTempConfig(t, "storage: [unclosed")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid yaml, got nil")
	}
}

func TestLoad_MissingBucket(t *testing.T) {
	yaml := `
storage:
  type: oss
  oss:
    endpoint: e
`
	path := writeTempConfig(t, yaml)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for missing bucket, got nil")
	}
}

func TestLoad_MissingOSSEndpoint(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: b
`
	path := writeTempConfig(t, yaml)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for missing oss endpoint, got nil")
	}
}

func TestLoad_UnsupportedBackend(t *testing.T) {
	yaml := `
storage:
  type: s3
  bucket: b
`
	path := writeTempConfig(t, yaml)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for unimplemented backend, got nil")
	}
}

func TestLoad_EmptyPathUsesDefaults(t *testing.T) {
	// 空路径仅当其余校验通过时成功；此处缺 bucket 应报错
	if _, err := Load(""); err == nil {
		t.Fatal("expected error for missing bucket with empty path, got nil")
	}
}

func TestLoad_VerifyInterval(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: b
  oss:
    endpoint: e
cache:
  dir: /tmp/cache
  verifyInterval: 10m
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Cache.VerifyInterval != 10*time.Minute {
		t.Errorf("expected verifyInterval 10m, got %v", cfg.Cache.VerifyInterval)
	}
}

func TestLoad_VerifyIntervalDefaultZero(t *testing.T) {
	yaml := `
storage:
  type: oss
  bucket: b
  oss:
    endpoint: e
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Cache.VerifyInterval != 0 {
		t.Errorf("expected default verifyInterval 0 (disabled), got %v", cfg.Cache.VerifyInterval)
	}
}
