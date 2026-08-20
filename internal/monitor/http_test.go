package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"fuel/internal/objectstore"
)

// TestServer_HealthOK 健康检查通过 → 200 + status ok。
func TestServer_HealthOK(t *testing.T) {
	s := NewServer("127.0.0.1:0", func(ctx context.Context) error { return nil })
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = s.Stop() }()

	resp, err := http.Get("http://" + s.Addr() + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("body = %s, want status ok", body)
	}
}

// TestServer_HealthFail 健康检查失败 → 503 + 错误信息。
func TestServer_HealthFail(t *testing.T) {
	s := NewServer("127.0.0.1:0", func(ctx context.Context) error {
		return errors.New("redis unreachable")
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = s.Stop() }()

	resp, err := http.Get("http://" + s.Addr() + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "redis unreachable") {
		t.Errorf("body = %s, want error message", body)
	}
}

// TestServer_HealthTimeout 健康检查超时应受控返回 503（不挂死探针）。
func TestServer_HealthTimeout(t *testing.T) {
	s := NewServer("127.0.0.1:0", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = s.Stop() }()

	start := time.Now()
	resp, err := http.Get("http://" + s.Addr() + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 on timeout", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > healthCheckTimeout+2*time.Second {
		t.Errorf("health check took %v, should be bounded by %v", elapsed, healthCheckTimeout)
	}
}

// TestServer_Livez /livez 进程存活即 200，与依赖健康无关（供 K8s 探针）。
func TestServer_Livez(t *testing.T) {
	s := NewServer("127.0.0.1:0", func(ctx context.Context) error {
		return errors.New("dependency down") // /health 失败不影响 /livez
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = s.Stop() }()

	resp, err := http.Get("http://" + s.Addr() + "/livez")
	if err != nil {
		t.Fatalf("GET /livez failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 even when dependency down", resp.StatusCode)
	}
}

// TestServer_Metrics /metrics 暴露 fuel_ 前缀指标（promauto 注册到默认注册表）。
func TestServer_Metrics(t *testing.T) {
	// 触发各类指标，确保其 family 出现在 /metrics 文本中
	IncFuseOp("smoke")
	ObserveFuseRead(time.Millisecond)
	ObserveBatchPrefetch(1, 512)
	store := InstrumentStore(objectstore.NewMockStore("b"))
	_, _ = store.Head(context.Background(), "k") // 触发 fuel_storage_requests_total

	s := NewServer("127.0.0.1:0", nil)
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = s.Stop() }()

	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{
		"fuel_storage_requests_total",
		"fuel_fuse_operations_total",
		"fuel_fuse_read_duration_seconds",
		"fuel_prefetch_total",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

// TestServer_StartBindError 端口被占用时 Start 返回明确错误（不 panic）。
func TestServer_StartBindError(t *testing.T) {
	s1 := NewServer("127.0.0.1:0", nil)
	if err := s1.Start(); err != nil {
		t.Fatalf("Start s1 failed: %v", err)
	}
	defer func() { _ = s1.Stop() }()

	s2 := NewServer(s1.Addr(), nil)
	if err := s2.Start(); err == nil {
		t.Error("Start on occupied port should fail")
		_ = s2.Stop()
	}
}
