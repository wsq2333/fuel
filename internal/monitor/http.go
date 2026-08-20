package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// healthCheckTimeout 是 /health 调用健康检查函数的超时（避免元数据引擎卡死拖挂探针）。
const healthCheckTimeout = 3 * time.Second

// HealthCheckFunc 返回 nil 表示健康（用于探活元数据引擎等依赖）。
type HealthCheckFunc func(ctx context.Context) error

// Server 暴露 /metrics 与 /health 端点 (ARCH_SPEC §9.2)。
// /livez 始终返回 200（进程存活），供 K8s liveness/readiness 探针使用——
// 不能用 /health 做探针：依赖（如 Redis/OSS）不可用时 /health 返回 503，
// 若用于 liveness 会导致 kubelet 重启容器，FUSE 挂载中断反而放大故障。
type Server struct {
	srv     *http.Server
	ln      net.Listener
	checker HealthCheckFunc
}

// NewServer 构造监控端点服务器。addr 形如 ":49999"；checker 为健康检查函数。
func NewServer(addr string, checker HealthCheckFunc) *Server {
	s := &Server{checker: checker}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/livez", s.handleLivez)
	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Start 绑定端口并后台服务。绑定失败（如端口占用）立即返回错误。
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("monitor listen %s: %w", s.srv.Addr, err)
	}
	s.ln = ln
	go func() {
		_ = s.srv.Serve(ln)
	}()
	return nil
}

// Stop 优雅关闭（等待进行中请求结束，最多 5s）。
func (s *Server) Stop() error {
	if s.ln == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

// Addr 返回实际监听地址（Start 后可用；配置 ":0" 时为分配到的端口）。
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// handleLivez 进程存活探针：进程在跑就返回 200，不检查任何依赖。
func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"alive"}`))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
	defer cancel()

	status := http.StatusOK
	body := map[string]string{"status": "ok"}
	if s.checker != nil {
		if err := s.checker(ctx); err != nil {
			status = http.StatusServiceUnavailable
			body["status"] = "unhealthy"
			body["error"] = err.Error()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
