package objectstore

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"go.uber.org/zap"
)

// 重试策略 (IMPL_DESIGN §9.1): 指数退避, 最多 3 次。
// 基础间隔 100ms → 200ms → 400ms, 抖动 ±50ms。
const (
	retryMaxAttempts = 3
	retryBaseDelay   = 100 * time.Millisecond
	retryMaxJitter   = 50 * time.Millisecond
)

// doWithRetry 对可重试错误执行指数退避重试。
// fn 返回的 error 若不可重试或重试耗尽，则返回给调用方。
// 重试中 WARN，重试耗尽 ERROR（AGENTS.md §3.2 / PLAN §9.2）。
func doWithRetry(ctx context.Context, op, key string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < retryMaxAttempts; attempt++ {
		if attempt > 0 {
			delay := retryBaseDelay<<uint(attempt-1) + jitter()
			zap.L().Warn("storage request failed, retrying",
				zap.String("op", op), zap.String("key", key),
				zap.Int("attempt", attempt+1), zap.Int("maxAttempts", retryMaxAttempts),
				zap.Duration("delay", delay), zap.Error(lastErr))
			if err := sleep(ctx, delay); err != nil {
				return err
			}
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
	}
	zap.L().Error("storage request failed after retries",
		zap.String("op", op), zap.String("key", key),
		zap.Int("attempts", retryMaxAttempts), zap.Error(lastErr))
	return lastErr
}

// isRetryable 判断错误是否可重试。
// 可重试: 5xx, 429, 网络超时, 连接断开, context 内部超时。
// 不可重试: 400, 403, 404, 409, context 取消。
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// syscall.Errno (ENOENT/EACCES 等本地 errno) 实现了 net.Error.Timeout(),
	// 但其是本地 POSIX 错误而非网络错误，必须先排除，避免误判为可重试。
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var svcErr oss.ServiceError
	if errors.As(err, &svcErr) {
		return isRetryableStatus(svcErr.StatusCode)
	}

	msg := err.Error()
	if strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "TLS handshake") {
		return true
	}
	return false
}

// isRetryableStatus 判断 HTTP 状态码是否可重试。
func isRetryableStatus(code int) bool {
	return code == 429 || code >= 500
}

// jitter 返回 0~retryMaxJitter 的随机抖动，避免重试风暴。
func jitter() time.Duration {
	return time.Duration(rand.Int63n(int64(retryMaxJitter)))
}

// sleep 在 ctx 可取消的前提下睡眠 d。
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
