package objectstore

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"
)

type fakeNetError struct{ timeout bool }

func (e fakeNetError) Error() string   { return "fake net error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return e.timeout }

var _ net.Error = fakeNetError{}

// TestDoWithRetry_SuccessFirstTry 验证首次成功不重试。
func TestDoWithRetry_SuccessFirstTry(t *testing.T) {
	calls := 0
	err := doWithRetry(context.Background(), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

// TestDoWithRetry_RetryableThenSuccess 验证可重试错误重试后成功。
func TestDoWithRetry_RetryableThenSuccess(t *testing.T) {
	calls := 0
	err := doWithRetry(context.Background(), func() error {
		calls++
		if calls < 3 {
			return fakeNetError{timeout: true}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil after retries, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

// TestDoWithRetry_NonRetryable 验证不可重试错误立即返回。
func TestDoWithRetry_NonRetryable(t *testing.T) {
	calls := 0
	err := doWithRetry(context.Background(), func() error {
		calls++
		return syscall.ENOENT
	})
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected ENOENT, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry), got %d", calls)
	}
}

// TestDoWithRetry_Exhausted 验证重试耗尽返回最后错误。
func TestDoWithRetry_Exhausted(t *testing.T) {
	calls := 0
	retryable := fakeNetError{timeout: true}
	err := doWithRetry(context.Background(), func() error {
		calls++
		return retryable
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if calls != retryMaxAttempts {
		t.Errorf("expected %d calls, got %d", retryMaxAttempts, calls)
	}
}

// TestDoWithRetry_ContextCanceled 验证 context 取消不重试。
func TestDoWithRetry_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := doWithRetry(ctx, func() error {
		calls++
		cancel()
		return context.Canceled
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

// TestIsRetryable 验证各类错误的可重试判定。
func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, true},
		{"net timeout", fakeNetError{timeout: true}, true},
		{"ENOENT", syscall.ENOENT, false},
		{"EACCES", syscall.EACCES, false},
		{"connection reset", errors.New("read: connection reset by peer"), true},
		{"EOF", errors.New("unexpected EOF"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isRetryable(c.err); got != c.want {
				t.Errorf("isRetryable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestIsRetryableStatus 验证 HTTP 状态码可重试判定。
func TestIsRetryableStatus(t *testing.T) {
	cases := []struct {
		code int
		want bool
	}{
		{200, false},
		{400, false},
		{403, false},
		{404, false},
		{409, false},
		{429, true},
		{500, true},
		{502, true},
		{503, true},
	}
	for _, c := range cases {
		if got := isRetryableStatus(c.code); got != c.want {
			t.Errorf("isRetryableStatus(%d) = %v, want %v", c.code, got, c.want)
		}
	}
}

// TestSleep_ContextCancel 验证 sleep 可被 context 取消。
func TestSleep_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := sleep(ctx, 5*time.Second)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if time.Since(start) > time.Second {
		t.Errorf("sleep did not respect context cancellation, took %v", time.Since(start))
	}
}

// TestSleep_Completes 验证 sleep 正常完成。
func TestSleep_Completes(t *testing.T) {
	if err := sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// TestJitter_Range 验证 jitter 在 [0, max) 范围内。
func TestJitter_Range(t *testing.T) {
	for i := 0; i < 100; i++ {
		j := jitter()
		if j < 0 || j >= retryMaxJitter {
			t.Fatalf("jitter %v out of range [0, %v)", j, retryMaxJitter)
		}
	}
}
