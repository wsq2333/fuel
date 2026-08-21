package monitor

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestNewLogger_JSONFormat 验证日志输出为结构化 JSON 且包含规范字段。
func TestNewLogger_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger, err := newLoggerWithSink("info", zapcore.AddSync(&buf))
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	logger.Info("mount ok", zap.String("mountPoint", "/fuel/b"), zap.Int("pid", 1234))

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v; raw=%q", err, buf.String())
	}
	if entry["level"] != "info" {
		t.Errorf("level = %v, want info", entry["level"])
	}
	if entry["msg"] != "mount ok" {
		t.Errorf("msg = %v, want 'mount ok'", entry["msg"])
	}
	if entry["mountPoint"] != "/fuel/b" {
		t.Errorf("structured field lost: %v", entry)
	}
	if _, ok := entry["ts"]; !ok {
		t.Error("missing ts field")
	}
	if _, ok := entry["caller"]; !ok {
		t.Error("missing caller field")
	}
}

// TestNewLogger_LevelFiltering 验证级别过滤：info 级别丢弃 debug，warn 级别丢弃 info。
func TestNewLogger_LevelFiltering(t *testing.T) {
	cases := []struct {
		level      string
		logAt      zapcore.Level
		shouldEmit bool
	}{
		{"info", zapcore.DebugLevel, false},
		{"info", zapcore.InfoLevel, true},
		{"warn", zapcore.InfoLevel, false},
		{"warn", zapcore.WarnLevel, true},
		{"error", zapcore.WarnLevel, false},
		{"debug", zapcore.DebugLevel, true},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		logger, err := newLoggerWithSink(tc.level, zapcore.AddSync(&buf))
		if err != nil {
			t.Fatalf("NewLogger(%q) failed: %v", tc.level, err)
		}
		switch tc.logAt {
		case zapcore.DebugLevel:
			logger.Debug("probe")
		case zapcore.InfoLevel:
			logger.Info("probe")
		case zapcore.WarnLevel:
			logger.Warn("probe")
		}
		emitted := buf.Len() > 0
		if emitted != tc.shouldEmit {
			t.Errorf("level=%s logAt=%v: emitted=%v, want %v", tc.level, tc.logAt, emitted, tc.shouldEmit)
		}
	}
}

// TestNewLogger_InvalidLevel 非法级别返回明确错误。
func TestNewLogger_InvalidLevel(t *testing.T) {
	if _, err := NewLogger("verbose"); err == nil {
		t.Error("NewLogger with invalid level should fail")
	}
}
