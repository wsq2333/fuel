package monitor

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 日志规范 (AGENTS.md §3.4 / PLAN §9.2)：
//   INFO  挂载/卸载、缓存淘汰、对象存储请求重试成功
//   WARN  对象存储请求失败重试、缓存校验失败、元数据引擎降级
//   ERROR 对象存储请求最终失败、FUSE 操作错误
//   DEBUG 每次 read/write 的缓存命中/未命中（高基数，默认 info 级别下不输出）
// 格式：结构化 JSON（生产采集可直接入 ELK/Loki）。

// NewLogger 构造生产 JSON logger（写 stderr），level 取 debug|info|warn|error。
// 调用方通过 zap.ReplaceGlobals 使其成为全局 logger（zap.L()）。
func NewLogger(level string) (*zap.Logger, error) {
	return newLoggerWithSink(level, zapcore.Lock(os.Stderr))
}

// newLoggerWithSink 同 NewLogger，输出到指定 sink（供测试断言 JSON 格式）。
func newLoggerWithSink(level string, sink zapcore.WriteSyncer) (*zap.Logger, error) {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q (debug|info|warn|error): %w", level, err)
	}

	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		MessageKey:     "msg",
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		CallerKey:      "caller",
		EncodeCaller:   zapcore.ShortCallerEncoder,
	})
	return zap.New(zapcore.NewCore(enc, sink, lvl), zap.AddCaller()), nil
}
