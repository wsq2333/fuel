package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 全局配置。
type Config struct {
	Storage  StorageConfig  `yaml:"storage"`
	Metadata MetadataConfig `yaml:"metadata"`
	Cache    CacheConfig    `yaml:"cache"`
	Prefetch PrefetchConfig `yaml:"prefetch"`
	Fuse     FuseConfig     `yaml:"fuse"`
	Monitor  MonitorConfig  `yaml:"monitor"`
}

// StorageConfig 对象存储后端配置 (INV-8)。
type StorageConfig struct {
	Type   string    `yaml:"type"` // oss | s3 | minio
	Bucket string    `yaml:"bucket"`
	OSS    OSSConfig `yaml:"oss"`

	// 敏感信息通过环境变量注入，不写入配置文件。
	AccessKey    string `yaml:"-"`
	AccessSecret string `yaml:"-"`
}

// OSSConfig OSS 后端专属配置。
type OSSConfig struct {
	Endpoint string `yaml:"endpoint"`
}

// MetadataConfig 元数据引擎配置 (INV-4)。
type MetadataConfig struct {
	Engine string          `yaml:"engine"` // direct | redis | mysql
	Redis  RedisConfig     `yaml:"redis"`
	MySQL  MySQLConfig     `yaml:"mysql"`
	Cache  MetaCacheConfig `yaml:"cache"`
}

// RedisConfig Redis 元数据引擎配置。
type RedisConfig struct {
	Address string `yaml:"address"`
}

// MySQLConfig MySQL 元数据引擎配置。
type MySQLConfig struct {
	DSN string `yaml:"dsn"`
}

// MetaCacheConfig L1 元数据缓存 TTL。
type MetaCacheConfig struct {
	StatTTL time.Duration `yaml:"statTTL"`
	DirTTL  time.Duration `yaml:"dirTTL"`
	NegTTL  time.Duration `yaml:"negTTL"`
}

// CacheConfig 数据缓存配置。
type CacheConfig struct {
	Dir           string  `yaml:"dir"`
	Capacity      int64   `yaml:"capacity"`
	HighWatermark float64 `yaml:"highWatermark"`
	LowWatermark  float64 `yaml:"lowWatermark"`
	MaxFileSize   int64   `yaml:"maxFileSize"`

	// VerifyInterval 缓存内容完整性巡检周期（FUSE 层后台 goroutine 触发 DataCache.Verify）。
	// 0 表示关闭巡检。默认 0（MVP 不开启，由部署方按需打开）。
	VerifyInterval time.Duration `yaml:"verifyInterval"`
}

// PrefetchConfig 预读配置（Phase 2）。
type PrefetchConfig struct {
	Enabled     bool `yaml:"enabled"`
	Concurrency int  `yaml:"concurrency"`
	Readahead   struct {
		Initial int64 `yaml:"initial"`
		Max     int64 `yaml:"max"`
	} `yaml:"readahead"`
}

// FuseConfig FUSE 挂载配置。
type FuseConfig struct {
	MountPoint string   `yaml:"mountPoint"`
	MaxRead    int      `yaml:"maxRead"`
	Options    []string `yaml:"options"`
}

// MonitorConfig 监控配置。
type MonitorConfig struct {
	MetricsAddr string `yaml:"metricsAddr"`
	LogLevel    string `yaml:"logLevel"`
}

// Load 加载配置文件并应用环境变量覆盖。
// 优先级: 环境变量 > 配置文件 > 默认值 (命令行参数由调用方在 Load 后覆盖)。
func Load(path string) (*Config, error) {
	cfg := defaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file %s: %w", path, err)
		}
	}

	applyEnv(cfg)

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, nil
}

// applyEnv 用环境变量覆盖敏感字段。
// 兼容 OSS_ACCESS_KEY_ID/SECRET 与 FUEL_STORAGE_ACCESS_KEY/SECRET，前者优先。
func applyEnv(cfg *Config) {
	if v := firstNonEmpty(os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("FUEL_STORAGE_ACCESS_KEY")); v != "" {
		cfg.Storage.AccessKey = v
	}
	if v := firstNonEmpty(os.Getenv("OSS_ACCESS_KEY_SECRET"), os.Getenv("FUEL_STORAGE_ACCESS_SECRET")); v != "" {
		cfg.Storage.AccessSecret = v
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// validate 校验必填字段。
func (c *Config) validate() error {
	if c.Storage.Type == "" {
		return errors.New("storage.type is required")
	}
	if c.Storage.Bucket == "" {
		return errors.New("storage.bucket is required")
	}
	switch c.Storage.Type {
	case "oss":
		if c.Storage.OSS.Endpoint == "" {
			return errors.New("storage.oss.endpoint is required for oss backend")
		}
	case "s3", "minio":
		return fmt.Errorf("storage.type %q not implemented yet", c.Storage.Type)
	default:
		return fmt.Errorf("unknown storage.type %q", c.Storage.Type)
	}
	return nil
}

// defaultConfig 返回默认值配置。
func defaultConfig() *Config {
	cfg := &Config{}
	cfg.Storage.Type = "oss"
	cfg.Metadata.Engine = "direct"
	cfg.Metadata.Cache.StatTTL = 30 * time.Second
	cfg.Metadata.Cache.DirTTL = 10 * time.Second
	cfg.Metadata.Cache.NegTTL = 60 * time.Second
	cfg.Cache.Dir = "/mnt/nvme/cache"
	cfg.Cache.Capacity = 1800000000000
	cfg.Cache.HighWatermark = 0.85
	cfg.Cache.LowWatermark = 0.70
	cfg.Cache.MaxFileSize = 1073741824
	cfg.Prefetch.Enabled = true
	cfg.Prefetch.Concurrency = 4
	cfg.Prefetch.Readahead.Initial = 1048576
	cfg.Prefetch.Readahead.Max = 16777216
	cfg.Fuse.MaxRead = 1048576
	cfg.Fuse.Options = []string{"large_read", "kernel_cache", "auto_cache"}
	cfg.Monitor.MetricsAddr = ":49999"
	cfg.Monitor.LogLevel = "info"
	return cfg
}
