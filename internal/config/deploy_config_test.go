package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestDeployConfigMap_EmbeddedConfigLoads 校验 deploy/k8s/configmap.yaml 内嵌的
// fuel-config.yaml 能被 Load 解析并通过校验（部署清单与配置结构体的静态契约测试，
// 防止 YAML 字段漂移导致 DaemonSet 启动即失败）。
func TestDeployConfigMap_EmbeddedConfigLoads(t *testing.T) {
	cmPath := filepath.Join("..", "..", "deploy", "k8s", "configmap.yaml")
	raw, err := os.ReadFile(cmPath)
	if err != nil {
		t.Skipf("deploy/k8s/configmap.yaml not found: %v", err)
	}

	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(raw, &cm); err != nil {
		t.Fatalf("parse configmap yaml: %v", err)
	}
	embedded, ok := cm.Data["config.yaml"]
	if !ok || embedded == "" {
		t.Fatal("configmap missing data[config.yaml]")
	}

	tmp := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmp, []byte(embedded), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := Load(tmp)
	if err != nil {
		t.Fatalf("embedded config should load cleanly: %v", err)
	}

	if cfg.Monitor.MetricsAddr != ":49999" {
		t.Errorf("monitor.metricsAddr = %q, want :49999（DaemonSet 探针/抓取端口）", cfg.Monitor.MetricsAddr)
	}
	if cfg.Cache.Dir != "/mnt/nvme/fuel" {
		t.Errorf("cache.dir = %q, want /mnt/nvme/fuel（与 DaemonSet hostPath 一致）", cfg.Cache.Dir)
	}
	if cfg.Fuse.MountPoint != "/fuel" {
		t.Errorf("fuse.mountPoint = %q, want /fuel（与 DaemonSet Bidirectional hostPath 一致）", cfg.Fuse.MountPoint)
	}
	if cfg.Metadata.Cache.StatTTL != 30*time.Second {
		t.Errorf("statTTL = %v, want 30s", cfg.Metadata.Cache.StatTTL)
	}
}
