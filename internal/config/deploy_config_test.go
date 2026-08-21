package config

import (
	"os"
	"path/filepath"
	"strings"
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

// --- Week 10：部署清单与代码的静态契约测试（无真实集群） ---

// loadYAMLDocs 解析多文档 YAML 为通用 map 列表。
func loadYAMLDocs(t *testing.T, rel string) []map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Skipf("%s not found: %v", rel, err)
	}
	var docs []map[string]interface{}
	for _, part := range splitYAMLDocs(string(raw)) {
		var doc map[string]interface{}
		if err := yaml.Unmarshal([]byte(part), &doc); err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		if doc != nil && doc["kind"] != nil {
			docs = append(docs, doc)
		}
	}
	return docs
}

func splitYAMLDocs(s string) []string {
	var out []string
	cur := ""
	for _, line := range strings.Split(s, "\n") {
		if line == "---" {
			if strings.TrimSpace(cur) != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += line + "\n"
	}
	if strings.TrimSpace(cur) != "" {
		out = append(out, cur)
	}
	return out
}

func findDoc(t *testing.T, docs []map[string]interface{}, kind string) map[string]interface{} {
	t.Helper()
	for _, d := range docs {
		if d["kind"] == kind {
			return d
		}
	}
	t.Fatalf("kind %s not found", kind)
	return nil
}

// dig 按点分路径取值（仅 map 嵌套）。
func dig(m map[string]interface{}, path string) interface{} {
	var cur interface{} = m
	for _, p := range strings.Split(path, ".") {
		mm, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		cur = mm[p]
	}
	return cur
}

// TestDeploy_DaemonSetContract 校验 DaemonSet 与代码/配置的契约一致性。
func TestDeploy_DaemonSetContract(t *testing.T) {
	ds := findDoc(t, loadYAMLDocs(t, "deploy/k8s/daemonset.yaml"), "DaemonSet")

	// 1. Pod 标签（ServiceMonitor/Service 选择器依赖）
	labels := dig(ds, "spec.template.metadata.labels").(map[string]interface{})
	if labels["app"] != "fuel" {
		t.Errorf("pod label app = %v, want fuel", labels["app"])
	}

	// 2. Prometheus 抓取注解（monitoring.yaml 方式 B 的发现机制）
	ann := dig(ds, "spec.template.metadata.annotations").(map[string]interface{})
	if ann["prometheus.io/scrape"] != "true" || ann["prometheus.io/port"] != "49999" || ann["prometheus.io/path"] != "/metrics" {
		t.Errorf("prometheus annotations incomplete: %v", ann)
	}

	// 3. 容器配置：privileged、metrics 端口、探针路径
	containers := dig(ds, "spec.template.spec.containers").([]interface{})
	c := containers[0].(map[string]interface{})
	if dig(c, "securityContext.privileged") != true {
		t.Error("container must be privileged (FUSE /dev/fuse)")
	}
	ports := c["ports"].([]interface{})
	p0 := ports[0].(map[string]interface{})
	if p0["containerPort"] != 49999 || p0["name"] != "metrics" {
		t.Errorf("metrics port = %v, want 49999 named metrics", p0)
	}
	// 探针必须用 /livez（PLAN §11 D12：/health 依赖不可用时 503，会导致 kubelet 重启）
	for _, probe := range []string{"livenessProbe", "readinessProbe"} {
		path := dig(c, probe+".httpGet.path")
		if path != "/livez" {
			t.Errorf("%s path = %v, want /livez", probe, path)
		}
	}

	// 4. 挂载路径与 configmap 内嵌配置一致（INV-2 / ARCH_SPEC §6）
	mounts := c["volumeMounts"].([]interface{})
	var mountPoint, cacheDir string
	for _, m := range mounts {
		mm := m.(map[string]interface{})
		switch mm["name"] {
		case "mountpoint":
			mountPoint = mm["mountPath"].(string)
			if mm["mountPropagation"] != "Bidirectional" {
				t.Errorf("mountpoint mountPropagation = %v, want Bidirectional", mm["mountPropagation"])
			}
		case "cache":
			cacheDir = mm["mountPath"].(string)
		}
	}
	if mountPoint != "/fuel" || cacheDir != "/mnt/nvme/fuel" {
		t.Errorf("mount paths = %q/%q, want /fuel + /mnt/nvme/fuel", mountPoint, cacheDir)
	}
}

// TestDeploy_MonitoringContract 校验监控清单与 DaemonSet 的选择器/端口契约。
func TestDeploy_MonitoringContract(t *testing.T) {
	docs := loadYAMLDocs(t, "deploy/k8s/monitoring.yaml")

	svc := findDoc(t, docs, "Service")
	sel := dig(svc, "spec.selector").(map[string]interface{})
	if sel["app"] != "fuel" {
		t.Errorf("Service selector app = %v, want fuel (匹配 DaemonSet Pod 标签)", sel["app"])
	}
	port := dig(svc, "spec.ports").([]interface{})[0].(map[string]interface{})
	if port["port"] != 49999 {
		t.Errorf("Service port = %v, want 49999", port["port"])
	}

	sm := findDoc(t, docs, "ServiceMonitor")
	smSel := dig(sm, "spec.selector.matchLabels").(map[string]interface{})
	if smSel["app"] != "fuel" {
		t.Errorf("ServiceMonitor selector app = %v, want fuel (匹配 Service 标签)", smSel["app"])
	}
}

// TestDeploy_SecretContract 校验 Secret 键名与代码读取的环境变量契约。
func TestDeploy_SecretContract(t *testing.T) {
	secret := findDoc(t, loadYAMLDocs(t, "deploy/k8s/secret.yaml"), "Secret")
	keys := dig(secret, "stringData").(map[string]interface{})
	if _, ok := keys["accessKey"]; !ok {
		t.Error("secret missing accessKey")
	}
	if _, ok := keys["accessSecret"]; !ok {
		t.Error("secret missing accessSecret")
	}

	// DaemonSet 引用的 secret 键名与 secret.yaml 一致，且映射到代码读取的
	// OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET（applyEnv 兼容名）
	ds := findDoc(t, loadYAMLDocs(t, "deploy/k8s/daemonset.yaml"), "DaemonSet")
	env := dig(ds, "spec.template.spec.containers").([]interface{})[0].(map[string]interface{})["env"].([]interface{})
	envMap := make(map[string]string)
	for _, e := range env {
		ee := e.(map[string]interface{})
		envMap[ee["name"].(string)] = dig(ee, "valueFrom.secretKeyRef.key").(string)
	}
	if envMap["OSS_ACCESS_KEY_ID"] != "accessKey" || envMap["OSS_ACCESS_KEY_SECRET"] != "accessSecret" {
		t.Errorf("env mapping = %v, want OSS_ACCESS_KEY_ID→accessKey / OSS_ACCESS_KEY_SECRET→accessSecret", envMap)
	}
}

// TestDeploy_SystemdServiceContract 校验 systemd 单元的关键字段。
func TestDeploy_SystemdServiceContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "systemd", "fuel.service"))
	if err != nil {
		t.Skipf("fuel.service not found: %v", err)
	}
	content := string(raw)
	for _, want := range []string{
		"[Unit]", "[Service]", "[Install]",
		"ExecStart=/usr/local/bin/fuel mount --config /etc/fuel/config.yaml",
		"Restart=always",
		"KillSignal=SIGINT",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("fuel.service missing %q", want)
		}
	}
}
