package monitor

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/config"
	"fuel/internal/objectstore"
)

// histogramCount 便利函数：读取 HistogramVec 单个 label 的样本计数。
func histogramCount(t *testing.T, vec *prometheus.HistogramVec, label string) uint64 {
	t.Helper()
	h, ok := vec.WithLabelValues(label).(prometheus.Histogram)
	if !ok {
		t.Fatalf("metric %s is not a histogram", label)
	}
	return histogramCountSingle(t, h)
}

// histogramCountSingle 读取单个直方图的样本计数。
func histogramCountSingle(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("write metric failed: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestInstrumentStore 验证各操作计数与延迟观察。
// 注意：包级 promauto 指标在测试进程内共享，断言用前后增量（其他用例可能已计数）。
func TestInstrumentStore(t *testing.T) {
	inner := objectstore.NewMockStore("b")
	store := InstrumentStore(inner)
	ctx := context.Background()

	ops := []string{"put", "head", "get", "list", "copy", "delete"}
	before := make(map[string]float64)
	beforeDur := make(map[string]uint64)
	for _, op := range ops {
		before[op] = testutil.ToFloat64(storageRequests.WithLabelValues(op))
		beforeDur[op] = histogramCount(t, storageDuration, op)
	}

	_, _ = store.Put(ctx, "a", bytes.NewReader([]byte("x")), 1)
	_, _ = store.Head(ctx, "a")
	_, _ = store.Get(ctx, "a", 0, 0)
	_, _, _ = store.List(ctx, "", "/", 0)
	_ = store.Copy(ctx, "a", "b")
	_ = store.Delete(ctx, "a")

	for _, op := range ops {
		if got := testutil.ToFloat64(storageRequests.WithLabelValues(op)) - before[op]; got != 1 {
			t.Errorf("storage_requests_total{operation=%q} delta = %v, want 1", op, got)
		}
		if got := histogramCount(t, storageDuration, op) - beforeDur[op]; got != 1 {
			t.Errorf("storage_request_duration{operation=%q} count delta = %d, want 1", op, got)
		}
	}
	if got := store.Bucket(); got != "b" {
		t.Errorf("Bucket = %q, want b", got)
	}
}

// TestFuelCollector 验证缓存/元数据指标采集值与命名。
func TestFuelCollector(t *testing.T) {
	dc, err := cache.NewNVMeCache(t.TempDir(), "b", 1<<20, 0.85, 0.70, 0)
	if err != nil {
		t.Fatalf("NewNVMeCache failed: %v", err)
	}
	mc := cache.NewMetaCache(config.MetaCacheConfig{
		StatTTL: time.Minute, DirTTL: time.Minute, NegTTL: time.Minute,
	})

	data := []byte("hello")
	if _, hit, err := dc.Get("f", "e"); err != nil || hit {
		t.Fatalf("Get: err=%v hit=%v, want miss", err, hit)
	}
	if _, err := dc.Put("f", "e", int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, hit, _ := dc.Get("f", "e"); !hit {
		t.Fatal("expect cache hit")
	}

	me := &api.MetaEntry{Path: "f", ETag: "e", Size: 5}
	mc.SetStat("f", me)
	if _, ok := mc.GetStat("f"); !ok {
		t.Fatal("expect stat hit")
	}
	if _, ok := mc.GetStat("missing"); ok {
		t.Fatal("expect stat miss")
	}

	col := NewFuelCollector(dc, mc)
	reg := prometheus.NewRegistry()
	reg.MustRegister(col)

	want := map[string]float64{
		"fuel_cache_hit_total":         1,
		"fuel_cache_miss_total":        1,
		"fuel_cache_size_bytes":        5,
		"fuel_cache_capacity_bytes":    1 << 20,
		"fuel_cache_eviction_total":    0,
		"fuel_cache_entries":           1,
		"fuel_meta_hit_total":          1,
		"fuel_meta_miss_total":         1,
		"fuel_neg_cache_hit_total":     0,
		"fuel_process_goroutine_count": float64(1), // >=1
		"fuel_process_memory_bytes":    float64(1), // >0
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	values := make(map[string]float64)
	for _, mf := range mfs {
		if len(mf.GetMetric()) == 0 {
			continue
		}
		values[mf.GetName()] = mf.GetMetric()[0].GetGauge().GetValue() +
			mf.GetMetric()[0].GetCounter().GetValue() +
			mf.GetMetric()[0].GetSummary().GetSampleSum()
	}
	for name, w := range want {
		got, ok := values[name]
		if !ok {
			t.Errorf("metric %s missing", name)
			continue
		}
		if name == "fuel_process_goroutine_count" || name == "fuel_process_memory_bytes" {
			if got < w {
				t.Errorf("metric %s = %v, want >= %v", name, got, w)
			}
			continue
		}
		if got != w {
			t.Errorf("metric %s = %v, want %v", name, got, w)
		}
	}
	if values["fuel_meta_hit_total"] != 1 {
		t.Errorf("meta_hit = %v, want 1 (stat hits + dir hits)", values["fuel_meta_hit_total"])
	}
}

// TestFuelCollector_NilSources nil 依赖时不采集缓存指标也不 panic。
func TestFuelCollector_NilSources(t *testing.T) {
	col := NewFuelCollector(nil, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(col)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	if names["fuel_cache_hit_total"] || names["fuel_meta_miss_total"] {
		t.Error("cache/meta metrics should be skipped with nil sources")
	}
	if !names["fuel_process_memory_bytes"] {
		t.Error("process metrics should always be present")
	}
}

// TestFuseAndPrefetchCounters 验证 FUSE 操作计数与预取计数 API。
// 断言用前后增量（包级指标在测试进程内共享）。
func TestFuseAndPrefetchCounters(t *testing.T) {
	beforeRead := testutil.ToFloat64(fuseOps.WithLabelValues("read"))
	beforeTotal := testutil.ToFloat64(prefetchTotal)
	beforeBytes := testutil.ToFloat64(prefetchBytes)
	beforeDur := histogramCountSingle(t, fuseReadDuration)

	IncFuseOp("lookup")
	IncFuseOp("read")
	ObserveFuseRead(2 * time.Millisecond)
	ObserveBatchPrefetch(3, 1024)
	ObserveBatchPrefetch(0, 0) // 无副作用

	if got := testutil.ToFloat64(fuseOps.WithLabelValues("read")) - beforeRead; got != 1 {
		t.Errorf("fuse_operations_total{op=read} delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(prefetchTotal) - beforeTotal; got != 3 {
		t.Errorf("prefetch_total delta = %v, want 3", got)
	}
	if got := testutil.ToFloat64(prefetchBytes) - beforeBytes; got != 1024 {
		t.Errorf("prefetch_bytes_total delta = %v, want 1024", got)
	}
	if got := histogramCountSingle(t, fuseReadDuration) - beforeDur; got != 1 {
		t.Errorf("fuse read duration count delta = %d, want 1", got)
	}
}
