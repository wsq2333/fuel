// Package monitor 提供 Prometheus 指标定义、采集与健康检查端点 (PLAN §9.1, ARCH_SPEC §9)。
package monitor

import (
	"context"
	"io"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"fuel/api"
	"fuel/internal/cache"
)

// 指标命名遵循 ARCH_SPEC §9.1（fuel_ 前缀）。
// 与 ARCH_SPEC 的差异：storage_ 子系统替代 oss_ 前缀（INV-8 后端中立，
// 多后端落地时再加 backend label）；fuel_meta_hit_total 当前仅暴露 layer="l1"
// （L2 引擎无命中计数器，见 PLAN §11 D11）。

var (
	storageRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "fuel", Subsystem: "storage",
		Name: "requests_total", Help: "Object storage requests by operation.",
	}, []string{"operation"})
	storageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "fuel", Subsystem: "storage",
		Name:    "request_duration_seconds",
		Help:    "Object storage request latency by operation.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms ~ 20s
	}, []string{"operation"})

	fuseOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "fuel", Subsystem: "fuse",
		Name: "operations_total", Help: "FUSE operations by type.",
	}, []string{"op"})
	fuseReadDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "fuel", Subsystem: "fuse",
		Name:    "read_duration_seconds",
		Help:    "FUSE read latency (cache hit pread or miss fetch).",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1ms ~ 1.6s
	})

	prefetchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "fuel",
		Name:      "prefetch_total", Help: "Files fetched by batch prefetch.",
	})
	prefetchBytes = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "fuel",
		Name:      "prefetch_bytes_total", Help: "Bytes fetched by batch prefetch.",
	})
)

// IncFuseOp 记录一次 FUSE 操作（op: lookup/getattr/readdir/open/read/create/...）。
func IncFuseOp(op string) {
	fuseOps.WithLabelValues(op).Inc()
}

// ObserveFuseRead 记录一次 FUSE Read 的端到端耗时。
func ObserveFuseRead(d time.Duration) {
	fuseReadDuration.Observe(d.Seconds())
}

// ObserveBatchPrefetch 记录批量预取实际拉取的文件数与字节数。
func ObserveBatchPrefetch(files int, bytes int64) {
	if files <= 0 {
		return
	}
	prefetchTotal.Add(float64(files))
	prefetchBytes.Add(float64(bytes))
}

// FuelCollector 在 Prometheus scrape 时从缓存层实时拉取计数（避免双写不一致）。
// 采集项：数据缓存命中/未命中/容量/淘汰、L1 元数据命中/未命中、负缓存命中、
// 进程内存与 goroutine 数（fuel_ 前缀，ARCH_SPEC §9.1）。
type FuelCollector struct {
	data api.DataCache
	meta cache.MetaCache

	cacheHit     *prometheus.Desc
	cacheMiss    *prometheus.Desc
	cacheSize    *prometheus.Desc
	cacheCap     *prometheus.Desc
	cacheEvict   *prometheus.Desc
	cacheEntries *prometheus.Desc
	metaHit      *prometheus.Desc
	metaMiss     *prometheus.Desc
	negHit       *prometheus.Desc
	procMem      *prometheus.Desc
	procGor      *prometheus.Desc
}

func newDesc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc("fuel_"+name, help, labels, nil)
}

// NewFuelCollector 构造采集器。data/meta 可为 nil（对应指标跳过）。
func NewFuelCollector(data api.DataCache, meta cache.MetaCache) *FuelCollector {
	return &FuelCollector{
		data:         data,
		meta:         meta,
		cacheHit:     newDesc("cache_hit_total", "Data cache hits.", "type"),
		cacheMiss:    newDesc("cache_miss_total", "Data cache misses.", "type"),
		cacheSize:    newDesc("cache_size_bytes", "Data cache used bytes."),
		cacheCap:     newDesc("cache_capacity_bytes", "Data cache capacity bytes."),
		cacheEvict:   newDesc("cache_eviction_total", "Data cache LRU evictions."),
		cacheEntries: newDesc("cache_entries", "Data cache entry count."),
		metaHit:      newDesc("meta_hit_total", "Metadata cache hits.", "layer"),
		metaMiss:     newDesc("meta_miss_total", "Metadata cache misses (L1)."),
		negHit:       newDesc("neg_cache_hit_total", "Negative cache hits."),
		procMem:      newDesc("process_memory_bytes", "Process heap in-use bytes."),
		procGor:      newDesc("process_goroutine_count", "Goroutine count."),
	}
}

func (c *FuelCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.cacheHit, c.cacheMiss, c.cacheSize, c.cacheCap, c.cacheEvict, c.cacheEntries,
		c.metaHit, c.metaMiss, c.negHit, c.procMem, c.procGor,
	} {
		ch <- d
	}
}

func (c *FuelCollector) Collect(ch chan<- prometheus.Metric) {
	if c.data != nil {
		s := c.data.Stats()
		ch <- prometheus.MustNewConstMetric(c.cacheHit, prometheus.CounterValue, float64(s.HitCount), "data")
		ch <- prometheus.MustNewConstMetric(c.cacheMiss, prometheus.CounterValue, float64(s.MissCount), "data")
		ch <- prometheus.MustNewConstMetric(c.cacheSize, prometheus.GaugeValue, float64(s.UsedBytes))
		ch <- prometheus.MustNewConstMetric(c.cacheCap, prometheus.GaugeValue, float64(s.CapacityBytes))
		ch <- prometheus.MustNewConstMetric(c.cacheEvict, prometheus.CounterValue, float64(s.EvictionCount))
		ch <- prometheus.MustNewConstMetric(c.cacheEntries, prometheus.GaugeValue, float64(s.EntryCount))
	}
	if c.meta != nil {
		s := c.meta.Stats()
		ch <- prometheus.MustNewConstMetric(c.metaHit, prometheus.CounterValue, float64(s.StatHits+s.DirHits), "l1")
		ch <- prometheus.MustNewConstMetric(c.metaMiss, prometheus.CounterValue, float64(s.StatMisses+s.DirMisses))
		ch <- prometheus.MustNewConstMetric(c.negHit, prometheus.CounterValue, float64(s.NegHits))
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	ch <- prometheus.MustNewConstMetric(c.procMem, prometheus.GaugeValue, float64(mem.HeapInuse))
	ch <- prometheus.MustNewConstMetric(c.procGor, prometheus.GaugeValue, float64(runtime.NumGoroutine()))
}

// --- 对象存储插桩 ---

// instrumentedStore 以装饰器包装 ObjectStore，统计各操作次数与延迟。
// 对调用方透明（INV-7/8：无后端类型分支），不改动各后端实现。
type instrumentedStore struct {
	inner api.ObjectStore
}

// InstrumentStore 返回带指标的 ObjectStore 包装。
func InstrumentStore(inner api.ObjectStore) api.ObjectStore {
	return &instrumentedStore{inner: inner}
}

func (s *instrumentedStore) observe(op string, start time.Time) {
	storageRequests.WithLabelValues(op).Inc()
	storageDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

func (s *instrumentedStore) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	defer s.observe("head", time.Now())
	return s.inner.Head(ctx, key)
}

func (s *instrumentedStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	defer s.observe("get", time.Now())
	return s.inner.Get(ctx, key, offset, length)
}

func (s *instrumentedStore) Put(ctx context.Context, key string, r io.Reader, size int64) (*api.ObjectMeta, error) {
	defer s.observe("put", time.Now())
	return s.inner.Put(ctx, key, r, size)
}

func (s *instrumentedStore) List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]api.ObjectEntry, []string, error) {
	defer s.observe("list", time.Now())
	return s.inner.List(ctx, prefix, delimiter, maxKeys)
}

func (s *instrumentedStore) Copy(ctx context.Context, srcKey, dstKey string) error {
	defer s.observe("copy", time.Now())
	return s.inner.Copy(ctx, srcKey, dstKey)
}

func (s *instrumentedStore) Delete(ctx context.Context, key string) error {
	defer s.observe("delete", time.Now())
	return s.inner.Delete(ctx, key)
}

func (s *instrumentedStore) Bucket() string {
	return s.inner.Bucket()
}
