package fuse

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/config"
	"fuel/internal/metadata"
	"fuel/internal/objectstore"
)

// Week 10.3 故障恢复测试（无真实集群，单元级验证：功能正常仅性能降级、无 panic、无数据丢失）。
// 场景 6（BoltDB 索引持久化恢复）为 10.4 可选项，未实现，跳过。

// newEnvWithCacheDir 构造可指定缓存目录 / maxFileSize / L1 TTL 的测试环境，
// store 可注入包装器（故障注入）。
func newEnvWithCacheDir(t *testing.T, store api.ObjectStore, cacheDir string, maxFileSize int64, mc config.MetaCacheConfig, eng api.MetadataEngine) *testEnv {
	t.Helper()

	cfg := &config.Config{
		Storage: config.StorageConfig{
			Type:   "oss",
			Bucket: "test-bucket",
			OSS:    config.OSSConfig{Endpoint: "test-endpoint"},
		},
		Metadata: config.MetadataConfig{Engine: "direct", Cache: mc},
		Cache: config.CacheConfig{
			Dir:           cacheDir,
			Capacity:      1 << 30,
			HighWatermark: 0.85,
			LowWatermark:  0.70,
			MaxFileSize:   maxFileSize,
		},
	}
	dataCache, err := cache.NewNVMeCache(cacheDir, "test-bucket", cfg.Cache.Capacity, 0.85, 0.70, maxFileSize)
	if err != nil {
		t.Fatalf("NewNVMeCache failed: %v", err)
	}
	metaCache := cache.NewMetaCache(mc)
	root := NewFuelRoot(store, dataCache, metaCache, eng, cfg)
	_ = fs.NewNodeFS(root, &fs.Options{})
	return &testEnv{
		store:     store,
		dataCache: dataCache,
		metaCache: metaCache,
		root:      root,
		ctx:       context.Background(),
	}
}

// flakyStore 故障注入包装器：down 后 Head/Get 失败（模拟对象存储网络不可达）。
type flakyStore struct {
	api.ObjectStore
	down atomic.Bool
}

func (s *flakyStore) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	if s.down.Load() {
		return nil, fmt.Errorf("network unreachable")
	}
	return s.ObjectStore.Head(ctx, key)
}

func (s *flakyStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if s.down.Load() {
		return nil, fmt.Errorf("network unreachable")
	}
	return s.ObjectStore.Get(ctx, key, offset, length)
}

// 场景 1: FUSE 进程崩溃 → 重启 → 缓存索引扫描重建：空间恢复记账，
// 未知 ETag 条目按 miss 回源（INV-9），重拉后正常命中，无数据丢失。
func TestFailure_Restart_IndexRebuild(t *testing.T) {
	cacheDir := t.TempDir()
	store := objectstore.NewMockStore("test-bucket")
	data := []byte("restart-survives")

	// 实例 1：读文件使其进入缓存
	env1 := newEnvWithCacheDir(t, store, cacheDir, 0, config.MetaCacheConfig{}, newMockMetaEngine(store))
	env1.addFile("f.bin", data)
	readAndVerify(t, env1, "f.bin", data)
	// 进程崩溃：不 Unmount，直接丢弃实例

	// 实例 2：同缓存目录重启 → 扫描重建
	env2 := newEnvWithCacheDir(t, store, cacheDir, 0, config.MetaCacheConfig{}, newMockMetaEngine(store))

	// 空间记账恢复（文件参与淘汰统计，不泄漏）
	if got := env2.dataCache.Stats().UsedBytes; got != int64(len(data)) {
		t.Errorf("rebuilt usedBytes = %d, want %d", got, len(data))
	}
	// 重建条目 ETag 未知 → 不返回不可验证数据（INV-9），回源重拉后内容正确
	readAndVerify(t, env2, "f.bin", data)
	// 重拉后恢复命中
	meta := env2.head("f.bin")
	if !env2.dataCache.Contains("f.bin", meta.ETag) {
		t.Error("after refetch, cache should hit with real etag")
	}
}

// 场景 2: Redis 宕机 → L1 已热的读不经过引擎，正常工作（INV-4）。
func TestFailure_RedisDown_L1WarmRead(t *testing.T) {
	mr := miniredis.RunT(t)
	store := objectstore.NewMockStore("test-bucket")
	mc := config.MetaCacheConfig{StatTTL: time.Minute, DirTTL: time.Minute, NegTTL: time.Minute}

	cfg := &config.Config{
		Storage:  config.StorageConfig{Type: "oss", Bucket: "test-bucket", OSS: config.OSSConfig{Endpoint: "e"}},
		Metadata: config.MetadataConfig{Engine: "redis", Redis: config.RedisConfig{Address: mr.Addr()}, Cache: mc},
		Cache:    config.CacheConfig{Dir: t.TempDir(), Capacity: 1 << 30, HighWatermark: 0.85, LowWatermark: 0.70},
	}
	eng, err := metadata.NewMetadataEngine(cfg, store)
	if err != nil {
		t.Fatalf("NewMetadataEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	env := newEnvWithCacheDir(t, store, cfg.Cache.Dir, 0, mc, eng)

	data := []byte("warm-l1-data")
	env.addFile("f.txt", data)
	// 热 L1（stat）+ 数据缓存
	readAndVerify(t, env, "f.txt", data)

	mr.Close() // Redis 宕机

	// L1 命中 → 不接触 Redis → 正常读
	var out fuse.EntryOut
	if _, errno := env.root.Lookup(env.ctx, "f.txt", &out); errno != 0 {
		t.Fatalf("Lookup with redis down should hit L1, got %v", errno)
	}
	readAndVerify(t, env, "f.txt", data)
}

// 场景 3: MySQL 宕机 → 引擎降级直查对象存储（INV-4）。
// sql.Open 懒连接：坏 DSN 构造成功，查询时失败 → fallback direct（127.0.0.1:1 即时拒绝）。
func TestFailure_MySQLDown_DegradeToDirect(t *testing.T) {
	store := objectstore.NewMockStore("test-bucket")
	cfg := &config.Config{
		Storage:  config.StorageConfig{Type: "oss", Bucket: "test-bucket", OSS: config.OSSConfig{Endpoint: "e"}},
		Metadata: config.MetadataConfig{Engine: "mysql", MySQL: config.MySQLConfig{DSN: "u:p@tcp(127.0.0.1:1)/db"}},
	}
	eng, err := metadata.NewMetadataEngine(cfg, store)
	if err != nil {
		t.Fatalf("NewMetadataEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	env := newEnvWithCacheDir(t, store, t.TempDir(), 0, config.MetaCacheConfig{}, eng)

	data := []byte("mysql-down-degrade")
	env.addFile("f.txt", data)
	// L1 冷（TTL 关）→ 引擎失败 → 降级直查 → 正常工作
	readAndVerify(t, env, "f.txt", data)
}

// 场景 4: 对象存储不可达 → 已缓存数据正常读（L1 热 + 数据缓存命中）。
func TestFailure_StoreDown_CachedReadOK(t *testing.T) {
	store := &flakyStore{ObjectStore: objectstore.NewMockStore("test-bucket")}
	mc := config.MetaCacheConfig{StatTTL: time.Minute, DirTTL: time.Minute, NegTTL: time.Minute}
	env := newEnvWithCacheDir(t, store, t.TempDir(), 0, mc, newMockMetaEngine(store))

	data := []byte("cached-content")
	env.addFile("cached.txt", data)
	readAndVerify(t, env, "cached.txt", data) // 热 L1 + 数据缓存

	store.down.Store(true) // 对象存储网络不可达

	var out fuse.EntryOut
	if _, errno := env.root.Lookup(env.ctx, "cached.txt", &out); errno != 0 {
		t.Fatalf("Lookup cached file with store down should hit L1, got %v", errno)
	}
	readAndVerify(t, env, "cached.txt", data)
}

// 场景 4b: 对象存储不可达 → 未缓存（L1 无元数据）的文件返回 EIO（非 panic）。
func TestFailure_StoreDown_UncachedEIO(t *testing.T) {
	store := &flakyStore{ObjectStore: objectstore.NewMockStore("test-bucket")}
	env := newEnvWithCacheDir(t, store, t.TempDir(), 0, config.MetaCacheConfig{}, newMockMetaEngine(store))
	env.addFile("f.txt", []byte("x"))

	store.down.Store(true)

	var out fuse.EntryOut
	_, errno := env.root.Lookup(env.ctx, "f.txt", &out)
	if errno != syscall.EIO {
		t.Errorf("Lookup uncached file with store down = %v, want EIO (不可达≠不存在)", errno)
	}
}

// 场景 5: 缓存写拒绝（文件超过 maxFileSize）→ readThrough 降级直读，
// 内容正确、不缓存、无 panic（GOAL-7 仅性能降级；同时修复 PLAN §11 D7 大文件不可读）。
func TestFailure_CacheRejected_ReadThrough(t *testing.T) {
	store := objectstore.NewMockStore("test-bucket")
	// maxFileSize=4，文件 10 字节 → Put 必拒绝
	env := newEnvWithCacheDir(t, store, t.TempDir(), 4, config.MetaCacheConfig{}, newMockMetaEngine(store))

	data := []byte("0123456789")
	env.addFile("big.bin", data)

	var out fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, "big.bin", &out)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)
	fh, _, errno := node.Open(env.ctx, 0)
	if errno != 0 {
		t.Fatalf("Open failed: %v", errno)
	}
	defer fh.(fs.FileReleaser).Release(env.ctx)

	// 两次读：第一次触发降级，第二次走 degraded 粘性路径
	dest := make([]byte, 6)
	res, errno := node.Read(env.ctx, fh, dest, 0)
	if errno != 0 {
		t.Fatalf("read 0 failed: %v", errno)
	}
	got, _ := res.Bytes(nil)
	if string(got) != "012345" {
		t.Errorf("read 0 = %q, want %q", got, "012345")
	}

	res, errno = node.Read(env.ctx, fh, dest[:4], 6)
	if errno != 0 {
		t.Fatalf("read 1 failed: %v", errno)
	}
	got, _ = res.Bytes(nil)
	if string(got) != "6789" {
		t.Errorf("read 1 = %q, want %q", got, "6789")
	}

	// 未缓存（降级不写缓存）
	if n := env.dataCache.Stats().EntryCount; n != 0 {
		t.Errorf("read-through should not cache, entryCount = %d", n)
	}
}
