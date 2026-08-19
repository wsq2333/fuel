package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"fuel/internal/cache"
	"fuel/internal/config"
	"fuel/internal/metadata"
	"fuel/internal/objectstore"
)

// 8.3 元数据引擎模式切换验证。
//
// MySQL 引擎的契约行为由 internal/metadata 包的 sqlmock 测试覆盖（工厂不支持
// 注入 *sql.DB，无法在此构造）；此处验证 direct / redis 两种模式经工厂切换后
// FUSE 行为一致，以及引擎不可用时自动降级直查（INV-4）。

// newEngineEnv 用指定元数据引擎构造测试环境（L1 全关，保证走引擎路径）。
func newEngineEnv(t *testing.T, engine string, redisAddr string) *testEnv {
	t.Helper()

	store := objectstore.NewMockStore("test-bucket")
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Type:   "oss",
			Bucket: "test-bucket",
			OSS:    config.OSSConfig{Endpoint: "test-endpoint"},
		},
		Metadata: config.MetadataConfig{
			Engine: engine,
			Redis:  config.RedisConfig{Address: redisAddr},
			Cache:  config.MetaCacheConfig{},
		},
		Cache: config.CacheConfig{
			Dir:           t.TempDir(),
			Capacity:      1 << 30,
			HighWatermark: 0.85,
			LowWatermark:  0.70,
		},
	}
	metaEng, err := metadata.NewMetadataEngine(cfg, store)
	if err != nil {
		t.Fatalf("NewMetadataEngine(%s) failed: %v", engine, err)
	}
	t.Cleanup(func() { _ = metaEng.Close() })
	dataCache, err := cache.NewNVMeCache(cfg.Cache.Dir, "test-bucket", cfg.Cache.Capacity, 0.85, 0.70, 0)
	if err != nil {
		t.Fatalf("NewNVMeCache failed: %v", err)
	}
	root := NewFuelRoot(store, dataCache, cache.NewMetaCache(cfg.Metadata.Cache), metaEng, cfg)
	_ = fs.NewNodeFS(root, &fs.Options{})
	return &testEnv{
		store:     store,
		dataCache: dataCache,
		metaCache: cache.NewMetaCache(config.MetaCacheConfig{}),
		root:      root,
		ctx:       context.Background(),
	}
}

// exerciseEngine 对给定环境执行一组写+读+删操作，返回各步骤结果码。
// 用于跨引擎对比行为一致性。
func exerciseEngine(t *testing.T, env *testEnv) (readBack []byte, lsAfterMkdir bool, lookupAfterUnlink syscall.Errno) {
	t.Helper()

	writeFile(t, env, "sw/f.txt", []byte("mode-switch"))
	readBack, err := readFile(env, "sw/f.txt")
	if err != nil {
		t.Fatalf("readFile failed: %v", err)
	}

	var out fuse.EntryOut
	if _, errno := env.root.Mkdir(env.ctx, "sw/sub", 0o755, &out); errno != 0 {
		t.Fatalf("Mkdir failed: %v", errno)
	}
	stream, errno := env.root.Readdir(env.ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %v", errno)
	}
	for stream.HasNext() {
		if e, _ := stream.Next(); e.Name == "sw" {
			lsAfterMkdir = true
		}
	}

	if errno := env.root.Unlink(env.ctx, "sw/f.txt"); errno != 0 {
		t.Fatalf("Unlink failed: %v", errno)
	}
	_, lookupAfterUnlink = env.root.Lookup(env.ctx, "sw/f.txt", &out)
	return readBack, lsAfterMkdir, lookupAfterUnlink
}

// TestModeSwitch_DirectRedis_IdenticalBehavior 模式 A(direct) 与模式 B(redis)
// 经同一写读删流程产生一致结果。
func TestModeSwitch_DirectRedis_IdenticalBehavior(t *testing.T) {
	mr := miniredis.RunT(t)

	directEnv := newEngineEnv(t, "direct", "")
	redisEnv := newEngineEnv(t, "redis", mr.Addr())

	dRead, dLs, dUnlink := exerciseEngine(t, directEnv)
	rRead, rLs, rUnlink := exerciseEngine(t, redisEnv)

	if string(dRead) != string(rRead) {
		t.Errorf("read back differs: direct=%q redis=%q", dRead, rRead)
	}
	if dLs != rLs {
		t.Errorf("readdir visibility differs: direct=%v redis=%v", dLs, rLs)
	}
	if dUnlink != rUnlink {
		t.Errorf("unlink consistency differs: direct=%v redis=%v", dUnlink, rUnlink)
	}
	if rUnlink != syscall.ENOENT {
		t.Errorf("Lookup after Unlink = %v, want ENOENT", rUnlink)
	}
}

// TestModeSwitch_RedisDown_DegradesToDirect Redis 不可用时读/写路径自动降级
// 直查对象存储（INV-4），FUSE 行为不受影响。
// 使用拒绝连线的端口模拟不可用。注意：每次元数据操作需等待 go-redis
// 重试耗尽（连接池 5 次 dial × MaxRetries=3，约 1s/次）才降级——生产上
// 降级期间有明显延迟，靠 HealthCheck 探测 + 上层切引擎规避（见 PLAN §11 D10）。
// 因此本测试只跑最少操作（一读一写）。
func TestModeSwitch_RedisDown_DegradesToDirect(t *testing.T) {
	env := newEngineEnv(t, "redis", "127.0.0.1:1")

	// 读路径降级直查（redis 操作失败 → directEngine fallback）
	env.addFile("f.txt", []byte("readable"))
	readAndVerify(t, env, "f.txt", []byte("readable"))

	// 写路径仍可用（Invalidate 失败仅降级日志，不影响上传）
	var out fuse.EntryOut
	if _, errno := env.root.Mkdir(env.ctx, "d", 0o755, &out); errno != 0 {
		t.Errorf("Mkdir with redis down failed: %v", errno)
	}
}
