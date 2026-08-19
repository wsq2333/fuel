package fuse

import (
	"context"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"fuel/internal/cache"
	"fuel/internal/config"
	"fuel/internal/metadata"
	"fuel/internal/objectstore"
)

// --- 写路径测试辅助 ---

// writeFile 经 Create → Write → Flush → Release 完整写入一个文件。
func writeFile(t *testing.T, env *testEnv, path string, data []byte) {
	t.Helper()

	var out fuse.EntryOut
	inode, fh, _, errno := env.root.Create(env.ctx, path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC, 0o644, &out)
	if errno != 0 {
		t.Fatalf("Create %s failed: %v", path, errno)
	}
	node := inode.Operations().(*FuelNode)
	if len(data) > 0 {
		n, errno := node.Write(env.ctx, fh, data, 0)
		if errno != 0 {
			t.Fatalf("Write %s failed: %v", path, errno)
		}
		if int(n) != len(data) {
			t.Fatalf("Write %s = %d bytes, want %d", path, n, len(data))
		}
	}
	if errno := node.Flush(env.ctx, fh); errno != 0 {
		t.Fatalf("Flush %s failed: %v", path, errno)
	}
	if errno := fh.(fs.FileReleaser).Release(env.ctx); errno != 0 {
		t.Fatalf("Release %s failed: %v", path, errno)
	}
}

// readdirNames 返回根目录下所有子项名称。
func readdirNames(t *testing.T, env *testEnv) []string {
	t.Helper()
	stream, errno := env.root.Readdir(env.ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %v", errno)
	}
	var names []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		names = append(names, entry.Name)
	}
	return names
}

func containsName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// --- 8.2 写后读一致性场景 ---

// 场景 1: 写文件 → 同节点立即读 → 读到新数据（字节镜像 INV-3）。
func TestWrite_Scenario1_WriteThenReadSameNode(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("hello fuel write path")
	writeFile(t, env, "new.txt", data)

	readAndVerify(t, env, "new.txt", data)

	// 对象存储中的内容与写入完全一致（INV-3：与直接 SDK 上传等价）
	reader, err := env.store.Get(env.ctx, "new.txt", 0, 0)
	if err != nil {
		t.Fatalf("store.Get failed: %v", err)
	}
	raw, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(raw) != string(data) {
		t.Errorf("store content = %q, want %q (INV-3 byte mirror)", raw, data)
	}
}

// 场景 2 + 4: 覆盖写 → L1 有效期内读（强一致）→ L1 TTL 过期后读 → 均为新数据。
func TestWrite_Scenario2And4_OverwriteConsistency(t *testing.T) {
	env := newTestEnvWithMetaCache(t, config.MetaCacheConfig{
		StatTTL: 80 * time.Millisecond,
		DirTTL:  80 * time.Millisecond,
		NegTTL:  80 * time.Millisecond,
	})

	writeFile(t, env, "f.txt", []byte("version-one"))
	readAndVerify(t, env, "f.txt", []byte("version-one"))
	etagV1 := env.head("f.txt").ETag

	// 覆盖写（Create + O_TRUNC）
	writeFile(t, env, "f.txt", []byte("version-two!!"))
	etagV2 := env.head("f.txt").ETag
	if etagV1 == etagV2 {
		t.Fatal("precondition: overwrite should change etag")
	}

	// L1 有效期内：Flush 已 SetStat 新元数据 + 数据缓存已更新 → 强一致读 V2
	readAndVerify(t, env, "f.txt", []byte("version-two!!"))
	if env.dataCache.Contains("f.txt", etagV1) {
		t.Error("stale V1 cache entry should be invalidated after overwrite")
	}

	// L1 TTL 过期后：回源仍是 V2
	time.Sleep(100 * time.Millisecond)
	readAndVerify(t, env, "f.txt", []byte("version-two!!"))
}

// 场景 3: 跨节点写后读（模式 B Redis）→ 节点 A 写入后节点 B 读到新数据（L2 共享）。
func TestWrite_Scenario3_CrossNodeViaRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	// 两节点挂载同一 bucket：共享对象存储，各自独立的 L1/L2 客户端/数据缓存
	sharedStore := objectstore.NewMockStore("test-bucket")

	newRedisEnv := func(t *testing.T) *testEnv {
		cfg := &config.Config{
			Storage: config.StorageConfig{
				Type:   "oss",
				Bucket: "test-bucket",
				OSS:    config.OSSConfig{Endpoint: "test-endpoint"},
			},
			Metadata: config.MetadataConfig{
				Engine: "redis",
				Redis:  config.RedisConfig{Address: mr.Addr()},
				Cache:  config.MetaCacheConfig{}, // L1 全关，只走 L2
			},
			Cache: config.CacheConfig{
				Dir:           t.TempDir(),
				Capacity:      1 << 30,
				HighWatermark: 0.85,
				LowWatermark:  0.70,
			},
		}
		metaEng, err := metadata.NewMetadataEngine(cfg, sharedStore)
		if err != nil {
			t.Fatalf("NewMetadataEngine failed: %v", err)
		}
		t.Cleanup(func() { _ = metaEng.Close() })
		dataCache, err := cache.NewNVMeCache(cfg.Cache.Dir, "test-bucket", cfg.Cache.Capacity, 0.85, 0.70, 0)
		if err != nil {
			t.Fatalf("NewNVMeCache failed: %v", err)
		}
		metaCache := cache.NewMetaCache(cfg.Metadata.Cache)
		root := NewFuelRoot(sharedStore, dataCache, metaCache, metaEng, cfg)
		_ = fs.NewNodeFS(root, &fs.Options{})
		return &testEnv{
			store:     sharedStore,
			dataCache: dataCache,
			metaCache: metaCache,
			root:      root,
			ctx:       context.Background(),
		}
	}

	nodeA := newRedisEnv(t)
	nodeB := newRedisEnv(t)

	// 节点 A 写入 V1，节点 B 读到 V1
	writeFile(t, nodeA, "shared.txt", []byte("from-node-A"))
	readAndVerify(t, nodeB, "shared.txt", []byte("from-node-A"))

	// 节点 A 覆盖写 V2（Flush 失效 L2），节点 B 立即读到 V2
	writeFile(t, nodeA, "shared.txt", []byte("v2-by-node-A!"))
	readAndVerify(t, nodeB, "shared.txt", []byte("v2-by-node-A!"))
}

// 场景 5: 删除 → 读到 ENOENT。
func TestWrite_Scenario5_UnlinkThenENOENT(t *testing.T) {
	env := newTestEnv(t)
	writeFile(t, env, "del.txt", []byte("bye"))
	etag := env.head("del.txt").ETag

	if errno := env.root.Unlink(env.ctx, "del.txt"); errno != 0 {
		t.Fatalf("Unlink failed: %v", errno)
	}

	var out fuse.EntryOut
	if _, errno := env.root.Lookup(env.ctx, "del.txt", &out); errno != syscall.ENOENT {
		t.Errorf("Lookup after Unlink = %v, want ENOENT", errno)
	}
	if env.dataCache.Contains("del.txt", etag) {
		t.Error("data cache entry should be removed after Unlink")
	}
}

// 场景 6: rename → 旧路径 ENOENT，新路径可读且内容一致。
func TestWrite_Scenario6_Rename(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("rename me")
	writeFile(t, env, "old.txt", data)

	if errno := env.root.Rename(env.ctx, "old.txt", env.root, "new.txt", 0); errno != 0 {
		t.Fatalf("Rename failed: %v", errno)
	}

	var out fuse.EntryOut
	if _, errno := env.root.Lookup(env.ctx, "old.txt", &out); errno != syscall.ENOENT {
		t.Errorf("Lookup old path after Rename = %v, want ENOENT", errno)
	}
	readAndVerify(t, env, "new.txt", data)
}

// 场景 7: mkdir → readdir 可见 + Lookup 为目录。
func TestWrite_Scenario7_MkdirVisible(t *testing.T) {
	env := newTestEnv(t)

	var out fuse.EntryOut
	if _, errno := env.root.Mkdir(env.ctx, "d1", 0o755, &out); errno != 0 {
		t.Fatalf("Mkdir failed: %v", errno)
	}
	if !containsName(readdirNames(t, env), "d1") {
		t.Error("Readdir should contain d1 after Mkdir")
	}
	if _, errno := env.root.Lookup(env.ctx, "d1", &out); errno != 0 {
		t.Fatalf("Lookup d1 failed: %v", errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Errorf("d1 should be dir, mode=%o", out.Mode)
	}
}

// 场景 8: rmdir → readdir 不可见。
func TestWrite_Scenario8_RmdirInvisible(t *testing.T) {
	env := newTestEnv(t)

	var out fuse.EntryOut
	if _, errno := env.root.Mkdir(env.ctx, "d1", 0o755, &out); errno != 0 {
		t.Fatalf("Mkdir failed: %v", errno)
	}
	if errno := env.root.Rmdir(env.ctx, "d1"); errno != 0 {
		t.Fatalf("Rmdir failed: %v", errno)
	}
	if containsName(readdirNames(t, env), "d1") {
		t.Error("Readdir should not contain d1 after Rmdir")
	}
	if _, errno := env.root.Lookup(env.ctx, "d1", &out); errno != syscall.ENOENT {
		t.Errorf("Lookup d1 after Rmdir = %v, want ENOENT", errno)
	}
}

// --- 写路径边界行为 ---

// 零字节文件：Create + 无 Write + Flush → 存在且 size=0。
func TestWrite_ZeroByteFile(t *testing.T) {
	env := newTestEnv(t)
	writeFile(t, env, "empty.txt", nil)

	var out fuse.EntryOut
	if _, errno := env.root.Lookup(env.ctx, "empty.txt", &out); errno != 0 {
		t.Fatalf("Lookup empty.txt failed: %v", errno)
	}
	if out.Size != 0 {
		t.Errorf("size = %d, want 0", out.Size)
	}
	readAndVerify(t, env, "empty.txt", []byte{})
}

// Flush 后再 Write → ENOTSUP（一次写语义，不支持写后追写）。
func TestWrite_AfterFlush_ENOTSUP(t *testing.T) {
	env := newTestEnv(t)

	var out fuse.EntryOut
	inode, fh, _, errno := env.root.Create(env.ctx, "f.txt", syscall.O_WRONLY|syscall.O_CREAT, 0o644, &out)
	if errno != 0 {
		t.Fatalf("Create failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)
	if _, errno := node.Write(env.ctx, fh, []byte("a"), 0); errno != 0 {
		t.Fatalf("Write failed: %v", errno)
	}
	if errno := node.Flush(env.ctx, fh); errno != 0 {
		t.Fatalf("Flush failed: %v", errno)
	}
	if _, errno := node.Write(env.ctx, fh, []byte("b"), 1); errno != syscall.ENOTSUP {
		t.Errorf("Write after Flush = %v, want ENOTSUP", errno)
	}
	_ = fh.(fs.FileReleaser).Release(env.ctx)
}

// 已存在文件 Create 无 O_TRUNC → ENOTSUP（原地改写不支持）。
func TestWrite_CreateExistingNoTrunc_ENOTSUP(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("exist.txt", []byte("data"))

	var out fuse.EntryOut
	_, _, _, errno := env.root.Create(env.ctx, "exist.txt", syscall.O_WRONLY|syscall.O_CREAT, 0o644, &out)
	if errno != syscall.ENOTSUP {
		t.Errorf("Create existing without O_TRUNC = %v, want ENOTSUP", errno)
	}
}

// Open O_WRONLY|O_TRUNC 覆盖已有文件（不经 Create 的路径）。
func TestWrite_OpenTruncOverwrite(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("f.txt", []byte("old-content"))

	var out fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, "f.txt", &out)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)
	fh, _, errno := node.Open(env.ctx, syscall.O_WRONLY|syscall.O_TRUNC)
	if errno != 0 {
		t.Fatalf("Open O_TRUNC failed: %v", errno)
	}
	if _, errno := node.Write(env.ctx, fh, []byte("new-content"), 0); errno != 0 {
		t.Fatalf("Write failed: %v", errno)
	}
	if errno := node.Flush(env.ctx, fh); errno != 0 {
		t.Fatalf("Flush failed: %v", errno)
	}
	_ = fh.(fs.FileReleaser).Release(env.ctx)

	readAndVerify(t, env, "f.txt", []byte("new-content"))
}

// Unlink 不存在的文件 → ENOENT；Unlink 目录 → EISDIR。
func TestWrite_UnlinkErrors(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("d/f.txt", []byte("x"))

	if errno := env.root.Unlink(env.ctx, "missing.txt"); errno != syscall.ENOENT {
		t.Errorf("Unlink missing = %v, want ENOENT", errno)
	}
	if errno := env.root.Unlink(env.ctx, "d"); errno != syscall.EISDIR {
		t.Errorf("Unlink dir = %v, want EISDIR", errno)
	}
}

// Rmdir 非空目录 → ENOTEMPTY；不存在 → ENOENT。
func TestWrite_RmdirErrors(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("d/f.txt", []byte("x"))

	if errno := env.root.Rmdir(env.ctx, "d"); errno != syscall.ENOTEMPTY {
		t.Errorf("Rmdir non-empty = %v, want ENOTEMPTY", errno)
	}
	if errno := env.root.Rmdir(env.ctx, "missing"); errno != syscall.ENOENT {
		t.Errorf("Rmdir missing = %v, want ENOENT", errno)
	}
}

// Rename 不存在的源 → ENOENT；Rename 目录 → ENOTSUP；带 flags → ENOTSUP。
func TestWrite_RenameErrors(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("d/f.txt", []byte("x"))
	env.addFile("src.txt", []byte("y"))

	if errno := env.root.Rename(env.ctx, "missing.txt", env.root, "n.txt", 0); errno != syscall.ENOENT {
		t.Errorf("Rename missing = %v, want ENOENT", errno)
	}
	if errno := env.root.Rename(env.ctx, "d", env.root, "d2", 0); errno != syscall.ENOTSUP {
		t.Errorf("Rename dir = %v, want ENOTSUP", errno)
	}
	if errno := env.root.Rename(env.ctx, "src.txt", env.root, "n.txt", 1); errno != syscall.ENOTSUP {
		t.Errorf("Rename with flags = %v, want ENOTSUP", errno)
	}
}

// Fsync 与 Flush 等价（对象存储无 fsync 语义）。
func TestWrite_FsyncSameAsFlush(t *testing.T) {
	env := newTestEnv(t)

	var out fuse.EntryOut
	inode, fh, _, errno := env.root.Create(env.ctx, "f.txt", syscall.O_WRONLY|syscall.O_CREAT, 0o644, &out)
	if errno != 0 {
		t.Fatalf("Create failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)
	if _, errno := node.Write(env.ctx, fh, []byte("synced"), 0); errno != 0 {
		t.Fatalf("Write failed: %v", errno)
	}
	if errno := node.Fsync(env.ctx, fh, 0); errno != 0 {
		t.Fatalf("Fsync failed: %v", errno)
	}
	_ = fh.(fs.FileReleaser).Release(env.ctx)

	readAndVerify(t, env, "f.txt", []byte("synced"))
}
