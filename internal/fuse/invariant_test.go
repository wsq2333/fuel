package fuse

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/config"
	"fuel/internal/objectstore"
)

// =============================================================================
// E2E Invariant Validation Tests
//
// 验证所有架构不变量 (INV-1 ~ INV-9) 在完整 FUSE 读路径中成立。
// 使用 mock ObjectStore + 真实 DataCache + 真实 MetaCache + mock MetadataEngine，
// 模拟应用通过 FUSE 接口读数据的完整链路。
// =============================================================================

// --- INV-1: 对象存储是数据真相来源 ---
// 缓存层可以丢失，数据可从对象存储重建。

func TestINV1_CacheLoss_DataRecoverable(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("training frame sensor data")
	env.addFile("dataset/frame001.jpg", data)

	key := "dataset/frame001.jpg"

	// 首次读：触发 cache miss → 从对象存储拉取 → 写入缓存
	readAndVerify(t, env, key, data)

	// 验证缓存已存在
	meta := env.head(key)
	if !env.dataCache.Contains(key, meta.ETag) {
		t.Fatal("cache should exist after first read")
	}

	// 模拟缓存丢失：直接删除缓存文件和索引
	if err := env.dataCache.Remove(key); err != nil {
		t.Fatalf("Remove cache: %v", err)
	}
	if env.dataCache.Contains(key, meta.ETag) {
		t.Fatal("cache should be gone after Remove")
	}

	// 二次读：缓存丢失 → 自动从对象存储重新拉取 → 数据完整
	readAndVerify(t, env, key, data)

	// 验证缓存重建
	if !env.dataCache.Contains(key, meta.ETag) {
		t.Error("cache should be rebuilt from object store")
	}
}

func TestINV1_MetadataEngineLoss_DataReadable(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("important data")
	env.addFile("dir/file.bin", data)

	// 首次读正常
	readAndVerify(t, env, "dir/file.bin", data)

	// 模拟元数据引擎数据丢失（清空 entries）
	env.metaEng.entries = make(map[string]*api.MetaEntry)
	env.metaEng.dirs = make(map[string][]api.DirEntry)

	// L1 cache 也清空
	env.metaCache = cache.NewMetaCache(config.MetaCacheConfig{})
	env.root.metaCache = env.metaCache

	// 数据仍可读（direct 引擎 fallback 到 ObjectStore.Head/List）
	readAndVerify(t, env, "dir/file.bin", data)
}

// --- INV-2: 缓存是对象存储对象的字节镜像 ---
// 缓存文件路径 = {dir}/{bucket}/{key}，内容完全一致。

func TestINV2_CacheIsByteIdenticalMirror(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("exact byte content 0xDEADBEEF")
	env.addFile("train/frame.pcd", data)

	key := "train/frame.pcd"
	readAndVerify(t, env, key, data)

	meta := env.head(key)
	localPath, hit, err := env.dataCache.Get(key, meta.ETag)
	if err != nil || !hit {
		t.Fatalf("Get cache: hit=%v err=%v", hit, err)
	}

	// 验证路径映射: {dir}/{bucket}/{key}
	if !strings.HasSuffix(filepath.ToSlash(localPath), "/test-bucket/train/frame.pcd") {
		t.Errorf("cache path should be {dir}/{bucket}/{key}, got %q", localPath)
	}

	// 验证内容完全一致（字节镜像）：缓存文件可被外部工具直接读取
	cacheContent, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	if !bytes.Equal(cacheContent, data) {
		t.Errorf("cache content mismatch:\ngot:  %q\nwant: %q", cacheContent, data)
	}

	// 验证 md5sum 一致
	srcMD5 := md5sum(data)
	cacheMD5 := md5sum(cacheContent)
	if srcMD5 != cacheMD5 {
		t.Errorf("md5 mismatch: src=%s cache=%s", srcMD5, cacheMD5)
	}
}

func TestINV2_CacheCanBeCleared_AutoRebuilds(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("rebuildable data")
	env.addFile("data.bin", data)

	readAndVerify(t, env, "data.bin", data)

	// 模拟 rm -rf /nvme/cache/*：直接删除缓存文件（不通过 Remove 接口）
	meta := env.head("data.bin")
	localPath, hit, _ := env.dataCache.Get("data.bin", meta.ETag)
	if hit {
		_ = os.Remove(localPath)
	}

	// 下次访问自动重建
	readAndVerify(t, env, "data.bin", data)
}

// --- INV-4: 元数据引擎是可选的加速层 ---
// direct 模式无外部依赖，功能完整。

func TestINV4_DirectMode_NoExternalDependency(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("direct mode data")
	env.addFile("dir1/a.txt", data)
	env.addFile("dir1/b.txt", []byte("b content"))

	// Lookup 文件正常
	readAndVerify(t, env, "dir1/a.txt", data)

	// Lookup 目录（隐式目录由前缀推断）
	var dirOut fuse.EntryOut
	dirInode, errno := env.root.Lookup(env.ctx, "dir1", &dirOut)
	if errno != 0 {
		t.Fatalf("Lookup dir: %v", errno)
	}
	if dirOut.Mode&syscall.S_IFDIR == 0 {
		t.Error("dir1 should be a directory")
	}

	// Readdir 正常
	dirNode := dirInode.Operations().(*FuelNode)
	stream, errno := dirNode.Readdir(env.ctx)
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}
	names := collectDirNames(stream)
	if len(names) < 2 {
		t.Errorf("Readdir should have >= 2 entries, got %v", names)
	}
}

// --- INV-7: 模块边界通过接口隔离 ---
// 编译期验证：FuelRoot 不直接引用具体实现类型。

func TestINV7_InterfaceIsolation_CompileTime(t *testing.T) {
	// 这些接口断言在 node.go 底部已编译期检查。
	// 此测试补充验证 FuelRoot 的依赖注入通过接口完成。
	var _ api.ObjectStore = objectstore.NewMockStore("b")
	var _ api.MetadataEngine = newMockMetaEngine(objectstore.NewMockStore("b"))
	var _ cache.MetaCache = cache.NewMetaCache(config.MetaCacheConfig{})

	// 验证 FuelRoot 构造函数接受接口类型（不是具体类型）
	root := NewFuelRoot(
		objectstore.NewMockStore("b"),
		newNoopDataCache(),
		cache.NewMetaCache(config.MetaCacheConfig{}),
		newMockMetaEngine(objectstore.NewMockStore("b")),
		&config.Config{},
	)
	if root == nil {
		t.Fatal("NewFuelRoot should not return nil")
	}
}

// --- INV-9: 读路径要么返回绝对正确的数据，要么回退到真相来源 ---
// 缓存命中后 ETag 不匹配 → 立即剔除并回源。

func TestINV9_ETagMismatch_EvictsCache_Refetches(t *testing.T) {
	env := newTestEnv(t)
	originalData := []byte("version 1")
	env.addFile("mutable.txt", originalData)

	// 首次读：数据被缓存
	readAndVerify(t, env, "mutable.txt", originalData)

	// 模拟外部修改：对象存储内容变更，ETag 变化
	updatedData := []byte("version 2 updated")
	env.addFile("mutable.txt", updatedData)

	// 清除 L1 缓存（模拟 TTL 过期）
	env.metaCache = cache.NewMetaCache(config.MetaCacheConfig{})
	env.root.metaCache = env.metaCache

	// 再次读：ETag 不匹配 → 旧缓存被剔除 → 从对象存储重新拉取新数据
	readAndVerify(t, env, "mutable.txt", updatedData)
}

func TestINV9_CacheCorrupted_FallsBackToSource(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("original content")
	env.addFile("important.dat", data)

	// 首次读，缓存写入
	readAndVerify(t, env, "important.dat", data)

	// 模拟磁盘损坏：直接删除缓存文件（索引仍存在，但文件不见了）
	meta := env.head("important.dat")
	localPath, hit, _ := env.dataCache.Get("important.dat", meta.ETag)
	if hit {
		_ = os.Remove(localPath)
	}

	// 再次读：缓存文件丢失 → DataCache.Get miss → 回源对象存储
	readAndVerify(t, env, "important.dat", data)
}

func TestINV9_NeverReturnsStaleData(t *testing.T) {
	env := newTestEnv(t)

	// 写入初始版本
	env.addFile("evolving.bin", []byte("v1"))
	readAndVerify(t, env, "evolving.bin", []byte("v1"))

	// 连续 3 次修改，每次验证读到最新数据
	for i := 2; i <= 4; i++ {
		newData := []byte("version-" + string(rune('0'+i)))
		env.addFile("evolving.bin", newData)

		// 清 L1（模拟 TTL 过期）
		env.metaCache = cache.NewMetaCache(config.MetaCacheConfig{})
		env.root.metaCache = env.metaCache

		readAndVerify(t, env, "evolving.bin", newData)
	}
}

// --- INV-9 补充: singleflight 并发安全 ---

func TestINV9_ConcurrentReads_AllGetCorrectData(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("concurrent read target data")
	env.addFile("shared.bin", data)

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := readFile(env, "shared.bin")
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, data) {
				errs <- &dataError{key: "shared.bin", got: got, want: data}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent read error: %v", err)
	}
}

// --- INV-1 + INV-9 综合: 对象存储不可达时已缓存数据仍可读 ---

func TestINV1_INV9_ObjectStoreDown_CachedDataStillReadable(t *testing.T) {
	// 使用有 TTL 的 L1 缓存（非零 StatTTL），模拟生产环境
	env := newTestEnvWithTTL(t)
	data := []byte("cached before outage")
	env.addFile("cached.bin", data)

	// 首次读，数据进入缓存 + L1 stat 缓存
	readAndVerify(t, env, "cached.bin", data)

	// 模拟对象存储不可达：替换 store 为总是报错的实现
	failStore := &failingStore{}
	env.root.store = failStore
	env.metaEng.store = failStore

	// L1 stat 缓存仍有效（TTL 内），数据缓存仍在 NVMe
	// 读缓存命中 → 数据正确返回（不需要访问对象存储）
	got, err := readFile(env, "cached.bin")
	if err != nil {
		t.Fatalf("read cached file during outage: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

// --- INV-2 补充: 不做格式转换/压缩/去重 ---

func TestINV2_NoFormatTransformation(t *testing.T) {
	env := newTestEnv(t)

	// 测试各种二进制内容保持不变
	testCases := []struct {
		key  string
		data []byte
	}{
		{"binary.bin", []byte{0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF}},
		{"zeros.dat", bytes.Repeat([]byte{0x00}, 1024)},
		{"repeated.dat", bytes.Repeat([]byte("AAAA"), 256)},
		{"utf8.txt", []byte("你好世界 🌍")},
	}

	for _, tc := range testCases {
		env.addFile(tc.key, tc.data)
		readAndVerify(t, env, tc.key, tc.data)

		// 验证缓存文件与源数据按字节完全一致
		meta := env.head(tc.key)
		localPath, hit, _ := env.dataCache.Get(tc.key, meta.ETag)
		if !hit {
			t.Errorf("%s: expected cache hit", tc.key)
			continue
		}
		cached, err := os.ReadFile(localPath)
		if err != nil {
			t.Errorf("%s: read cache: %v", tc.key, err)
			continue
		}
		if !bytes.Equal(cached, tc.data) {
			t.Errorf("%s: cache content differs from source (len cached=%d, source=%d)",
				tc.key, len(cached), len(tc.data))
		}
	}
}

// =============================================================================
// Test helpers
// =============================================================================

// newTestEnvWithTTL 构造启用 L1 TTL 的测试环境（模拟生产环境行为）。
func newTestEnvWithTTL(t *testing.T) *testEnv {
	t.Helper()

	store := objectstore.NewMockStore("test-bucket")

	cfg := &config.Config{
		Storage: config.StorageConfig{
			Type:   "oss",
			Bucket: "test-bucket",
			OSS:    config.OSSConfig{Endpoint: "test-endpoint"},
		},
		Metadata: config.MetadataConfig{
			Engine: "direct",
			Cache: config.MetaCacheConfig{
				StatTTL: 30 * time.Second,
				DirTTL:  10 * time.Second,
				NegTTL:  60 * time.Second,
			},
		},
		Cache: config.CacheConfig{
			Dir:           t.TempDir(),
			Capacity:      1 << 30,
			HighWatermark: 0.85,
			LowWatermark:  0.70,
		},
	}

	dataCache, err := cache.NewNVMeCache(cfg.Cache.Dir, "test-bucket", 1<<30, 0.85, 0.70, 0)
	if err != nil {
		t.Fatalf("NewNVMeCache failed: %v", err)
	}

	metaCache := cache.NewMetaCache(cfg.Metadata.Cache)
	metaEng := newMockMetaEngine(store)

	root := NewFuelRoot(store, dataCache, metaCache, metaEng, cfg)
	_ = fs.NewNodeFS(root, &fs.Options{})

	return &testEnv{
		store:     store,
		dataCache: dataCache,
		metaCache: metaCache,
		metaEng:   metaEng,
		root:      root,
		ctx:       context.Background(),
	}
}

func readAndVerify(t *testing.T, env *testEnv, key string, expected []byte) {
	t.Helper()
	got, err := readFile(env, key)
	if err != nil {
		t.Fatalf("readFile(%s): %v", key, err)
	}
	if !bytes.Equal(got, expected) {
		t.Errorf("readFile(%s) = %q, want %q", key, got, expected)
	}
}

func readFile(env *testEnv, key string) ([]byte, error) {
	parts := strings.Split(key, "/")
	var currentNode interface {
		Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno)
	}
	currentNode = env.root

	// Navigate to the file's parent
	for i := 0; i < len(parts)-1; i++ {
		var out fuse.EntryOut
		inode, errno := currentNode.Lookup(env.ctx, parts[i], &out)
		if errno != 0 {
			return nil, errno
		}
		currentNode = inode.Operations().(interface {
			Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno)
		})
	}

	// Lookup the file
	var fileOut fuse.EntryOut
	fileInode, errno := currentNode.Lookup(env.ctx, parts[len(parts)-1], &fileOut)
	if errno != 0 {
		return nil, errno
	}
	node := fileInode.Operations().(*FuelNode)

	// Open
	fh, _, errno := node.Open(env.ctx, 0)
	if errno != 0 {
		return nil, errno
	}
	defer fh.(fs.FileReleaser).Release(env.ctx)

	// Read entire file
	dest := make([]byte, fileOut.Size)
	result, errno := node.Read(env.ctx, fh, dest, 0)
	if errno != 0 {
		return nil, errno
	}
	got, _ := result.Bytes(nil)
	return got, nil
}

func collectDirNames(stream fs.DirStream) []string {
	var names []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		names = append(names, entry.Name)
	}
	return names
}

func md5sum(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

// failingStore 是总是返回错误的 ObjectStore（模拟对象存储不可达）。
type failingStore struct{}

func (f *failingStore) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	return nil, syscall.EIO
}
func (f *failingStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return nil, syscall.EIO
}
func (f *failingStore) Put(ctx context.Context, key string, r io.Reader, size int64) (*api.ObjectMeta, error) {
	return nil, syscall.EIO
}
func (f *failingStore) List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]api.ObjectEntry, []string, error) {
	return nil, nil, syscall.EIO
}
func (f *failingStore) Copy(ctx context.Context, srcKey, dstKey string) error {
	return syscall.EIO
}
func (f *failingStore) Delete(ctx context.Context, key string) error {
	return syscall.EIO
}
func (f *failingStore) Bucket() string { return "test-bucket" }

var _ api.ObjectStore = (*failingStore)(nil)

// noopDataCache 用于 INV-7 接口隔离测试。
type noopDataCache struct{}

func newNoopDataCache() api.DataCache                                         { return &noopDataCache{} }
func (n *noopDataCache) Get(key, etag string) (string, bool, error)           { return "", false, nil }
func (n *noopDataCache) Put(key, etag string, size int64, r io.Reader) (string, error) {
	return "", nil
}
func (n *noopDataCache) Remove(key string) error        { return nil }
func (n *noopDataCache) Contains(key, etag string) bool { return false }
func (n *noopDataCache) Stats() api.CacheStats          { return api.CacheStats{} }

var _ api.DataCache = (*noopDataCache)(nil)

// dataError 用于并发测试的错误报告。
type dataError struct {
	key  string
	got  []byte
	want []byte
}

func (e *dataError) Error() string {
	return "data mismatch for " + e.key
}
