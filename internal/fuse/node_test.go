package fuse

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"fuel/api"
	"fuel/internal/cache"
	"fuel/internal/config"
	"fuel/internal/objectstore"
)

// mockMetaEngine 是 MetadataEngine 的内存实现，用于单元测试。
// GetAttr miss 时 fallback 到 store（模拟 direct 引擎行为）。
type mockMetaEngine struct {
	store   api.ObjectStore
	entries map[string]*api.MetaEntry
	dirs    map[string][]api.DirEntry
	uid     uint32
	gid     uint32
}

func newMockMetaEngine(store api.ObjectStore) *mockMetaEngine {
	return &mockMetaEngine{
		store:   store,
		entries: make(map[string]*api.MetaEntry),
		dirs:    make(map[string][]api.DirEntry),
		uid:     uint32(os.Getuid()),
		gid:     uint32(os.Getgid()),
	}
}

func (m *mockMetaEngine) GetAttr(ctx context.Context, path string) (*api.MetaEntry, error) {
	if e, ok := m.entries[path]; ok {
		return e, nil
	}
	// fallback to store: file → explicit dir marker → implicit dir (prefix)
	om, err := m.store.Head(ctx, path)
	if err == nil {
		return api.MetaEntryFromObjectMeta(om, m.uid, m.gid), nil
	}
	if !errors.Is(err, syscall.ENOENT) {
		return nil, err
	}
	dirKey := path + "/"
	if _, err := m.store.Head(ctx, dirKey); err == nil {
		return api.DirMetaEntry(path, m.uid, m.gid), nil
	}
	entries, prefixes, err := m.store.List(ctx, dirKey, "/", 1)
	if err != nil {
		return nil, err
	}
	if len(entries) > 0 || len(prefixes) > 0 {
		return api.DirMetaEntry(path, m.uid, m.gid), nil
	}
	return nil, syscall.ENOENT
}

func (m *mockMetaEngine) SetAttr(ctx context.Context, path string, entry *api.MetaEntry) error {
	m.entries[path] = entry
	return nil
}

func (m *mockMetaEngine) DeleteAttr(ctx context.Context, path string) error {
	delete(m.entries, path)
	return nil
}

func (m *mockMetaEngine) ListDir(ctx context.Context, dirPath string) ([]api.DirEntry, error) {
	if entries, ok := m.dirs[dirPath]; ok {
		return entries, nil
	}
	prefix := dirPath
	if prefix != "" {
		prefix += "/"
	}
	objs, prefixes, err := m.store.List(ctx, prefix, "/", 0)
	if err != nil {
		return nil, err
	}
	var entries []api.DirEntry
	for _, cp := range prefixes {
		name := strings.TrimSuffix(strings.TrimPrefix(cp, prefix), "/")
		if name == "" {
			continue
		}
		entries = append(entries, api.DirEntry{
			Name:  name,
			IsDir: true,
			Meta:  api.DirMetaEntry(prefix+name, m.uid, m.gid),
		})
	}
	for _, obj := range objs {
		name := strings.TrimPrefix(obj.Key, prefix)
		if name == "" || strings.HasSuffix(name, "/") {
			continue
		}
		entries = append(entries, api.DirEntry{
			Name:  name,
			IsDir: false,
			Meta: api.MetaEntryFromObjectMeta(&api.ObjectMeta{
				Key:  obj.Key,
				Size: obj.Size,
			}, m.uid, m.gid),
		})
	}
	return entries, nil
}

func (m *mockMetaEngine) SetDir(ctx context.Context, dirPath string, entries []api.DirEntry) error {
	m.dirs[dirPath] = entries
	return nil
}

func (m *mockMetaEngine) DeleteDir(ctx context.Context, dirPath string) error {
	delete(m.dirs, dirPath)
	return nil
}

func (m *mockMetaEngine) BatchGetAttr(ctx context.Context, paths []string) (map[string]*api.MetaEntry, error) {
	result := make(map[string]*api.MetaEntry, len(paths))
	for _, p := range paths {
		if e, ok := m.entries[p]; ok {
			result[p] = e
		}
	}
	return result, nil
}

func (m *mockMetaEngine) Invalidate(ctx context.Context, path string) error { return nil }
func (m *mockMetaEngine) HealthCheck(ctx context.Context) error            { return nil }
func (m *mockMetaEngine) Close() error                                     { return nil }

// testEnv 构造测试环境（mock 依赖，无真实挂载）。
type testEnv struct {
	store     api.ObjectStore
	dataCache api.DataCache
	metaCache cache.MetaCache
	metaEng   *mockMetaEngine
	root      *FuelRoot
	ctx       context.Context
}

func newTestEnv(t *testing.T) *testEnv {
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
				StatTTL: 0,
				DirTTL:  0,
				NegTTL:  0,
			},
		},
		Cache: config.CacheConfig{
			Dir:       t.TempDir(),
			Capacity:  1 << 30,
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

	// 通过 NewNodeFS 初始化 bridge（不挂载，仅用于测试 NewInode）。
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

func (e *testEnv) addFile(key string, data []byte) {
	_, _ = e.store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)))
}

func (e *testEnv) addDir(dirPath string, entries []api.DirEntry) {
	e.metaEng.dirs[dirPath] = entries
}

func (e *testEnv) head(key string) *api.ObjectMeta {
	meta, _ := e.store.Head(context.Background(), key)
	return meta
}

// --- 测试 ---

func TestFuelRoot_Getattr_Root(t *testing.T) {
	env := newTestEnv(t)

	var out fuse.AttrOut
	errno := env.root.Getattr(env.ctx, nil, &out)
	if errno != 0 {
		t.Fatalf("Getattr root failed: %v", errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Errorf("root should be dir, mode=%o", out.Mode)
	}
}

func TestFuelRoot_Lookup_File(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("a.txt", []byte("hello"))

	var out fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, "a.txt", &out)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	if inode == nil {
		t.Fatal("Lookup returned nil inode")
	}
	if out.Mode&syscall.S_IFREG == 0 {
		t.Errorf("file should be regular, mode=%o", out.Mode)
	}
	if out.Size != 5 {
		t.Errorf("size = %d, want 5", out.Size)
	}
}

func TestFuelRoot_Lookup_NotFound(t *testing.T) {
	env := newTestEnv(t)

	var out fuse.EntryOut
	_, errno := env.root.Lookup(env.ctx, "missing.txt", &out)
	if errno != syscall.ENOENT {
		t.Errorf("Lookup missing = %v, want ENOENT", errno)
	}
}

func TestFuelRoot_Readdir(t *testing.T) {
	env := newTestEnv(t)
	env.addDir("", []api.DirEntry{
		{Name: "file1", IsDir: false, Meta: &api.MetaEntry{Path: "file1", Inode: 100, Size: 10}},
		{Name: "dir1", IsDir: true, Meta: &api.MetaEntry{Path: "dir1", Inode: 101, IsDir: true}},
	})

	stream, errno := env.root.Readdir(env.ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %v", errno)
	}

	var names []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		names = append(names, entry.Name)
	}
	if len(names) != 2 {
		t.Errorf("Readdir got %d entries, want 2", len(names))
	}
}

func TestFuelNode_Lookup_Nested(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("dir/b.txt", []byte("world"))

	var dirOut fuse.EntryOut
	dirInode, errno := env.root.Lookup(env.ctx, "dir", &dirOut)
	if errno != 0 {
		t.Fatalf("Lookup dir failed: %v", errno)
	}

	var fileOut fuse.EntryOut
	fileNode := dirInode.Operations().(*FuelNode)
	fileInode, errno := fileNode.Lookup(env.ctx, "b.txt", &fileOut)
	if errno != 0 {
		t.Fatalf("Lookup dir/b.txt failed: %v", errno)
	}
	if fileInode == nil {
		t.Fatal("Lookup returned nil inode")
	}
	if fileOut.Size != 5 {
		t.Errorf("size = %d, want 5", fileOut.Size)
	}
}

func TestFuelNode_OpenAndRead_CacheHit(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("hello world")
	env.addFile("a.txt", data)

	key := "a.txt"
	meta := env.head(key)
	_, err := env.dataCache.Put(key, meta.ETag, int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	var attrOut fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, key, &attrOut)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)

	fh, _, errno := node.Open(env.ctx, 0)
	if errno != 0 {
		t.Fatalf("Open failed: %v", errno)
	}
	defer fh.(fs.FileReleaser).Release(env.ctx)

	dest := make([]byte, len(data))
	result, errno := node.Read(env.ctx, fh, dest, 0)
	if errno != 0 {
		t.Fatalf("Read failed: %v", errno)
	}
	got, _ := result.Bytes(nil)
	if string(got) != string(data) {
		t.Errorf("Read = %q, want %q", got, data)
	}
}

func TestFuelNode_OpenAndRead_CacheMiss(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("hello world")
	env.addFile("a.txt", data)

	key := "a.txt"
	var attrOut fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, key, &attrOut)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)

	fh, _, errno := node.Open(env.ctx, 0)
	if errno != 0 {
		t.Fatalf("Open failed: %v", errno)
	}
	defer fh.(fs.FileReleaser).Release(env.ctx)

	dest := make([]byte, len(data))
	result, errno := node.Read(env.ctx, fh, dest, 0)
	if errno != 0 {
		t.Fatalf("Read failed: %v", errno)
	}
	got, _ := result.Bytes(nil)
	if string(got) != string(data) {
		t.Errorf("Read = %q, want %q", got, data)
	}

	meta := env.head(key)
	if !env.dataCache.Contains(key, meta.ETag) {
		t.Error("cache should contain file after read")
	}
}

func TestFuelNode_Read_Partial(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("hello world")
	env.addFile("a.txt", data)

	key := "a.txt"
	var attrOut fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, key, &attrOut)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)

	fh, _, errno := node.Open(env.ctx, 0)
	if errno != 0 {
		t.Fatalf("Open failed: %v", errno)
	}
	defer fh.(fs.FileReleaser).Release(env.ctx)

	dest := make([]byte, 5)
	result, errno := node.Read(env.ctx, fh, dest, 6)
	if errno != 0 {
		t.Fatalf("Read failed: %v", errno)
	}
	got, _ := result.Bytes(nil)
	if string(got) != "world" {
		t.Errorf("Read = %q, want %q", got, "world")
	}
}

func TestFuelNode_Open_WriteMode_NotSupported(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("a.txt", []byte("hello"))

	key := "a.txt"
	var attrOut fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, key, &attrOut)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)

	_, _, errno = node.Open(env.ctx, syscall.O_WRONLY)
	if errno != syscall.ENOTSUP {
		t.Errorf("Open WRONLY = %v, want ENOTSUP", errno)
	}

	_, _, errno = node.Open(env.ctx, syscall.O_RDWR)
	if errno != syscall.ENOTSUP {
		t.Errorf("Open RDWR = %v, want ENOTSUP", errno)
	}
}

func TestFuelNode_Read_EOF(t *testing.T) {
	env := newTestEnv(t)
	data := []byte("hello")
	env.addFile("a.txt", data)

	key := "a.txt"
	var attrOut fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, key, &attrOut)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)

	fh, _, errno := node.Open(env.ctx, 0)
	if errno != 0 {
		t.Fatalf("Open failed: %v", errno)
	}
	defer fh.(fs.FileReleaser).Release(env.ctx)

	dest := make([]byte, 10)
	result, errno := node.Read(env.ctx, fh, dest, int64(len(data)))
	if errno != 0 {
		t.Fatalf("Read at EOF failed: %v", errno)
	}
	got, _ := result.Bytes(nil)
	if len(got) != 0 {
		t.Errorf("Read at EOF = %d bytes, want 0", len(got))
	}
}

func TestFuelNode_Readdir_Dir(t *testing.T) {
	env := newTestEnv(t)
	// 添加一个子文件，使 store 能检测到 dir1 为隐式目录
	env.addFile("dir1/nested", []byte("abc"))
	env.addDir("dir1", []api.DirEntry{
		{Name: "nested", IsDir: false, Meta: &api.MetaEntry{Path: "dir1/nested", Inode: 200, Size: 3}},
	})

	var dirOut fuse.EntryOut
	dirInode, errno := env.root.Lookup(env.ctx, "dir1", &dirOut)
	if errno != 0 {
		t.Fatalf("Lookup dir1 failed: %v", errno)
	}
	dirNode := dirInode.Operations().(*FuelNode)

	stream, errno := dirNode.Readdir(env.ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %v", errno)
	}

	var names []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		names = append(names, entry.Name)
	}
	if len(names) != 1 || names[0] != "nested" {
		t.Errorf("Readdir = %v, want [nested]", names)
	}
}

func TestFuelNode_Getattr(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("a.txt", []byte("hello"))

	var out fuse.EntryOut
	inode, errno := env.root.Lookup(env.ctx, "a.txt", &out)
	if errno != 0 {
		t.Fatalf("Lookup failed: %v", errno)
	}
	node := inode.Operations().(*FuelNode)

	var attrOut fuse.AttrOut
	errno = node.Getattr(env.ctx, nil, &attrOut)
	if errno != 0 {
		t.Fatalf("Getattr failed: %v", errno)
	}
	if attrOut.Size != 5 {
		t.Errorf("size = %d, want 5", attrOut.Size)
	}
}

func TestPathJoin(t *testing.T) {
	if got := pathJoin("", "a"); got != "a" {
		t.Errorf("pathJoin('', 'a') = %q, want 'a'", got)
	}
	if got := pathJoin("dir", "a"); got != "dir/a" {
		t.Errorf("pathJoin('dir', 'a') = %q, want 'dir/a'", got)
	}
}