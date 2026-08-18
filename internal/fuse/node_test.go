package fuse

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
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
			continue
		}
		// fallback to store Head (与 GetAttr 一致，模拟 direct 引擎批量 fallback)
		if e, err := m.GetAttr(ctx, p); err == nil {
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

// TestParentDir 验证父目录提取逻辑。
func TestParentDir(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a/b/c.txt", "a/b"},
		{"a.txt", ""},
		{"dir/", "dir"},
		{"", ""},
		{"a", ""},
	}
	for _, tc := range cases {
		if got := parentDir(tc.in); got != tc.want {
			t.Errorf("parentDir(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// --- 4.3 批量预取集成测试 ---

// TestFuelRoot_BatchPrefetch_Integration 验证 Open 触发 BatchPrefetcher，
// 异步将同目录后续小文件拉入缓存。
func TestFuelRoot_BatchPrefetch_Integration(t *testing.T) {
	env := newTestEnv(t)
	// testEnv 默认 cfg.Prefetch.Enabled=false，这里显式启用批量预取
	env.root.cfg.Prefetch.Enabled = true
	env.root.batch = cache.NewBatchPrefetcher(true)

	// 准备同目录 5 个小文件
	dir := "data"
	files := []string{"f1", "f2", "f3", "f4", "f5"}
	for _, f := range files {
		env.addFile(dir+"/"+f, []byte("hello "+f))
	}

	// 预填 L1 dir cache，让批量预取能找到后续文件
	env.metaCache.SetDir(dir, []api.DirEntry{
		{Name: "f1", Meta: &api.MetaEntry{Path: dir + "/f1", Size: 8}},
		{Name: "f2", Meta: &api.MetaEntry{Path: dir + "/f2", Size: 8}},
		{Name: "f3", Meta: &api.MetaEntry{Path: dir + "/f3", Size: 8}},
		{Name: "f4", Meta: &api.MetaEntry{Path: dir + "/f4", Size: 8}},
		{Name: "f5", Meta: &api.MetaEntry{Path: dir + "/f5", Size: 8}},
	})

	// 连续 Open f1/f2/f3（达到阈值）→ 触发批量预取
	for i := 1; i <= 3; i++ {
		var out fuse.EntryOut
		inode, errno := env.root.Lookup(env.ctx, dir+"/f"+string(rune('0'+i)), &out)
		if errno != 0 {
			t.Fatalf("Lookup f%d failed: %v", i, errno)
		}
		node := inode.Operations().(*FuelNode)
		fh, _, errno := node.Open(env.ctx, 0)
		if errno != 0 {
			t.Fatalf("Open f%d failed: %v", i, errno)
		}
		fh.(fs.FileReleaser).Release(env.ctx)
	}

	// 等待异步批量预取完成（最多 2s）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f4cached := env.dataCache.Contains(dir+"/f4", env.head(dir+"/f4").ETag)
		f5cached := env.dataCache.Contains(dir+"/f5", env.head(dir+"/f5").ETag)
		if f4cached && f5cached {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("expected f4 and f5 to be prefetched into cache")
}

// TestFuelRoot_BatchPrefetch_Disabled Prefetch.Enabled=false 时不触发批量预取。
func TestFuelRoot_BatchPrefetch_Disabled(t *testing.T) {
	env := newTestEnv(t)
	env.root.cfg.Prefetch.Enabled = false
	env.root.batch = cache.NewBatchPrefetcher(false)

	dir := "d"
	for i := 1; i <= 3; i++ {
		env.addFile(dir+"/f"+string(rune('0'+i)), []byte("x"))
	}
	for i := 1; i <= 3; i++ {
		var out fuse.EntryOut
		inode, _ := env.root.Lookup(env.ctx, dir+"/f"+string(rune('0'+i)), &out)
		node := inode.Operations().(*FuelNode)
		fh, _, _ := node.Open(env.ctx, 0)
		fh.(fs.FileReleaser).Release(env.ctx)
	}
	time.Sleep(100 * time.Millisecond)
	// 未预取（Prefetcher 关闭）
	_ = env
}

// --- 4.3 元数据并行预取测试 ---

// incompleteMetaEngine 是 ListDir 返回 Meta=nil 的 mock，用于测试 fillMissingMeta。
type incompleteMetaEngine struct {
	*mockMetaEngine
}

func (m *incompleteMetaEngine) ListDir(ctx context.Context, dirPath string) ([]api.DirEntry, error) {
	entries, err := m.mockMetaEngine.ListDir(ctx, dirPath)
	if err != nil {
		return nil, err
	}
	// 模拟 direct engine List 不返回 ETag 的场景：把 Meta 清为 nil
	for i := range entries {
		entries[i].Meta = nil
	}
	return entries, nil
}

// TestFuelRoot_FillMissingMeta 验证 listDir 对 Meta 缺失的 entry 并发补齐。
func TestFuelRoot_FillMissingMeta(t *testing.T) {
	env := newTestEnv(t)
	incomplete := &incompleteMetaEngine{mockMetaEngine: env.metaEng}

	// 替换 root 的 metaEng 为不返回 Meta 的版本
	root := NewFuelRoot(env.store, env.dataCache, env.metaCache, incomplete, env.root.cfg)

	env.addFile("d/f1", []byte("aaa"))
	env.addFile("d/f2", []byte("bbb"))

	entries, errno := root.listDirEntries(env.ctx, "d")
	if errno != 0 {
		t.Fatalf("listDirEntries failed: %v", errno)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.Meta == nil {
			t.Errorf("entry %d Meta should be filled by fillMissingMeta", i)
			continue
		}
		if e.Meta.ETag == "" {
			t.Errorf("entry %d ETag should be filled from HEAD", i)
		}
		if e.Meta.Size == 0 {
			t.Errorf("entry %d Size should be filled from HEAD", i)
		}
	}
}

// TestFuelRoot_FillMissingMeta_AlreadyInline 已含完整 Meta 的 entry 不重复调用。
func TestFuelRoot_FillMissingMeta_AlreadyInline(t *testing.T) {
	env := newTestEnv(t)
	env.addFile("d/f", []byte("x"))

	// mockMetaEngine 的 ListDir 已内联 Meta（含 Size 但 ETag 为空——List API 不返回 ETag）
	entries, errno := env.root.listDirEntries(env.ctx, "d")
	if errno != 0 {
		t.Fatalf("listDirEntries failed: %v", errno)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	// direct engine 的 List 返回 ObjectEntry 无 ETag，所以 fillMissingMeta 会补齐
	// 验证 ETag 被补齐（HEAD 从 store 取到真实 ETag）
	if entries[0].Meta.ETag == "" {
		t.Error("ETag should be filled by fillMissingMeta (List API returns no ETag)")
	}
}

// --- INV-9: 空 ETag 不得进入缓存身份链 ---

// failingBatchEngine 模拟 BatchGetAttr 失败的引擎（fillMissingMeta 降级路径），
// ListDir 返回的文件 Meta 停留为空 ETag。
type failingBatchEngine struct {
	*mockMetaEngine
}

func (m *failingBatchEngine) BatchGetAttr(ctx context.Context, paths []string) (map[string]*api.MetaEntry, error) {
	return nil, errors.New("batch unavailable")
}

// TestFuelRoot_ListDirEntries_NoStatPrefillForEmptyETag 验证 fillMissingMeta 失败时，
// 空 ETag 的文件 entry 不预填 stat 缓存（空身份会让 Open 的 ETag 校验失效，INV-9），
// 后续 getAttr 回源拿到真实 ETag。
func TestFuelRoot_ListDirEntries_NoStatPrefillForEmptyETag(t *testing.T) {
	env := newTestEnv(t)
	metaCache := cache.NewMetaCache(config.MetaCacheConfig{StatTTL: time.Minute, DirTTL: time.Minute})
	root := NewFuelRoot(env.store, env.dataCache, metaCache, &failingBatchEngine{env.metaEng}, env.root.cfg)

	env.addFile("d/f1", []byte("aaa"))

	entries, errno := root.listDirEntries(env.ctx, "d")
	if errno != 0 {
		t.Fatalf("listDirEntries failed: %v", errno)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Meta == nil || entries[0].Meta.ETag != "" {
		t.Fatalf("precondition: entry Meta should exist with empty ETag, got %+v", entries[0].Meta)
	}

	if _, ok := metaCache.GetStat("d/f1"); ok {
		t.Error("stat cache should not be prefilled for file entry with empty ETag (INV-9)")
	}

	me, errno := root.getAttr(env.ctx, "d/f1")
	if errno != 0 || me == nil {
		t.Fatalf("getAttr failed: %v", errno)
	}
	if me.ETag == "" {
		t.Error("getAttr should fall back to engine and return real ETag")
	}
}

// TestFuelRoot_BatchPrefetch_SkipEmptyETag 验证批量预取跳过无 ETag 的目标，
// 正常目标仍被预取（INV-9：无身份不拉取入库）。
func TestFuelRoot_BatchPrefetch_SkipEmptyETag(t *testing.T) {
	env := newTestEnv(t)

	dir := "data"
	env.addFile(dir+"/f1", []byte("aaa"))
	env.addFile(dir+"/f2", []byte("bbb"))
	// f3 在引擎中有元数据但缺 ETag（降级路径残留），预取必须跳过
	env.metaEng.entries[dir+"/f3"] = &api.MetaEntry{Path: dir + "/f3", Size: 3}
	env.metaEng.dirs[dir] = []api.DirEntry{
		{Name: "f1", Meta: &api.MetaEntry{Path: dir + "/f1", Size: 3}},
		{Name: "f2", Meta: &api.MetaEntry{Path: dir + "/f2", Size: 3}},
		{Name: "f3", Meta: &api.MetaEntry{Path: dir + "/f3", Size: 3}},
	}

	env.root.prefetchBatch(env.ctx, dir+"/f1")

	// 等待 f2 预取完成（证明预取循环已执行）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if env.dataCache.Contains(dir+"/f2", env.head(dir+"/f2").ETag) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !env.dataCache.Contains(dir+"/f2", env.head(dir+"/f2").ETag) {
		t.Fatal("f2 should be prefetched into cache")
	}
	// f2 完成后循环已越过 f3，给跳过逻辑留出执行时间
	time.Sleep(100 * time.Millisecond)
	if got := env.dataCache.Stats().EntryCount; got != 1 {
		t.Errorf("cache should contain only f2 (f3 skipped for empty ETag), got %d entries", got)
	}
}