package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"syscall"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"fuel/api"
	"fuel/internal/objectstore"
)

// TestRedisEngine_GetAttr_RootPath 根路径返回目录元数据。
func TestRedisEngine_GetAttr_RootPath(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	m, err := eng.GetAttr(context.Background(), "/")
	if err != nil {
		t.Fatalf("GetAttr root: %v", err)
	}
	if !m.IsDir {
		t.Error("root should be a directory")
	}
}

// TestRedisEngine_GetAttr_EmptyPath 空路径视为根。
func TestRedisEngine_GetAttr_EmptyPath(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	m, err := eng.GetAttr(context.Background(), "")
	if err != nil {
		t.Fatalf("GetAttr empty: %v", err)
	}
	if !m.IsDir {
		t.Error("empty path should be treated as root dir")
	}
}

// TestRedisEngine_GetAttr_ConcurrentAccess 多 goroutine 并发 GetAttr 不 panic。
func TestRedisEngine_GetAttr_ConcurrentAccess(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = eng.GetAttr(ctx, "/train/f1.jpg")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

// TestRedisEngine_ListDir_CorruptJSON dir key 含损坏 JSON 时回源 (INV-9)。
func TestRedisEngine_ListDir_CorruptJSON(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.dirKey("train"), "not valid json!!!!")

	entries, err := eng.ListDir(ctx, "train")
	if err != nil {
		t.Fatalf("ListDir with corrupt JSON should refetch: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries from refetch, got %d", len(entries))
	}
}

// TestRedisEngine_SetAttr_EmptyPath 根路径 SetAttr 是 no-op。
func TestRedisEngine_SetAttr_EmptyPath(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	err := eng.SetAttr(context.Background(), "/", &api.MetaEntry{Path: "/"})
	if err != nil {
		t.Errorf("SetAttr root should be no-op: %v", err)
	}
}

// TestRedisEngine_DeleteAttr_NonExistentKey 删除不存在的 key 不报错。
func TestRedisEngine_DeleteAttr_NonExistentKey(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	err := eng.DeleteAttr(context.Background(), "nonexistent/path")
	if err != nil {
		t.Errorf("DeleteAttr nonexistent should not error: %v", err)
	}
}

// TestRedisEngine_BatchGetAttr_EmptyPaths 空路径列表返回空 map。
func TestRedisEngine_BatchGetAttr_EmptyPaths(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	result, err := eng.BatchGetAttr(context.Background(), nil)
	if err != nil {
		t.Fatalf("BatchGetAttr empty: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

// TestRedisEngine_BatchGetAttr_AllMiss 全部 miss 时都回源。
func TestRedisEngine_BatchGetAttr_AllMiss(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	result, err := eng.BatchGetAttr(ctx, []string{"train/f1.jpg", "train/f2.jpg"})
	if err != nil {
		t.Fatalf("BatchGetAttr: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result["train/f1.jpg"] == nil || result["train/f2.jpg"] == nil {
		t.Error("all paths should be found via store fallback")
	}
}

// TestRedisEngine_BatchGetAttr_RedisDown 全部降级为直查。
func TestRedisEngine_BatchGetAttr_RedisDown(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	mr.Close()

	result, err := eng.BatchGetAttr(context.Background(), []string{"train/f1.jpg"})
	if err != nil {
		t.Fatalf("BatchGetAttr: %v", err)
	}
	if result["train/f1.jpg"] == nil || result["train/f1.jpg"].Size != 5 {
		t.Errorf("should degrade to direct: %+v", result["train/f1.jpg"])
	}
}

// TestRedisEngine_Invalidate_NestedPaths 深层嵌套路径失效正确。
func TestRedisEngine_Invalidate_NestedPaths(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.metaKey("a/b/c/d"), mustMarshalMeta(t, &api.MetaEntry{Path: "a/b/c/d"}))
	mr.Set(eng.metaKey("a/b/c/d/e"), mustMarshalMeta(t, &api.MetaEntry{Path: "a/b/c/d/e"}))
	mr.Set(eng.dirKey("a/b/c/d"), mustMarshalDir(t, nil))
	mr.Set(eng.metaKey("a/other"), mustMarshalMeta(t, &api.MetaEntry{Path: "a/other"}))

	if err := eng.Invalidate(ctx, "a/b/c/d"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	if mr.Exists(eng.metaKey("a/b/c/d")) {
		t.Error("a/b/c/d should be invalidated")
	}
	if mr.Exists(eng.metaKey("a/b/c/d/e")) {
		t.Error("a/b/c/d/e should be invalidated (child)")
	}
	if mr.Exists(eng.dirKey("a/b/c/d")) {
		t.Error("dir:a/b/c/d should be invalidated")
	}
	if !mr.Exists(eng.metaKey("a/other")) {
		t.Error("unrelated key a/other should survive")
	}
}

// TestRedisEngine_ListDir_RedisDown 降级直查 (INV-4)。
func TestRedisEngine_ListDir_RedisDown(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	mr.Close()

	entries, err := eng.ListDir(context.Background(), "train")
	if err != nil {
		t.Fatalf("ListDir with redis down should degrade: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries from direct fallback, got %d", len(entries))
	}
}

// TestRedisEngine_GetAttr_DirectoryDetection 隐式目录检测（有子项即为目录）。
func TestRedisEngine_GetAttr_DirectoryDetection(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	// "train" 不是对象，但 "train/" 下有子项 → 应被推断为目录
	m, err := eng.GetAttr(ctx, "train")
	if err != nil {
		t.Fatalf("GetAttr dir: %v", err)
	}
	if !m.IsDir {
		t.Error("train should be detected as directory")
	}
}

// TestRedisEngine_GetAttr_ENOENT_NotInStore 不存在的对象返回 ENOENT。
func TestRedisEngine_GetAttr_ENOENT_NotInStore(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	_, err := eng.GetAttr(ctx, "absolutely/does/not/exist")
	if !errors.Is(err, syscall.ENOENT) {
		t.Errorf("expected ENOENT for missing path, got %v", err)
	}
}

// TestRedisEngine_SetDir_Overwrite 重复 SetDir 覆盖旧数据。
func TestRedisEngine_SetDir_Overwrite(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	entries1 := []api.DirEntry{{Name: "a", Meta: &api.MetaEntry{Path: "d/a", Size: 1}}}
	if err := eng.SetDir(ctx, "d", entries1); err != nil {
		t.Fatalf("SetDir first: %v", err)
	}

	entries2 := []api.DirEntry{
		{Name: "b", Meta: &api.MetaEntry{Path: "d/b", Size: 2}},
		{Name: "c", Meta: &api.MetaEntry{Path: "d/c", Size: 3}},
	}
	if err := eng.SetDir(ctx, "d", entries2); err != nil {
		t.Fatalf("SetDir second: %v", err)
	}

	got, err := eng.ListDir(ctx, "d")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 entries after overwrite, got %d", len(got))
	}
}

// TestRedisEngine_Close_Idempotent 多次 Close 不 panic。
func TestRedisEngine_Close_Idempotent(t *testing.T) {
	mr := miniredis.RunT(t)
	store := objectstore.NewMockStore("b")
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	eng := newRedisEngineWithClient("b", store, client)

	_ = eng.Close()
	// go-redis Close is not idempotent but shouldn't panic
	_ = eng.Close()
}

// TestRedisEngine_BatchGetAttr_PartialCorruptJSON 部分 entry JSON 损坏时那些 miss 回源。
func TestRedisEngine_BatchGetAttr_PartialCorruptJSON(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	// f1 有损坏 JSON
	mr.Set(eng.metaKey("train/f1.jpg"), "{bad-json")
	// f2 没有 meta（miss）

	result, err := eng.BatchGetAttr(ctx, []string{"train/f1.jpg", "train/f2.jpg"})
	if err != nil {
		t.Fatalf("BatchGetAttr: %v", err)
	}
	// f1 的损坏 JSON 导致 miss → 回源，f2 也回源
	if result["train/f1.jpg"] == nil || result["train/f1.jpg"].Size != 5 {
		t.Errorf("f1 corrupt JSON should fallback to store, got %+v", result["train/f1.jpg"])
	}
	if result["train/f2.jpg"] == nil || result["train/f2.jpg"].Size != 5 {
		t.Errorf("f2 should fallback to store, got %+v", result["train/f2.jpg"])
	}
}

// TestRedisEngine_SetDir_NilMeta entries 含 nil Meta 时不 panic。
func TestRedisEngine_SetDir_NilMeta(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	entries := []api.DirEntry{
		{Name: "no-meta", IsDir: false, Meta: nil},
	}
	// SetDir 应该能处理 nil Meta（序列化为 null）
	if err := eng.SetDir(ctx, "d", entries); err != nil {
		t.Fatalf("SetDir with nil Meta: %v", err)
	}

	got, err := eng.ListDir(ctx, "d")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(got) != 1 || got[0].Name != "no-meta" {
		t.Errorf("expected [no-meta], got %v", got)
	}
}

// helper for this file
func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, _ := json.Marshal(v)
	return string(b)
}

// TestRedisEngine_GetAttr_TrailingSlash 路径带尾部 / 被规范化。
func TestRedisEngine_GetAttr_TrailingSlash(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	// "train/" 应被规范化为 "train"，触发目录推断
	m, err := eng.GetAttr(ctx, "train/")
	if err != nil {
		t.Fatalf("GetAttr trailing slash: %v", err)
	}
	if !m.IsDir {
		t.Error("train/ should be detected as directory")
	}
}

// --- 并发写+读竞争验证 ---

// TestRedisEngine_ConcurrentSetAttr_GetAttr 并发写读不 panic。
func TestRedisEngine_ConcurrentSetAttr_GetAttr(t *testing.T) {
	eng, _, _ := newRedisTestEngine(t)
	ctx := context.Background()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n * 2)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			_ = eng.SetAttr(ctx, "race/file", &api.MetaEntry{Path: "race/file", Size: int64(idx)})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = eng.GetAttr(ctx, "race/file")
		}()
	}
	wg.Wait()
}

// TestRedisEngine_Invalidate_SelfIsFile 失效单个文件（无子路径）也正确。
func TestRedisEngine_Invalidate_SelfIsFile(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.metaKey("single"), mustMarshalMeta(t, &api.MetaEntry{Path: "single", Size: 1}))
	mr.Set(eng.dirKey(""), mustMarshalDir(t, []api.DirEntry{{Name: "single"}}))

	if err := eng.Invalidate(ctx, "single"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if mr.Exists(eng.metaKey("single")) {
		t.Error("meta should be invalidated")
	}
	// 父目录 (root) 的 dir cache 也应失效
	if mr.Exists(eng.dirKey("")) {
		t.Error("parent dir cache should be invalidated")
	}
}
