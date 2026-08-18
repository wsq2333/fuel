package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"fuel/api"
	"fuel/internal/objectstore"
)

// newRedisTestEngine 构造 miniredis 内存 Redis + mock ObjectStore 的引擎。
// 预置对象:
//   train/f1.jpg            (文件)
//   train/f2.jpg            (文件)
//   train/sub/g1.jpg        (使 train/sub 成为隐式目录)
//   explicit/               (显式目录标记, 0 字节对象)
func newRedisTestEngine(t *testing.T) (*redisEngine, *miniredis.Miniredis, api.ObjectStore) {
	t.Helper()
	mr := miniredis.RunT(t)

	store := objectstore.NewMockStore("test-bucket")
	ctx := context.Background()
	files := map[string]string{
		"train/f1.jpg":     "data1",
		"train/f2.jpg":     "data2",
		"train/sub/g1.jpg": "data3",
		"explicit/":        "",
	}
	for k, v := range files {
		if _, err := store.Put(ctx, k, bytes.NewReader([]byte(v)), int64(len(v))); err != nil {
			t.Fatalf("seed put %s: %v", k, err)
		}
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	eng := newRedisEngineWithClient("test-bucket", store, client)
	t.Cleanup(func() { _ = eng.Close() })
	return eng, mr, store
}

// TestRedisEngine_GetAttr_Backfill 首次 GetAttr 回源对象存储并写回 Redis，
// 二次 GetAttr 直接命中 Redis（PLAN §6.1 验证: 元数据回填）。
func TestRedisEngine_GetAttr_Backfill(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	m1, err := eng.GetAttr(ctx, "/train/f1.jpg")
	if err != nil {
		t.Fatalf("first GetAttr: %v", err)
	}
	if m1.Size != 5 || m1.IsDir {
		t.Errorf("unexpected meta: size=%d isDir=%v", m1.Size, m1.IsDir)
	}

	if !mr.Exists(eng.metaKey("train/f1.jpg")) {
		t.Fatal("meta key should be written back to Redis after first GetAttr")
	}

	mr.Set(eng.metaKey("train/f1.jpg"), mustMarshalMeta(t, &api.MetaEntry{
		Path: "train/f1.jpg", Size: 999, Mode: 0o644,
	}))
	m2, err := eng.GetAttr(ctx, "/train/f1.jpg")
	if err != nil {
		t.Fatalf("second GetAttr: %v", err)
	}
	if m2.Size != 999 {
		t.Errorf("second GetAttr should hit Redis (size 999), got %d", m2.Size)
	}
}

// TestRedisEngine_GetAttr_NegCache ENOENT 写入负缓存，二次直接返回 ENOENT 不回源。
func TestRedisEngine_GetAttr_NegCache(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	_, err := eng.GetAttr(ctx, "/ghost")
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected ENOENT, got %v", err)
	}
	if !mr.Exists(eng.negKey("ghost")) {
		t.Fatal("neg key should be written after ENOENT")
	}
	if ttl := mr.TTL(eng.negKey("ghost")); ttl != redisNegTTL {
		t.Errorf("neg TTL should be %v, got %v", redisNegTTL, ttl)
	}
}

// TestRedisEngine_GetAttr_RedisDown Redis 不可用时降级直查对象存储 (INV-4)。
func TestRedisEngine_GetAttr_RedisDown(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	mr.Close()

	m, err := eng.GetAttr(context.Background(), "/train/f1.jpg")
	if err != nil {
		t.Fatalf("GetAttr with redis down should degrade to direct: %v", err)
	}
	if m.Size != 5 {
		t.Errorf("expected size 5 from direct fallback, got %d", m.Size)
	}
}

// TestRedisEngine_GetAttr_CorruptJSON Redis 中 JSON 损坏时视为 miss 回源覆盖 (INV-9)。
func TestRedisEngine_GetAttr_CorruptJSON(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.metaKey("train/f1.jpg"), "{not-json")

	m, err := eng.GetAttr(ctx, "/train/f1.jpg")
	if err != nil {
		t.Fatalf("GetAttr with corrupt JSON should refetch: %v", err)
	}
	if m.Size != 5 {
		t.Errorf("expected size 5 from refetch, got %d", m.Size)
	}
}

// TestRedisEngine_SetAttr_DelNeg SetAttr 写入 meta 并清除负缓存 (PLAN §6.1 写路径)。
func TestRedisEngine_SetAttr_DelNeg(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.negKey("new/file"), "1")

	entry := &api.MetaEntry{Path: "new/file", Size: 10, Mode: 0o644, ETag: "e1"}
	if err := eng.SetAttr(ctx, "new/file", entry); err != nil {
		t.Fatalf("SetAttr: %v", err)
	}

	if !mr.Exists(eng.metaKey("new/file")) {
		t.Error("meta key should exist after SetAttr")
	}
	if mr.Exists(eng.negKey("new/file")) {
		t.Error("neg key should be deleted after SetAttr")
	}

	m, err := eng.GetAttr(ctx, "new/file")
	if err != nil || m.ETag != "e1" {
		t.Errorf("GetAttr after SetAttr: m=%v err=%v", m, err)
	}
}

// TestRedisEngine_DeleteAttr 删除 meta 并写入 60s 负缓存。
func TestRedisEngine_DeleteAttr(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	if err := eng.SetAttr(ctx, "train/f1.jpg", &api.MetaEntry{Path: "train/f1.jpg", Size: 5}); err != nil {
		t.Fatalf("SetAttr: %v", err)
	}
	if err := eng.DeleteAttr(ctx, "train/f1.jpg"); err != nil {
		t.Fatalf("DeleteAttr: %v", err)
	}

	if mr.Exists(eng.metaKey("train/f1.jpg")) {
		t.Error("meta key should be deleted")
	}
	if !mr.Exists(eng.negKey("train/f1.jpg")) {
		t.Error("neg key should be set after DeleteAttr")
	}
}

// TestRedisEngine_ListDir_Backfill ListDir miss 回源并写回，二次命中 Redis。
func TestRedisEngine_ListDir_Backfill(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	entries, err := eng.ListDir(ctx, "train")
	if err != nil {
		t.Fatalf("first ListDir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries (f1,f2,sub), got %d", len(entries))
	}
	if !mr.Exists(eng.dirKey("train")) {
		t.Fatal("dir key should be written back")
	}

	mr.Set(eng.dirKey("train"), mustMarshalDir(t, []api.DirEntry{
		{Name: "only", Meta: &api.MetaEntry{Path: "train/only", Size: 1}},
	}))
	entries, err = eng.ListDir(ctx, "train")
	if err != nil {
		t.Fatalf("second ListDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "only" {
		t.Errorf("second ListDir should hit Redis, got %v", entries)
	}
}

// TestRedisEngine_SetDir_DeleteDir SetDir/DeleteDir 基本语义。
func TestRedisEngine_SetDir_DeleteDir(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	entries := []api.DirEntry{
		{Name: "a", Meta: &api.MetaEntry{Path: "d/a", Size: 1}},
		{Name: "b", Meta: &api.MetaEntry{Path: "d/b", Size: 2}},
	}
	if err := eng.SetDir(ctx, "d", entries); err != nil {
		t.Fatalf("SetDir: %v", err)
	}
	got, err := eng.ListDir(ctx, "d")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Errorf("ListDir after SetDir: %v", got)
	}

	if err := eng.DeleteDir(ctx, "d"); err != nil {
		t.Fatalf("DeleteDir: %v", err)
	}
	if mr.Exists(eng.dirKey("d")) {
		t.Error("dir key should be deleted")
	}
}

// TestRedisEngine_BatchGetAttr 混合 hit/miss: hit 走 Redis, miss 回源并写回。
func TestRedisEngine_BatchGetAttr(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	if err := eng.SetAttr(ctx, "train/f1.jpg", &api.MetaEntry{Path: "train/f1.jpg", Size: 5, ETag: "cached"}); err != nil {
		t.Fatalf("SetAttr: %v", err)
	}

	result, err := eng.BatchGetAttr(ctx, []string{"train/f1.jpg", "train/f2.jpg", "ghost"})
	if err != nil {
		t.Fatalf("BatchGetAttr: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results (ghost ENOENT excluded), got %d", len(result))
	}
	if result["train/f1.jpg"].ETag != "cached" {
		t.Errorf("f1 should hit Redis cache, got etag %q", result["train/f1.jpg"].ETag)
	}
	if result["train/f2.jpg"].Size != 5 {
		t.Errorf("f2 should backfill from store, got size %d", result["train/f2.jpg"].Size)
	}
	if !mr.Exists(eng.metaKey("train/f2.jpg")) {
		t.Error("f2 should be written back to Redis after BatchGetAttr miss")
	}
	if !mr.Exists(eng.negKey("ghost")) {
		t.Error("ghost should get neg cache after BatchGetAttr")
	}
}

// TestRedisEngine_Invalidate 级联失效: 自身三 key + 子路径 + 父目录列表。
func TestRedisEngine_Invalidate(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.metaKey("train"), mustMarshalMeta(t, &api.MetaEntry{Path: "train", IsDir: true}))
	mr.Set(eng.dirKey("train"), mustMarshalDir(t, []api.DirEntry{{Name: "f1.jpg"}}))
	mr.Set(eng.negKey("train"), "1")
	mr.Set(eng.metaKey("train/f1.jpg"), mustMarshalMeta(t, &api.MetaEntry{Path: "train/f1.jpg"}))
	mr.Set(eng.dirKey("train/sub"), mustMarshalDir(t, []api.DirEntry{{Name: "g1.jpg"}}))
	mr.Set(eng.negKey("train/ghost"), "1")
	mr.Set(eng.dirKey(""), mustMarshalDir(t, []api.DirEntry{{Name: "train"}}))
	mr.Set(eng.metaKey("other/x"), mustMarshalMeta(t, &api.MetaEntry{Path: "other/x"}))

	if err := eng.Invalidate(ctx, "train"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	gone := []string{
		eng.metaKey("train"), eng.dirKey("train"), eng.negKey("train"),
		eng.metaKey("train/f1.jpg"), eng.dirKey("train/sub"), eng.negKey("train/ghost"),
		eng.dirKey(""),
	}
	for _, k := range gone {
		if mr.Exists(k) {
			t.Errorf("key %s should be invalidated", k)
		}
	}
	if !mr.Exists(eng.metaKey("other/x")) {
		t.Error("unrelated key meta:other/x should survive")
	}
}

// TestRedisEngine_Invalidate_Root 根路径失效清空该 bucket 全部缓存。
func TestRedisEngine_Invalidate_Root(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	ctx := context.Background()

	mr.Set(eng.metaKey("a"), mustMarshalMeta(t, &api.MetaEntry{Path: "a"}))
	mr.Set(eng.dirKey("a/b"), mustMarshalDir(t, nil))
	mr.Set(eng.negKey("c"), "1")

	if err := eng.Invalidate(ctx, "/"); err != nil {
		t.Fatalf("Invalidate root: %v", err)
	}
	if len(mr.Keys()) != 0 {
		t.Errorf("all keys should be cleared, got %v", mr.Keys())
	}
}

// TestRedisEngine_HealthCheck PING 成功/失败。
func TestRedisEngine_HealthCheck(t *testing.T) {
	eng, mr, _ := newRedisTestEngine(t)
	if err := eng.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck should pass, got %v", err)
	}
	mr.Close()
	if err := eng.HealthCheck(context.Background()); err == nil {
		t.Error("HealthCheck should fail when redis down")
	}
}

// TestRedisEngine_CrossNodeShare 两个引擎实例共享同一 Redis:
// 节点 A SetAttr 后, 节点 B GetAttr 命中 (PLAN §6.1 跨节点共享验证)。
func TestRedisEngine_CrossNodeShare(t *testing.T) {
	engA, mr, store := newRedisTestEngine(t)
	ctx := context.Background()

	entry := &api.MetaEntry{Path: "shared/f", Size: 42, ETag: "from-node-a", Mode: 0o644}
	if err := engA.SetAttr(ctx, "shared/f", entry); err != nil {
		t.Fatalf("node A SetAttr: %v", err)
	}

	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	engB := newRedisEngineWithClient("test-bucket", store, clientB)
	defer engB.Close()

	m, err := engB.GetAttr(ctx, "shared/f")
	if err != nil {
		t.Fatalf("node B GetAttr: %v", err)
	}
	if m.ETag != "from-node-a" || m.Size != 42 {
		t.Errorf("node B should read node A's write, got %+v", m)
	}
}

// TestRedisEngine_NegCacheExpiry 负缓存 TTL 到期后自动失效，回源重查。
func TestRedisEngine_NegCacheExpiry(t *testing.T) {
	eng, mr, store := newRedisTestEngine(t)
	ctx := context.Background()

	_, err := eng.GetAttr(ctx, "late")
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected ENOENT, got %v", err)
	}

	if _, err := store.Put(ctx, "late", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatalf("put late: %v", err)
	}

	// neg 未过期 → 仍 ENOENT（最终一致，TTL 60s 内）
	if _, err := eng.GetAttr(ctx, "late"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("before TTL expiry should still ENOENT, got %v", err)
	}

	// TTL 过期后 → 回源查到新对象
	mr.FastForward(redisNegTTL + time.Second)
	m, err := eng.GetAttr(ctx, "late")
	if err != nil {
		t.Fatalf("after TTL expiry should refetch, got %v", err)
	}
	if m.Size != 1 {
		t.Errorf("expected size 1, got %d", m.Size)
	}
}

// mustMarshalMeta 测试辅助: JSON 序列化 MetaEntry。
func mustMarshalMeta(t *testing.T, e *api.MetaEntry) string {
	t.Helper()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	return string(data)
}

// mustMarshalDir 测试辅助: JSON 序列化 []DirEntry。
func mustMarshalDir(t *testing.T, entries []api.DirEntry) string {
	t.Helper()
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("marshal dir: %v", err)
	}
	return string(data)
}