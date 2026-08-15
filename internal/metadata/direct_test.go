package metadata

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"fuel/api"
	"fuel/internal/config"
	"fuel/internal/objectstore"
)

// newTestEngine 构造一个基于内存 mock ObjectStore 的 direct 引擎。
// 预置对象布局:
//   train/f1.jpg            (文件)
//   train/f2.jpg            (文件)
//   train/sub/g1.jpg        (使 train/sub 成为隐式目录)
//   explicit/               (显式目录标记, 0 字节对象)
func newTestEngine(t *testing.T) api.MetadataEngine {
	t.Helper()
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

	cfg := &config.Config{}
	cfg.Metadata.Engine = "direct"
	eng, err := NewMetadataEngine(cfg, store)
	if err != nil {
		t.Fatalf("NewMetadataEngine failed: %v", err)
	}
	return eng
}

func TestDirectEngine_GetAttr_File(t *testing.T) {
	eng := newTestEngine(t)
	m, err := eng.GetAttr(context.Background(), "/train/f1.jpg")
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if m.IsDir {
		t.Error("expected file, got dir")
	}
	if m.Size != 5 {
		t.Errorf("expected size 5, got %d", m.Size)
	}
	if m.Mode != 0o644 {
		t.Errorf("expected mode 0644, got %#o", m.Mode)
	}
	if m.Nlink != 1 {
		t.Errorf("expected nlink 1, got %d", m.Nlink)
	}
	if m.Path != "train/f1.jpg" {
		t.Errorf("expected path train/f1.jpg, got %q", m.Path)
	}
}

func TestDirectEngine_GetAttr_Root(t *testing.T) {
	eng := newTestEngine(t)
	m, err := eng.GetAttr(context.Background(), "/")
	if err != nil {
		t.Fatalf("GetAttr root failed: %v", err)
	}
	if !m.IsDir {
		t.Error("root must be dir")
	}
	if m.Nlink != 2 {
		t.Errorf("expected nlink 2 for root, got %d", m.Nlink)
	}
}

func TestDirectEngine_GetAttr_ExplicitDir(t *testing.T) {
	eng := newTestEngine(t)
	// explicit/ 是显式目录标记对象
	m, err := eng.GetAttr(context.Background(), "/explicit")
	if err != nil {
		t.Fatalf("GetAttr explicit dir failed: %v", err)
	}
	if !m.IsDir {
		t.Error("expected dir for explicit marker")
	}
	if m.Mode != 0o755 {
		t.Errorf("expected mode 0755, got %#o", m.Mode)
	}
}

func TestDirectEngine_GetAttr_ImplicitDir(t *testing.T) {
	eng := newTestEngine(t)
	// train/sub 没有实体对象，但 train/sub/ 前缀下有 g1.jpg → 隐式目录
	m, err := eng.GetAttr(context.Background(), "/train/sub")
	if err != nil {
		t.Fatalf("GetAttr implicit dir failed: %v", err)
	}
	if !m.IsDir {
		t.Error("expected dir for implicit prefix")
	}
}

func TestDirectEngine_GetAttr_ImplicitDirWithSlash(t *testing.T) {
	eng := newTestEngine(t)
	m, err := eng.GetAttr(context.Background(), "/train/")
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if !m.IsDir {
		t.Error("expected dir for train/")
	}
}

func TestDirectEngine_GetAttr_NotFound(t *testing.T) {
	eng := newTestEngine(t)
	_, err := eng.GetAttr(context.Background(), "/nonexistent")
	if !errors.Is(err, syscall.ENOENT) {
		t.Errorf("expected ENOENT, got %v", err)
	}
}

func TestDirectEngine_ListDir(t *testing.T) {
	eng := newTestEngine(t)
	entries, err := eng.ListDir(context.Background(), "/train")
	if err != nil {
		t.Fatalf("ListDir failed: %v", err)
	}

	byName := map[string]api.DirEntry{}
	for _, e := range entries {
		byName[e.Name] = e
	}

	// 直接文件子项
	f1, ok := byName["f1.jpg"]
	if !ok {
		t.Fatalf("expected f1.jpg in listdir, got %+v", entries)
	}
	if f1.IsDir {
		t.Error("f1.jpg should be file")
	}
	if f1.Meta == nil || f1.Meta.Size != 5 {
		t.Errorf("f1.jpg inline meta wrong: %+v", f1.Meta)
	}

	if _, ok := byName["f2.jpg"]; !ok {
		t.Errorf("expected f2.jpg in listdir")
	}

	// 子目录 sub
	sub, ok := byName["sub"]
	if !ok {
		t.Fatalf("expected sub dir in listdir, got %+v", entries)
	}
	if !sub.IsDir {
		t.Error("sub should be dir")
	}
	if sub.Meta == nil || !sub.Meta.IsDir {
		t.Errorf("sub inline meta should be dir: %+v", sub.Meta)
	}
}

func TestDirectEngine_ListDir_FiltersDirMarker(t *testing.T) {
	eng := newTestEngine(t)
	entries, err := eng.ListDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("ListDir root failed: %v", err)
	}
	// 根下应有 train/ 和 explicit/ 两个子目录，不含 "explicit/" 这种名字的文件项
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
		if e.Name == "" {
			t.Error("empty entry name")
		}
	}
	if !names["train"] {
		t.Errorf("expected train dir at root, got %+v", entries)
	}
	if !names["explicit"] {
		t.Errorf("expected explicit dir at root, got %+v", entries)
	}
}

func TestDirectEngine_BatchGetAttr(t *testing.T) {
	eng := newTestEngine(t)
	result, err := eng.BatchGetAttr(context.Background(), []string{
		"/train/f1.jpg", "/train/sub", "/missing",
	})
	if err != nil {
		t.Fatalf("BatchGetAttr failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 results (missing skipped), got %d", len(result))
	}
	if m, ok := result["/train/f1.jpg"]; !ok || m.IsDir {
		t.Errorf("f1.jpg missing or wrong: %+v", m)
	}
	if m, ok := result["/train/sub"]; !ok || !m.IsDir {
		t.Errorf("train/sub missing or wrong: %+v", m)
	}
	if _, ok := result["/missing"]; ok {
		t.Error("missing path should not appear in result")
	}
}

func TestDirectEngine_NoOpMethods(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()

	if err := eng.SetAttr(ctx, "/x", &api.MetaEntry{}); err != nil {
		t.Errorf("SetAttr should be no-op, got %v", err)
	}
	if err := eng.DeleteAttr(ctx, "/x"); err != nil {
		t.Errorf("DeleteAttr should be no-op, got %v", err)
	}
	if err := eng.SetDir(ctx, "/x", nil); err != nil {
		t.Errorf("SetDir should be no-op, got %v", err)
	}
	if err := eng.DeleteDir(ctx, "/x"); err != nil {
		t.Errorf("DeleteDir should be no-op, got %v", err)
	}
	if err := eng.Invalidate(ctx, "/x"); err != nil {
		t.Errorf("Invalidate should be no-op, got %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Errorf("Close should be no-op, got %v", err)
	}
}

func TestDirectEngine_HealthCheck(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck failed: %v", err)
	}
}

func TestNewMetadataEngine_Unsupported(t *testing.T) {
	cfg := &config.Config{}
	cfg.Metadata.Engine = "no-such-engine"
	store := objectstore.NewMockStore("b")
	if _, err := NewMetadataEngine(cfg, store); err == nil {
		t.Fatal("expected error for unsupported engine, got nil")
	}
}

func TestNewDirectEngine_NilStore(t *testing.T) {
	cfg := &config.Config{}
	if _, err := newDirectEngine(cfg, nil); err == nil {
		t.Fatal("expected error for nil store, got nil")
	}
}

// errStore 包装 ObjectStore，对指定操作注入非 ENOENT 错误，用于测试错误传播路径。
type errStore struct {
	api.ObjectStore
	headErr error
	listErr error
}

func (s *errStore) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	if s.headErr != nil {
		return nil, s.headErr
	}
	return s.ObjectStore.Head(ctx, key)
}

func (s *errStore) List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]api.ObjectEntry, []string, error) {
	if s.listErr != nil {
		return nil, nil, s.listErr
	}
	return s.ObjectStore.List(ctx, prefix, delimiter, maxKeys)
}

func TestDirectEngine_GetAttr_HeadError(t *testing.T) {
	store := objectstore.NewMockStore("b")
	wrapped := &errStore{ObjectStore: store, headErr: syscall.EACCES}
	eng, err := newDirectEngine(&config.Config{}, wrapped)
	if err != nil {
		t.Fatalf("newDirectEngine failed: %v", err)
	}
	_, err = eng.GetAttr(context.Background(), "/train/f1.jpg")
	if !errors.Is(err, syscall.EACCES) {
		t.Errorf("expected EACCES propagation, got %v", err)
	}
}

func TestDirectEngine_GetAttr_ListError(t *testing.T) {
	store := objectstore.NewMockStore("b")
	// Head 全部 ENOENT，List 返回错误
	wrapped := &errStore{ObjectStore: store, listErr: syscall.EIO}
	eng, err := newDirectEngine(&config.Config{}, wrapped)
	if err != nil {
		t.Fatalf("newDirectEngine failed: %v", err)
	}
	_, err = eng.GetAttr(context.Background(), "/somewhere")
	if !errors.Is(err, syscall.EIO) {
		t.Errorf("expected EIO propagation, got %v", err)
	}
}

func TestDirectEngine_ListDir_Error(t *testing.T) {
	store := objectstore.NewMockStore("b")
	wrapped := &errStore{ObjectStore: store, listErr: syscall.EIO}
	eng, err := newDirectEngine(&config.Config{}, wrapped)
	if err != nil {
		t.Fatalf("newDirectEngine failed: %v", err)
	}
	if _, err := eng.ListDir(context.Background(), "/train"); !errors.Is(err, syscall.EIO) {
		t.Errorf("expected EIO propagation, got %v", err)
	}
}

func TestDirectEngine_HealthCheck_Error(t *testing.T) {
	store := objectstore.NewMockStore("b")
	wrapped := &errStore{ObjectStore: store, listErr: syscall.EIO}
	eng, err := newDirectEngine(&config.Config{}, wrapped)
	if err != nil {
		t.Fatalf("newDirectEngine failed: %v", err)
	}
	if err := eng.HealthCheck(context.Background()); err == nil {
		t.Error("expected HealthCheck error, got nil")
	}
}

func TestDirectEngine_BatchGetAttr_Error(t *testing.T) {
	store := objectstore.NewMockStore("b")
	wrapped := &errStore{ObjectStore: store, headErr: syscall.EIO}
	eng, err := newDirectEngine(&config.Config{}, wrapped)
	if err != nil {
		t.Fatalf("newDirectEngine failed: %v", err)
	}
	if _, err := eng.BatchGetAttr(context.Background(), []string{"/a"}); !errors.Is(err, syscall.EIO) {
		t.Errorf("expected EIO propagation, got %v", err)
	}
}
