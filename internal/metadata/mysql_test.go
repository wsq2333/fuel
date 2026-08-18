package metadata

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"fuel/api"
	"fuel/internal/objectstore"
)

// newMysqlTestEngine 构造 sqlmock + mock ObjectStore 的引擎。
func newMysqlTestEngine(t *testing.T) (*mysqlEngine, sqlmock.Sqlmock, api.ObjectStore) {
	t.Helper()

	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.MonitorPingsOption(true),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

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

	eng := newMysqlEngineWithDB("test-bucket", store, db)
	return eng, mock, store
}

func metaRow(path string, size int64, etag string, isDir bool, contentType string) *sqlmock.Rows {
	mode := uint32(0o644)
	if isDir {
		mode = 0o755
	}
	return sqlmock.NewRows([]string{"size", "etag", "mtime", "is_dir", "content_type", "mode", "uid", "gid"}).
		AddRow(size, etag, time.Now(), isDir, contentType, mode, 1000, 1000)
}

func getAttrSQL() string {
	return `SELECT size, etag, mtime, is_dir, content_type, mode, uid, gid` +
		` FROM fuel_meta WHERE bucket = ? AND path = ?`
}

func upsertSQL() string {
	return `INSERT INTO fuel_meta (bucket, path, size, etag, mtime, is_dir, content_type, mode, uid, gid)` +
		` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE` +
		` size = VALUES(size), etag = VALUES(etag), mtime = VALUES(mtime),` +
		` is_dir = VALUES(is_dir), content_type = VALUES(content_type),` +
		` mode = VALUES(mode), uid = VALUES(uid), gid = VALUES(gid)`
}

func deleteMetaSQL() string {
	return `DELETE FROM fuel_meta WHERE bucket = ? AND path = ?`
}

func selectDentriesSQL() string {
	return `SELECT child_name, is_dir, size, etag, mtime, content_type` +
		` FROM fuel_dentries WHERE bucket = ? AND dir_path = ? ORDER BY child_name`
}

func deleteDentriesSQL() string {
	return `DELETE FROM fuel_dentries WHERE bucket = ? AND dir_path = ?`
}

func insertDentrySQL() string {
	return `INSERT INTO fuel_dentries (bucket, dir_path, child_name, is_dir, size, etag, mtime, content_type)` +
		` VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
}

func batchGetAttrSQL(n int) string {
	return `SELECT path, size, etag, mtime, is_dir, content_type, mode, uid, gid` +
		` FROM fuel_meta WHERE bucket = ? AND path IN (` + placeholders(n) + `)`
}

func invalidateMetaSQL() string {
	return `DELETE FROM fuel_meta WHERE bucket = ? AND (path = ? OR path LIKE ? ESCAPE '\')`
}

func invalidateDentriesSQL() string {
	return `DELETE FROM fuel_dentries WHERE bucket = ? AND (dir_path = ? OR dir_path LIKE ? ESCAPE '\')`
}

func invalidateRootMetaSQL() string {
	return `DELETE FROM fuel_meta WHERE bucket = ?`
}

func invalidateRootDentriesSQL() string {
	return `DELETE FROM fuel_dentries WHERE bucket = ?`
}

// TestMysqlEngine_GetAttr_Hit SELECT 命中直接返回。
func TestMysqlEngine_GetAttr_Hit(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "train/f1.jpg").
		WillReturnRows(metaRow("train/f1.jpg", 5, "etag-cached", false, "image/jpeg"))

	m, err := eng.GetAttr(ctx, "/train/f1.jpg")
	if err != nil {
		t.Fatalf("GetAttr: %v", err)
	}
	if m.Size != 5 || m.ETag != "etag-cached" || m.IsDir {
		t.Errorf("unexpected meta: %+v", m)
	}
	if m.ContentType != "image/jpeg" {
		t.Errorf("content_type mismatch: %q", m.ContentType)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_GetAttr_Backfill miss 回源，UPSERT 写回，二次 SELECT 命中。
func TestMysqlEngine_GetAttr_Backfill(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "train/f1.jpg").WillReturnError(sql.ErrNoRows)

	mock.ExpectExec(upsertSQL()).WillReturnResult(sqlmock.NewResult(1, 1))

	m1, err := eng.GetAttr(ctx, "/train/f1.jpg")
	if err != nil {
		t.Fatalf("first GetAttr: %v", err)
	}
	if m1.Size != 5 || m1.IsDir {
		t.Errorf("unexpected meta: size=%d isDir=%v", m1.Size, m1.IsDir)
	}

	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "train/f1.jpg").
		WillReturnRows(metaRow("train/f1.jpg", 999, "etag-from-mysql", false, ""))

	m2, err := eng.GetAttr(ctx, "/train/f1.jpg")
	if err != nil {
		t.Fatalf("second GetAttr: %v", err)
	}
	if m2.Size != 999 || m2.ETag != "etag-from-mysql" {
		t.Errorf("second GetAttr should hit MySQL, got %+v", m2)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_GetAttr_MySQLDown MySQL 不可用时降级直查对象存储 (INV-4)。
func TestMysqlEngine_GetAttr_MySQLDown(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)

	mock.ExpectQuery(getAttrSQL()).
		WillReturnError(errors.New("connection refused"))

	m, err := eng.GetAttr(context.Background(), "/train/f1.jpg")
	if err != nil {
		t.Fatalf("GetAttr with mysql down should degrade to direct: %v", err)
	}
	if m.Size != 5 {
		t.Errorf("expected size 5 from direct fallback, got %d", m.Size)
	}
}

// TestMysqlEngine_SetAttr UPSERT 写入 meta。
func TestMysqlEngine_SetAttr(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectExec(upsertSQL()).WillReturnResult(sqlmock.NewResult(1, 1))

	entry := &api.MetaEntry{Path: "new/file", Size: 10, Mode: 0o644, ETag: "e1", MTime: time.Now()}
	if err := eng.SetAttr(ctx, "new/file", entry); err != nil {
		t.Fatalf("SetAttr: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_DeleteAttr 删除 meta 行。
func TestMysqlEngine_DeleteAttr(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectExec(deleteMetaSQL()).
		WithArgs("test-bucket", "train/f1.jpg").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := eng.DeleteAttr(ctx, "train/f1.jpg"); err != nil {
		t.Fatalf("DeleteAttr: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_ListDir_Hit dentries 命中直接返回。
func TestMysqlEngine_ListDir_Hit(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectQuery(selectDentriesSQL()).
		WithArgs("test-bucket", "train").
		WillReturnRows(sqlmock.NewRows([]string{"child_name", "is_dir", "size", "etag", "mtime", "content_type"}).
			AddRow("f1.jpg", false, 5, "e1", time.Now(), "image/jpeg").
			AddRow("f2.jpg", false, 5, "e2", time.Now(), "image/jpeg").
			AddRow("sub", true, 0, "", time.Now(), ""))

	entries, err := eng.ListDir(ctx, "train")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Name != "f1.jpg" || entries[2].Name != "sub" {
		t.Errorf("unexpected order: %v", entries)
	}
	if entries[2].IsDir != true {
		t.Error("sub should be dir")
	}
	if entries[0].Meta.Size != 5 || entries[0].Meta.ETag != "e1" {
		t.Errorf("unexpected meta: %+v", entries[0].Meta)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_ListDir_Backfill miss 回源 List 并写回 dentries (事务)。
func TestMysqlEngine_ListDir_Backfill(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectQuery(selectDentriesSQL()).
		WithArgs("test-bucket", "train").WillReturnError(sql.ErrNoRows)

	mock.ExpectBegin()
	mock.ExpectExec(deleteDentriesSQL()).
		WithArgs("test-bucket", "train").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectPrepare(insertDentrySQL())
	mock.ExpectExec(insertDentrySQL()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(insertDentrySQL()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(insertDentrySQL()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	entries, err := eng.ListDir(ctx, "train")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries (f1,f2,sub), got %d", len(entries))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_SetDir_DeleteDir SetDir/DeleteDir 基本语义。
func TestMysqlEngine_SetDir_DeleteDir(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectExec(deleteDentriesSQL()).
		WithArgs("test-bucket", "d").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectPrepare(insertDentrySQL())
	mock.ExpectExec(insertDentrySQL()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	entries := []api.DirEntry{
		{Name: "a", Meta: &api.MetaEntry{Path: "d/a", Size: 1}},
	}
	if err := eng.SetDir(ctx, "d", entries); err != nil {
		t.Fatalf("SetDir: %v", err)
	}

	mock.ExpectExec(deleteDentriesSQL()).
		WithArgs("test-bucket", "d").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := eng.DeleteDir(ctx, "d"); err != nil {
		t.Fatalf("DeleteDir: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_BatchGetAttr 混合 hit/miss: hit 走 MySQL, miss 回源。
func TestMysqlEngine_BatchGetAttr(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectQuery(batchGetAttrSQL(2)).
		WithArgs("test-bucket", "train/f1.jpg", "train/f2.jpg").
		WillReturnRows(sqlmock.NewRows(
			[]string{"path", "size", "etag", "mtime", "is_dir", "content_type", "mode", "uid", "gid"},
		).AddRow("train/f1.jpg", 5, "etag-cached", time.Now(), false, "", 0o644, 1000, 1000))

	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "train/f2.jpg").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(upsertSQL()).WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := eng.BatchGetAttr(ctx, []string{"train/f1.jpg", "train/f2.jpg"})
	if err != nil {
		t.Fatalf("BatchGetAttr: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result["train/f1.jpg"].ETag != "etag-cached" {
		t.Errorf("f1 should hit MySQL cache, got etag %q", result["train/f1.jpg"].ETag)
	}
	if result["train/f2.jpg"].Size != 5 {
		t.Errorf("f2 should backfill from store, got size %d", result["train/f2.jpg"].Size)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_Invalidate 级联失效: meta 自身+前缀 + dentries 自身+前缀 + 父目录 dentries。
func TestMysqlEngine_Invalidate(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectExec(invalidateMetaSQL()).
		WithArgs("test-bucket", "train", "train/%").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec(invalidateDentriesSQL()).
		WithArgs("test-bucket", "train", "train/%").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(deleteDentriesSQL()).
		WithArgs("test-bucket", "").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := eng.Invalidate(ctx, "train"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_Invalidate_Root 根路径失效清空该 bucket 全部缓存。
func TestMysqlEngine_Invalidate_Root(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectExec(invalidateRootMetaSQL()).
		WithArgs("test-bucket").WillReturnResult(sqlmock.NewResult(0, 5))
	mock.ExpectExec(invalidateRootDentriesSQL()).
		WithArgs("test-bucket").WillReturnResult(sqlmock.NewResult(0, 3))

	if err := eng.Invalidate(ctx, "/"); err != nil {
		t.Fatalf("Invalidate root: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_Invalidate_LikeEscape path 含 LIKE 通配符时正确转义。
func TestMysqlEngine_Invalidate_LikeEscape(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectExec(invalidateMetaSQL()).
		WithArgs("test-bucket", "100%_complete", `100\%\_complete/%`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(invalidateDentriesSQL()).
		WithArgs("test-bucket", "100%_complete", `100\%\_complete/%`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(deleteDentriesSQL()).
		WithArgs("test-bucket", "").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := eng.Invalidate(ctx, "100%_complete"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_HealthCheck PING 成功/失败。
func TestMysqlEngine_HealthCheck(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)

	mock.ExpectPing()
	if err := eng.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck should pass, got %v", err)
	}

	mock.ExpectPing().WillReturnError(errors.New("connection refused"))
	if err := eng.HealthCheck(context.Background()); err == nil {
		t.Error("HealthCheck should fail when mysql down")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_GetAttr_ENOENT 对象不存在时透传 ENOENT。
func TestMysqlEngine_GetAttr_ENOENT(t *testing.T) {
	eng, mock, _ := newMysqlTestEngine(t)
	ctx := context.Background()

	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "ghost").WillReturnError(sql.ErrNoRows)

	_, err := eng.GetAttr(ctx, "ghost")
	if !errors.Is(err, syscall.ENOENT) {
		t.Errorf("expected ENOENT, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMysqlEngine_PersistenceAcrossRestart 模拟进程重启，
// 新引擎实例读同一 DB 直接命中 (PLAN §7.1 验证: 持久化)。
func TestMysqlEngine_PersistenceAcrossRestart(t *testing.T) {
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	store := objectstore.NewMockStore("test-bucket")
	ctx := context.Background()
	_, _ = store.Put(ctx, "persist/f", bytes.NewReader([]byte("x")), 1)

	eng1 := newMysqlEngineWithDB("test-bucket", store, db)
	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "persist/f").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(upsertSQL()).WillReturnResult(sqlmock.NewResult(1, 1))
	m1, err := eng1.GetAttr(ctx, "persist/f")
	if err != nil {
		t.Fatalf("process1 GetAttr: %v", err)
	}

	eng2 := newMysqlEngineWithDB("test-bucket", store, db)
	mock.ExpectQuery(getAttrSQL()).
		WithArgs("test-bucket", "persist/f").
		WillReturnRows(metaRow("persist/f", m1.Size, m1.ETag, false, ""))
	m2, err := eng2.GetAttr(ctx, "persist/f")
	if err != nil {
		t.Fatalf("process2 GetAttr: %v", err)
	}
	if m2.ETag != m1.ETag {
		t.Errorf("etag should survive restart: process1=%q process2=%q", m1.ETag, m2.ETag)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}