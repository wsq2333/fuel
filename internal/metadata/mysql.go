package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"fuel/api"
	"fuel/internal/config"
)

// mysqlEngine 是模式 C: MySQL 元数据引擎 (PLAN §7.1)。
// 内嵌 directEngine 作为 miss 回源路径 (INV-1: 对象存储是真相来源)。
// 持久化：进程重启后元数据不丢失 (PLAN §7.1 验证标准)。
//
// 表结构 (schema.sql):
//
//	fuel_meta     (bucket, path PK, size, etag, mtime, is_dir, content_type, mode, uid, gid)
//	fuel_dentries (bucket, dir_path, child_name PK, is_dir, size, etag, mtime, content_type)
//
// 无过期：写路径主动失效 (INV-1)。负缓存由 L1 处理，L2 不做负缓存。
type mysqlEngine struct {
	*directEngine
	db     *sql.DB
	bucket string
}

func init() {
	RegisterMetadataEngine("mysql", newMysqlEngine)
}

// newMysqlEngine 构造 MySQL 引擎。DSN 为空返回配置错误。
// 不在构造期 PING（sql.Open 懒验证）；可用性由 HealthCheck 暴露给上层。
func newMysqlEngine(cfg *config.Config, store api.ObjectStore) (api.MetadataEngine, error) {
	if store == nil {
		return nil, fmt.Errorf("mysql engine requires a non-nil ObjectStore")
	}
	dsn := cfg.Metadata.MySQL.DSN
	if dsn == "" {
		return nil, fmt.Errorf("metadata.mysql.dsn is required for mysql engine")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return newMysqlEngineWithDB(cfg.Storage.Bucket, store, db), nil
}

// newMysqlEngineWithDB 用给定 *sql.DB 构造引擎（测试注入 sqlmock）。
func newMysqlEngineWithDB(bucket string, store api.ObjectStore, db *sql.DB) *mysqlEngine {
	return &mysqlEngine{
		directEngine: &directEngine{store: store, uid: uint32(os.Getuid()), gid: uint32(os.Getgid())},
		db:           db,
		bucket:       bucket,
	}
}

// GetAttr 获取 path 元数据: SELECT → hit 返回; miss → 回源 (写回 MySQL)。
// MySQL 不可用时降级为直查对象存储 (INV-4)。
func (e *mysqlEngine) GetAttr(ctx context.Context, path string) (*api.MetaEntry, error) {
	key := normalizeKey(path)
	if key == "" {
		return api.DirMetaEntry("/", e.uid, e.gid), nil
	}

	entry, err := e.selectMeta(ctx, key)
	if err == nil {
		return entry, nil
	}
	if err != sql.ErrNoRows {
		return e.directEngine.GetAttr(ctx, path)
	}

	entry, err = e.directEngine.GetAttr(ctx, path)
	if err != nil {
		return nil, err
	}
	e.upsertMeta(ctx, key, entry)
	return entry, nil
}

// selectMeta 查询 fuel_meta 单行，构建 MetaEntry。
func (e *mysqlEngine) selectMeta(ctx context.Context, key string) (*api.MetaEntry, error) {
	var size int64
	var etag, contentType string
	var isDir bool
	var mtime time.Time
	var mode, uid uint32
	var gid uint32
	err := e.db.QueryRowContext(ctx,
		`SELECT size, etag, mtime, is_dir, content_type, mode, uid, gid
		 FROM fuel_meta WHERE bucket = ? AND path = ?`,
		e.bucket, key,
	).Scan(&size, &etag, &mtime, &isDir, &contentType, &mode, &uid, &gid)
	if err != nil {
		return nil, err
	}
	return &api.MetaEntry{
		Path:        key,
		Inode:       api.InodeFromPath(key),
		Size:        size,
		ETag:        etag,
		Mode:        enforceMode(mode, isDir),
		Uid:         uid,
		Gid:         gid,
		MTime:       mtime,
		ATime:       time.Now(),
		Nlink:       nlink(isDir),
		IsDir:       isDir,
		ContentType: contentType,
	}, nil
}

// upsertMeta INSERT ... ON DUPLICATE KEY UPDATE。静默失败（加速层，下次 miss 回源）。
func (e *mysqlEngine) upsertMeta(ctx context.Context, key string, entry *api.MetaEntry) {
	_, _ = e.db.ExecContext(ctx,
		`INSERT INTO fuel_meta
		 (bucket, path, size, etag, mtime, is_dir, content_type, mode, uid, gid)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		 size = VALUES(size), etag = VALUES(etag), mtime = VALUES(mtime),
		 is_dir = VALUES(is_dir), content_type = VALUES(content_type),
		 mode = VALUES(mode), uid = VALUES(uid), gid = VALUES(gid)`,
		e.bucket, key, entry.Size, entry.ETag, entry.MTime, entry.IsDir,
		entry.ContentType, entry.Mode, entry.Uid, entry.Gid,
	)
}

// SetAttr 写入 meta（UPSERT 语义）。
func (e *mysqlEngine) SetAttr(ctx context.Context, path string, entry *api.MetaEntry) error {
	key := normalizeKey(path)
	if key == "" {
		return nil
	}
	e.upsertMeta(ctx, key, entry)
	return nil
}

// DeleteAttr 删除 meta 行。
func (e *mysqlEngine) DeleteAttr(ctx context.Context, path string) error {
	key := normalizeKey(path)
	if key == "" {
		return nil
	}
	_, err := e.db.ExecContext(ctx,
		`DELETE FROM fuel_meta WHERE bucket = ? AND path = ?`,
		e.bucket, key,
	)
	if err != nil {
		return fmt.Errorf("mysql delete meta %s: %w", key, err)
	}
	return nil
}

// ListDir 列出目录: SELECT dentries → hit; miss → 回源 List → 写回。
// MySQL 不可用时降级直查 (INV-4)。
func (e *mysqlEngine) ListDir(ctx context.Context, dirPath string) ([]api.DirEntry, error) {
	key := normalizeKey(dirPath)

	entries, err := e.selectDentries(ctx, key)
	if err == nil {
		return entries, nil
	}
	if err != sql.ErrNoRows {
		return e.directEngine.ListDir(ctx, dirPath)
	}

	entries, err = e.directEngine.ListDir(ctx, dirPath)
	if err != nil {
		return nil, err
	}
	e.replaceDentries(ctx, key, entries)
	return entries, nil
}

// selectDentries 查询 fuel_dentries 构建 []DirEntry（含内联 MetaEntry）。
func (e *mysqlEngine) selectDentries(ctx context.Context, dirKey string) ([]api.DirEntry, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT child_name, is_dir, size, etag, mtime, content_type
		 FROM fuel_dentries
		 WHERE bucket = ? AND dir_path = ?
		 ORDER BY child_name`,
		e.bucket, dirKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []api.DirEntry
	for rows.Next() {
		var name string
		var isDir bool
		var size int64
		var etag, contentType string
		var mtime time.Time
		if err := rows.Scan(&name, &isDir, &size, &etag, &mtime, &contentType); err != nil {
			return nil, err
		}
		childPath := dirKey
		if childPath != "" {
			childPath += "/"
		}
		childPath += name
		entries = append(entries, api.DirEntry{
			Name:  name,
			IsDir: isDir,
			Meta: &api.MetaEntry{
				Path:        childPath,
				Inode:       api.InodeFromPath(childPath),
				Size:        size,
				ETag:        etag,
				Mode:        enforceMode(0, isDir),
				Uid:         e.uid,
				Gid:         e.gid,
				MTime:       mtime,
				ATime:       time.Now(),
				Nlink:       nlink(isDir),
				IsDir:       isDir,
				ContentType: contentType,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, sql.ErrNoRows
	}
	return entries, nil
}

// replaceDentries 事务：DELETE 旧行 + INSERT 新行。
func (e *mysqlEngine) replaceDentries(ctx context.Context, dirKey string, entries []api.DirEntry) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql begin tx for dir %s: %w", dirKey, err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM fuel_dentries WHERE bucket = ? AND dir_path = ?`,
		e.bucket, dirKey,
	); err != nil {
		return fmt.Errorf("mysql delete dentries %s: %w", dirKey, err)
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO fuel_dentries
		 (bucket, dir_path, child_name, is_dir, size, etag, mtime, content_type)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("mysql prepare dentry insert %s: %w", dirKey, err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		me := entry.Meta
		if me == nil {
			me = &api.MetaEntry{}
		}
		if _, err := stmt.ExecContext(ctx, e.bucket, dirKey, entry.Name, entry.IsDir,
			me.Size, me.ETag, me.MTime, me.ContentType,
		); err != nil {
			return fmt.Errorf("mysql insert dentry %s/%s: %w", dirKey, entry.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql commit dir %s: %w", dirKey, err)
	}
	return nil
}

// SetDir 整体替换目录列表缓存（事务）。
func (e *mysqlEngine) SetDir(ctx context.Context, dirPath string, entries []api.DirEntry) error {
	key := normalizeKey(dirPath)
	return e.replaceDentries(ctx, key, entries)
}

// DeleteDir 删除目录列表缓存。
func (e *mysqlEngine) DeleteDir(ctx context.Context, dirPath string) error {
	key := normalizeKey(dirPath)
	_, err := e.db.ExecContext(ctx,
		`DELETE FROM fuel_dentries WHERE bucket = ? AND dir_path = ?`,
		e.bucket, key,
	)
	if err != nil {
		return fmt.Errorf("mysql delete dir %s: %w", key, err)
	}
	return nil
}

// BatchGetAttr 批量查询: SELECT ... WHERE path IN (...)。
// miss 的逐个回源 (GetAttr 自带写回)。MySQL 不可用时降级直查。
func (e *mysqlEngine) BatchGetAttr(ctx context.Context, paths []string) (map[string]*api.MetaEntry, error) {
	result := make(map[string]*api.MetaEntry, len(paths))
	if len(paths) == 0 {
		return result, nil
	}

	keys := make([]string, 0, len(paths))
	for _, p := range paths {
		k := normalizeKey(p)
		if k == "" {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return result, nil
	}

	query := `SELECT path, size, etag, mtime, is_dir, content_type, mode, uid, gid
		 FROM fuel_meta WHERE bucket = ? AND path IN (` + placeholders(len(keys)) + `)`
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, e.bucket)
	for _, k := range keys {
		args = append(args, k)
	}

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		for _, p := range paths {
			if entry, gerr := e.GetAttr(ctx, p); gerr == nil {
				result[p] = entry
			}
		}
		return result, nil
	}
	defer rows.Close()

	hit := map[string]bool{}
	for rows.Next() {
		var path string
		var size int64
		var etag, contentType string
		var isDir bool
		var mtime time.Time
		var mode, uid, gid uint32
		if err := rows.Scan(&path, &size, &etag, &mtime, &isDir, &contentType, &mode, &uid, &gid); err != nil {
			break
		}
		result[path] = &api.MetaEntry{
			Path:        path,
			Inode:       api.InodeFromPath(path),
			Size:        size,
			ETag:        etag,
			Mode:        enforceMode(mode, isDir),
			Uid:         uid,
			Gid:         gid,
			MTime:       mtime,
			ATime:       time.Now(),
			Nlink:       nlink(isDir),
			IsDir:       isDir,
			ContentType: contentType,
		}
		hit[path] = true
	}
	_ = rows.Close()

	for _, p := range paths {
		k := normalizeKey(p)
		if k == "" {
			continue
		}
		if hit[k] {
			continue
		}
		if entry, gerr := e.GetAttr(ctx, p); gerr == nil {
			result[p] = entry
		}
	}
	return result, nil
}

// placeholders 返回 n 个 "?" 用逗号分隔的字符串。
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// Invalidate 级联失效 path 及其所有子路径 (IMPL_DESIGN §4.2)。
// 删除: fuel_meta 自身 + 所有前缀匹配的子项
//
//	+ fuel_dentries 自身 dir_path + 所有前缀匹配的子目录 dentries
//	+ 父目录 dentries (父目录列表包含 path 条目，PLAN §6.1 "失效 dir 缓存")
//
// key 为根 ("/") 时清空该 bucket 的全部 meta + dentries。
func (e *mysqlEngine) Invalidate(ctx context.Context, path string) error {
	key := normalizeKey(path)

	if key == "" {
		if _, err := e.db.ExecContext(ctx,
			`DELETE FROM fuel_meta WHERE bucket = ?`, e.bucket,
		); err != nil {
			return fmt.Errorf("mysql invalidate root meta: %w", err)
		}
		if _, err := e.db.ExecContext(ctx,
			`DELETE FROM fuel_dentries WHERE bucket = ?`, e.bucket,
		); err != nil {
			return fmt.Errorf("mysql invalidate root dentries: %w", err)
		}
		return nil
	}

	escaped := escapeLike(key)
	likePattern := escaped + "/%"

	_, err := e.db.ExecContext(ctx,
		`DELETE FROM fuel_meta
		 WHERE bucket = ? AND (path = ? OR path LIKE ? ESCAPE '\')`,
		e.bucket, key, likePattern,
	)
	if err != nil {
		return fmt.Errorf("mysql invalidate meta %s: %w", key, err)
	}

	_, err = e.db.ExecContext(ctx,
		`DELETE FROM fuel_dentries
		 WHERE bucket = ? AND (dir_path = ? OR dir_path LIKE ? ESCAPE '\')`,
		e.bucket, key, likePattern,
	)
	if err != nil {
		return fmt.Errorf("mysql invalidate dentries %s: %w", key, err)
	}

	if parent := parentOf(key); parent != key {
		if _, err := e.db.ExecContext(ctx,
			`DELETE FROM fuel_dentries
			 WHERE bucket = ? AND dir_path = ?`,
			e.bucket, parent,
		); err != nil {
			return fmt.Errorf("mysql invalidate parent dentries %s: %w", parent, err)
		}
	}
	return nil
}

// escapeLike 转义 MySQL LIKE 通配符 (% _ \)。
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

// HealthCheck 检查 MySQL 可达性（Ping）。供上层做降级决策 (IMPL_DESIGN §9.2)。
func (e *mysqlEngine) HealthCheck(ctx context.Context) error {
	if err := e.db.PingContext(ctx); err != nil {
		return fmt.Errorf("mysql ping: %w", err)
	}
	return nil
}

// Close 释放数据库连接池。
func (e *mysqlEngine) Close() error {
	return e.db.Close()
}

// enforceMode 确保 mode 非零且与 is_dir 一致。
func enforceMode(mode uint32, isDir bool) uint32 {
	if mode == 0 {
		if isDir {
			return 0o755
		}
		return 0o644
	}
	return mode
}

// nlink 返回文件/目录的 nlink 默认值。
func nlink(isDir bool) uint32 {
	if isDir {
		return 2
	}
	return 1
}

// 编译期接口检查
var (
	_ api.MetadataEngine = (*redisEngine)(nil)
	_ api.MetadataEngine = (*mysqlEngine)(nil)
)