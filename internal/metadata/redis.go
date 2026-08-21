package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"fuel/api"
	"fuel/internal/config"
)

// redisNegTTL 负缓存条目 TTL (PLAN §6.1): neg:{bucket}/{path} → "1" (TTL 60s)。
const redisNegTTL = 60 * time.Second

// redisScanCount 是 Invalidate 级联删除时 SCAN 的 COUNT hint。
const redisScanCount = 200

// redisEngine 是模式 B: Redis 元数据引擎 (IMPL_DESIGN §4.2)。
// 内嵌 directEngine 作为 miss 回源路径 (INV-1: 对象存储是真相来源)。
// Redis 不可用时读路径降级为直查对象存储 (INV-4, IMPL_DESIGN §9.2)。
//
// Key 设计 (IMPL_DESIGN §4.2):
//
//	meta:{bucket}/{path} → JSON(MetaEntry)   无过期, 写路径主动删
//	dir:{bucket}/{path}  → JSON([]DirEntry)  无过期, 写路径主动删
//	neg:{bucket}/{path}  → "1"               TTL 60s
//
// 跨节点共享 (PLAN §6.1): 所有节点读同一 Redis, 节点 A 写入后节点 B 可读到。
type redisEngine struct {
	*directEngine
	client redis.UniversalClient
	bucket string
}

// init 注册 redis 引擎到工厂。
func init() {
	RegisterMetadataEngine("redis", newRedisEngine)
}

// newRedisEngine 构造 Redis 引擎。address 为空返回配置错误。
// 不在构造期 PING（go-redis 懒连接）；可用性由 HealthCheck 暴露给上层。
func newRedisEngine(cfg *config.Config, store api.ObjectStore) (api.MetadataEngine, error) {
	if store == nil {
		return nil, fmt.Errorf("redis engine requires a non-nil ObjectStore")
	}
	addr := cfg.Metadata.Redis.Address
	if addr == "" {
		return nil, fmt.Errorf("metadata.redis.address is required for redis engine")
	}
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		MaxRetries: 3, // §3.2: 可重试网络错误指数退避（go-redis 内置）
	})
	return newRedisEngineWithClient(cfg.Storage.Bucket, store, client), nil
}

// newRedisEngineWithClient 用给定 client 构造引擎（测试注入 miniredis client）。
func newRedisEngineWithClient(bucket string, store api.ObjectStore, client redis.UniversalClient) *redisEngine {
	return &redisEngine{
		directEngine: &directEngine{store: store, uid: uint32(os.Getuid()), gid: uint32(os.Getgid())},
		client:       client,
		bucket:       bucket,
	}
}

func (e *redisEngine) metaKey(path string) string {
	return "meta:" + e.bucket + "/" + normalizeKey(path)
}

func (e *redisEngine) dirKey(dirPath string) string {
	return "dir:" + e.bucket + "/" + normalizeKey(dirPath)
}

func (e *redisEngine) negKey(path string) string {
	return "neg:" + e.bucket + "/" + normalizeKey(path)
}

// GetAttr 获取 path 的元数据: neg → meta → 回源 (写回 Redis)。
// Redis 操作失败时降级为直查对象存储 (INV-4)，不返回错误。
func (e *redisEngine) GetAttr(ctx context.Context, path string) (*api.MetaEntry, error) {
	key := normalizeKey(path)
	if key == "" {
		return api.DirMetaEntry("/", e.uid, e.gid), nil
	}

	pipe := e.client.Pipeline()
	negCmd := pipe.Exists(ctx, e.negKey(key))
	metaCmd := pipe.Get(ctx, e.metaKey(key))
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		zap.L().Warn("redis getattr failed, degrade to direct", zap.String("path", path), zap.Error(err))
		return e.directEngine.GetAttr(ctx, path)
	}
	if negCmd.Val() > 0 {
		return nil, fmt.Errorf("path %s: %w", path, syscall.ENOENT)
	}
	if data, err := metaCmd.Bytes(); err == nil {
		var entry api.MetaEntry
		if json.Unmarshal(data, &entry) == nil {
			return &entry, nil
		}
		// JSON 损坏 → 视为 miss，回源覆盖 (INV-9: 不返回不可信数据)
	}

	entry, err := e.directEngine.GetAttr(ctx, path)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			e.setNeg(ctx, key)
		}
		return nil, err
	}
	e.setMeta(ctx, key, entry)
	return entry, nil
}

// setMeta 写回 meta key。失败静默（写回只是加速层，下次 miss 还会回源）。
func (e *redisEngine) setMeta(ctx context.Context, key string, entry *api.MetaEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	_ = e.client.Set(ctx, e.metaKey(key), data, 0).Err()
}

// setNeg 写入负缓存（TTL 60s）。失败静默。
func (e *redisEngine) setNeg(ctx context.Context, key string) {
	_ = e.client.Set(ctx, e.negKey(key), "1", redisNegTTL).Err()
}

// SetAttr 写入 meta 并清除对应负缓存（PLAN §6.1: SET meta + DEL neg）。
func (e *redisEngine) SetAttr(ctx context.Context, path string, entry *api.MetaEntry) error {
	key := normalizeKey(path)
	if key == "" {
		return nil
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal meta %s: %w", key, err)
	}
	pipe := e.client.Pipeline()
	pipe.Set(ctx, e.metaKey(key), data, 0)
	pipe.Del(ctx, e.negKey(key))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis set meta %s: %w", key, err)
	}
	return nil
}

// DeleteAttr 删除 meta 并写入负缓存（TTL 60s），跨节点立即可见 ENOENT。
func (e *redisEngine) DeleteAttr(ctx context.Context, path string) error {
	key := normalizeKey(path)
	if key == "" {
		return nil
	}
	pipe := e.client.Pipeline()
	pipe.Del(ctx, e.metaKey(key))
	pipe.Set(ctx, e.negKey(key), "1", redisNegTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis delete meta %s: %w", key, err)
	}
	return nil
}

// ListDir 列出目录: dir key 命中 → 返回; miss → 回源 List → 写回 Redis。
// Redis 故障时降级为直查对象存储 (INV-4)。
func (e *redisEngine) ListDir(ctx context.Context, dirPath string) ([]api.DirEntry, error) {
	key := normalizeKey(dirPath)

	data, err := e.client.Get(ctx, e.dirKey(key)).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		zap.L().Warn("redis listdir failed, degrade to direct", zap.String("dir", dirPath), zap.Error(err))
		return e.directEngine.ListDir(ctx, dirPath)
	}
	if err == nil {
		var entries []api.DirEntry
		if json.Unmarshal(data, &entries) == nil {
			return entries, nil
		}
		// JSON 损坏 → miss 回源 (INV-9)
	}

	entries, err := e.directEngine.ListDir(ctx, dirPath)
	if err != nil {
		return nil, err
	}
	if data, err := json.Marshal(entries); err == nil {
		_ = e.client.Set(ctx, e.dirKey(key), data, 0).Err()
	}
	return entries, nil
}

// SetDir 写入目录列表缓存（整体 JSON，与 SetDir 接口"整体替换"语义一致）。
func (e *redisEngine) SetDir(ctx context.Context, dirPath string, entries []api.DirEntry) error {
	key := normalizeKey(dirPath)
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal dir %s: %w", key, err)
	}
	if err := e.client.Set(ctx, e.dirKey(key), data, 0).Err(); err != nil {
		return fmt.Errorf("redis set dir %s: %w", key, err)
	}
	return nil
}

// DeleteDir 删除目录列表缓存。
func (e *redisEngine) DeleteDir(ctx context.Context, dirPath string) error {
	if err := e.client.Del(ctx, e.dirKey(dirPath)).Err(); err != nil {
		return fmt.Errorf("redis delete dir %s: %w", dirPath, err)
	}
	return nil
}

// BatchGetAttr 批量获取元数据: MGET meta keys → miss 的逐个回源 (自带写回 + neg)。
// Redis 故障时全部降级为直查 (INV-4)。ENOENT 的 path 不出现在结果 map 中。
func (e *redisEngine) BatchGetAttr(ctx context.Context, paths []string) (map[string]*api.MetaEntry, error) {
	result := make(map[string]*api.MetaEntry, len(paths))
	if len(paths) == 0 {
		return result, nil
	}

	keys := make([]string, 0, len(paths))
	keyToPath := make(map[string]string, len(paths))
	for _, p := range paths {
		k := normalizeKey(p)
		if k == "" {
			continue
		}
		mk := e.metaKey(k)
		keys = append(keys, mk)
		keyToPath[mk] = p
	}

	vals, err := e.client.MGet(ctx, keys...).Result()
	if err != nil {
		zap.L().Warn("redis mget failed, degrade to direct", zap.Int("paths", len(paths)), zap.Error(err))
		for _, p := range paths {
			if entry, gerr := e.GetAttr(ctx, p); gerr == nil {
				result[p] = entry
			}
		}
		return result, nil
	}

	var missPaths []string
	for i, v := range vals {
		p := keyToPath[keys[i]]
		data, ok := v.(string)
		if !ok {
			missPaths = append(missPaths, p)
			continue
		}
		var entry api.MetaEntry
		if json.Unmarshal([]byte(data), &entry) != nil {
			missPaths = append(missPaths, p)
			continue
		}
		result[p] = &entry
	}
	for _, p := range missPaths {
		if entry, gerr := e.GetAttr(ctx, p); gerr == nil {
			result[p] = entry
		}
	}
	return result, nil
}

// Invalidate 级联失效 path 及其所有子路径 (IMPL_DESIGN §4.2)。
// 删除: 自身的 meta/dir/neg 三 key + 所有 "{path}/" 前缀的子项 key + 父目录列表 key
// （父目录列表包含 path 的条目，写/删后必须失效，PLAN §6.1 "失效 dir 缓存"）。
// key 为根 ("/") 时清空该 bucket 的全部三种前缀缓存。
func (e *redisEngine) Invalidate(ctx context.Context, path string) error {
	key := normalizeKey(path)

	if key == "" {
		for _, prefix := range []string{"meta:", "dir:", "neg:"} {
			if err := e.scanDeletePrefix(ctx, prefix+e.bucket+"/"); err != nil {
				return err
			}
		}
		return nil
	}

	pipe := e.client.Pipeline()
	pipe.Del(ctx, e.metaKey(key), e.dirKey(key), e.negKey(key))
	if parent := parentOf(key); parent != key {
		pipe.Del(ctx, e.dirKey(parent))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis invalidate %s: %w", key, err)
	}

	for _, prefix := range []string{e.metaKey(key) + "/", e.dirKey(key) + "/", e.negKey(key) + "/"} {
		if err := e.scanDeletePrefix(ctx, prefix); err != nil {
			return err
		}
	}
	return nil
}

// scanDeletePrefix SCAN 删除所有匹配 prefix 的 key。
// SCAN MATCH 使用 glob，path 中可能含通配符，故匹配结果用 HasPrefix 二次过滤。
func (e *redisEngine) scanDeletePrefix(ctx context.Context, prefix string) error {
	var cursor uint64
	for {
		keys, next, err := e.client.Scan(ctx, cursor, prefix+"*", redisScanCount).Result()
		if err != nil {
			return fmt.Errorf("redis scan %s*: %w", prefix, err)
		}
		var match []string
		for _, k := range keys {
			if strings.HasPrefix(k, prefix) {
				match = append(match, k)
			}
		}
		if len(match) > 0 {
			if err := e.client.Del(ctx, match...).Err(); err != nil {
				return fmt.Errorf("redis delete %d keys under %s: %w", len(match), prefix, err)
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// HealthCheck 检查 Redis 可达性（PING）。供上层做降级决策 (IMPL_DESIGN §9.2)。
func (e *redisEngine) HealthCheck(ctx context.Context) error {
	if err := e.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

// Close 释放 Redis 连接。
func (e *redisEngine) Close() error {
	return e.client.Close()
}

// parentOf 返回对象 key 的父目录 key（"a/b/c" → "a/b"，"c" → ""）。
func parentOf(key string) string {
	i := strings.LastIndex(key, "/")
	if i < 0 {
		return ""
	}
	return key[:i]
}