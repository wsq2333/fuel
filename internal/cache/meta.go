package cache

import (
	"strings"
	"sync/atomic"

	"fuel/api"
	"fuel/internal/config"
)

// MetaCache 是 FUSE 层使用的 L1 元数据内存缓存接口 (IMPL_DESIGN §4.4)。
// 不属于 MetadataEngine (L2) 接口，是 cache 包内部加速层，被 FUSE 层直接持有。
type MetaCache interface {
	GetStat(path string) (*api.MetaEntry, bool)
	SetStat(path string, entry *api.MetaEntry)
	DeleteStat(path string)
	GetDir(dirPath string) ([]api.DirEntry, bool)
	SetDir(dirPath string, entries []api.DirEntry)
	DeleteDir(dirPath string)
	GetNeg(path string) bool
	SetNeg(path string)
	DeleteNeg(path string)
	// InvalidatePrefix 级联失效 prefix 及其所有子路径。
	// prefix 应为规范化后的目录路径（不以 "/" 结尾），如 "/a/b"。
	// 同时清理 stat/dir/neg 三层缓存中匹配前缀的条目。
	InvalidatePrefix(prefix string)
	Stats() MetaCacheStats
}

// MetaCacheStats L1 元数据缓存统计（用于监控指标 fuel_meta_*）。
type MetaCacheStats struct {
	StatHits   int64
	StatMisses int64
	DirHits    int64
	DirMisses  int64
	NegHits    int64
	NegMisses  int64
}

// metaCache 是 MetaCache 的默认实现：三层 TTL 缓存 (stat/dir/neg)。
// 三层独立 TTL，对应配置 metadata.cache.{statTTL, dirTTL, negTTL}。
// ttl <= 0 表示对应层关闭：该层不分配缓存，所有 Get 永远 miss。
type metaCache struct {
	stat *ttlCache[*api.MetaEntry]
	dir  *ttlCache[[]api.DirEntry]
	neg  *ttlCache[struct{}]

	statHits   atomic.Int64
	statMisses atomic.Int64
	dirHits    atomic.Int64
	dirMisses  atomic.Int64
	negHits    atomic.Int64
	negMisses  atomic.Int64
}

// NewMetaCache 根据 config.Metadata.Cache 配置构造 L1 元数据缓存。
// statTTL/dirTTL/negTTL <= 0 表示对应层关闭（永不上缓存，所有调用 miss）。
func NewMetaCache(cfg config.MetaCacheConfig) MetaCache {
	mc := &metaCache{}
	if cfg.StatTTL > 0 {
		mc.stat = newTTLCache[*api.MetaEntry](cfg.StatTTL)
	}
	if cfg.DirTTL > 0 {
		mc.dir = newTTLCache[[]api.DirEntry](cfg.DirTTL)
	}
	if cfg.NegTTL > 0 {
		mc.neg = newTTLCache[struct{}](cfg.NegTTL)
	}
	return mc
}

// GetStat 查询 path 的 stat 缓存。命中返回 MetaEntry 副本指针（调用方可修改）。
func (c *metaCache) GetStat(path string) (*api.MetaEntry, bool) {
	if c.stat == nil {
		c.statMisses.Add(1)
		return nil, false
	}
	v, ok := c.stat.Get(path)
	if !ok {
		c.statMisses.Add(1)
		return nil, false
	}
	c.statHits.Add(1)
	cp := *v
	return &cp, true
}

// SetStat 写入 path 的 stat 缓存。同时清除 path 的负缓存（存在的对象不应再是负缓存）。
func (c *metaCache) SetStat(path string, entry *api.MetaEntry) {
	if c.stat != nil {
		c.stat.Set(path, entry)
	}
	c.DeleteNeg(path)
}

// DeleteStat 删除 path 的 stat 缓存。
func (c *metaCache) DeleteStat(path string) {
	if c.stat != nil {
		c.stat.Delete(path)
	}
}

// GetDir 查询 dirPath 的目录列表缓存。命中返回 entries 切片副本（调用方可修改）。
func (c *metaCache) GetDir(dirPath string) ([]api.DirEntry, bool) {
	if c.dir == nil {
		c.dirMisses.Add(1)
		return nil, false
	}
	v, ok := c.dir.Get(dirPath)
	if !ok {
		c.dirMisses.Add(1)
		return nil, false
	}
	c.dirHits.Add(1)
	cp := make([]api.DirEntry, len(v))
	copy(cp, v)
	return cp, true
}

// SetDir 写入 dirPath 的目录列表缓存。
func (c *metaCache) SetDir(dirPath string, entries []api.DirEntry) {
	if c.dir != nil {
		c.dir.Set(dirPath, entries)
	}
}

// DeleteDir 删除 dirPath 的目录列表缓存。
func (c *metaCache) DeleteDir(dirPath string) {
	if c.dir != nil {
		c.dir.Delete(dirPath)
	}
}

// GetNeg 查询 path 的负缓存（path 不存在）。
func (c *metaCache) GetNeg(path string) bool {
	if c.neg == nil {
		c.negMisses.Add(1)
		return false
	}
	_, ok := c.neg.Get(path)
	if !ok {
		c.negMisses.Add(1)
		return false
	}
	c.negHits.Add(1)
	return true
}

// SetNeg 写入 path 的负缓存。同时清除 path 的 stat 缓存（不存在的对象不应有 stat）。
func (c *metaCache) SetNeg(path string) {
	if c.neg != nil {
		c.neg.Set(path, struct{}{})
	}
	c.DeleteStat(path)
}

// DeleteNeg 删除 path 的负缓存。
func (c *metaCache) DeleteNeg(path string) {
	if c.neg != nil {
		c.neg.Delete(path)
	}
}

// InvalidatePrefix 级联失效 prefix 及其所有子路径的 stat/dir/neg 缓存。
// 同时失效 prefix 自身（视为目录时其下子项整体失效）。
// prefix 为空字符串时清空所有缓存。
func (c *metaCache) InvalidatePrefix(prefix string) {
	statPrefix, dirExact, dirSub, negPrefix := normalizePrefixes(prefix)
	if c.stat != nil {
		c.stat.InvalidatePrefix(statPrefix)
	}
	if c.neg != nil {
		c.neg.InvalidatePrefix(negPrefix)
	}
	if c.dir != nil {
		c.dir.Delete(dirExact)
		c.dir.InvalidatePrefix(dirSub)
	}
}

// Stats 返回 L1 缓存的累计命中/未命中计数。
func (c *metaCache) Stats() MetaCacheStats {
	return MetaCacheStats{
		StatHits:   c.statHits.Load(),
		StatMisses: c.statMisses.Load(),
		DirHits:    c.dirHits.Load(),
		DirMisses:  c.dirMisses.Load(),
		NegHits:    c.negHits.Load(),
		NegMisses:  c.negMisses.Load(),
	}
}

// Len 返回 stat/dir/neg 三层缓存的总条目数（含可能未清理的过期项，仅供测试/监控）。
func (c *metaCache) Len() int {
	n := 0
	if c.stat != nil {
		n += c.stat.Len()
	}
	if c.dir != nil {
		n += c.dir.Len()
	}
	if c.neg != nil {
		n += c.neg.Len()
	}
	return n
}

// normalizePrefixes 将用户传入的 prefix 规范化为 stat/dir/neg 三层缓存各自使用的前缀。
// stat/neg 缓存以对象路径为 key（如 "/a/b/c.txt"），失效需匹配 "/a/b/" 前缀。
// dir 缓存以目录路径为 key（如 "/a/b"），需分别删除 prefix 自身（dirExact）
// 及其下子目录（dirSub = "a/b/"，匹配 "a/b/c" 等）。
func normalizePrefixes(prefix string) (statPrefix, dirExact, dirSub, negPrefix string) {
	if prefix == "" {
		return "", "", "", ""
	}
	p := strings.Trim(prefix, "/")
	if p == "" {
		return "", "", "", ""
	}
	statPrefix = p + "/"
	dirExact = p
	dirSub = p + "/"
	negPrefix = p + "/"
	return
}
