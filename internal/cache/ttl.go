package cache

import (
	"strings"
	"sync"
	"time"
)

// ttlEntry 是 ttlCache 中的一条记录，包含值和过期时间。
type ttlEntry[V any] struct {
	value  V
	expire time.Time
}

// ttlCache 是带 TTL 的泛型内存缓存 (IMPL_DESIGN §4.4)。
// 读多写少，使用 sync.RWMutex 保护；过期采用惰性淘汰（Get 时检查），
// 配合后台 janitor 周期扫描，避免无界增长。
type ttlCache[V any] struct {
	mu      sync.RWMutex
	items   map[string]ttlEntry[V]
	ttl     time.Duration
	janStop chan struct{}
}

// newTTLCache 构造一个 TTL 缓存。ttl <= 0 表示永不过期（仅用于测试/特殊场景）。
// 启动后台 janitor 周期清理过期项，janitorInterval = max(ttl/2, 50ms)。
func newTTLCache[V any](ttl time.Duration) *ttlCache[V] {
	c := &ttlCache[V]{
		items:   make(map[string]ttlEntry[V]),
		ttl:     ttl,
		janStop: make(chan struct{}),
	}
	if ttl > 0 {
		interval := ttl / 2
		if interval < 50*time.Millisecond {
			interval = 50 * time.Millisecond
		}
		go c.janitor(interval)
	}
	return c
}

// Get 查找 key，命中且未过期返回值。过期则惰性删除并返回 miss。
func (c *ttlCache[V]) Get(key string) (V, bool) {
	var zero V
	c.mu.RLock()
	e, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return zero, false
	}
	if c.ttl > 0 && !e.expire.After(time.Now()) {
		c.mu.Lock()
		if cur, ok := c.items[key]; ok && !cur.expire.After(time.Now()) {
			delete(c.items, key)
		}
		c.mu.Unlock()
		return zero, false
	}
	return e.value, true
}

// Set 写入 key→value，过期时间为 now+ttl。若 key 已存在则覆盖。
func (c *ttlCache[V]) Set(key string, value V) {
	if c.ttl <= 0 {
		c.mu.Lock()
		c.items[key] = ttlEntry[V]{value: value}
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	c.items[key] = ttlEntry[V]{
		value:  value,
		expire: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// Delete 删除 key。不存在时为 no-op。
func (c *ttlCache[V]) Delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Len 返回当前条目数（含可能未清理的过期项，仅供统计/测试）。
func (c *ttlCache[V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// InvalidatePrefix 删除所有以 prefix 开头的 key（路径前缀级联失效）。
// 要求 prefix 非空且以 "/" 结尾，避免误匹配（如 "a" 前缀会误删 "ab/c"）。
// 若 prefix 为空，则清空全部（用于 Invalidate 全局）。
func (c *ttlCache[V]) InvalidatePrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prefix == "" {
		n := len(c.items)
		c.items = make(map[string]ttlEntry[V])
		return n
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	removed := 0
	for k := range c.items {
		if strings.HasPrefix(k, prefix) {
			delete(c.items, k)
			removed++
		}
	}
	return removed
}

// Stop 停止后台 janitor。主要用于测试和优雅关闭。
// Stop 后不可再次 Start；调用方应放弃此实例。
func (c *ttlCache[V]) Stop() {
	select {
	case <-c.janStop:
	default:
		close(c.janStop)
	}
}

// janitor 后台周期清理过期项，避免内存无界增长。
func (c *ttlCache[V]) janitor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.janStop:
			return
		case <-t.C:
			c.sweep()
		}
	}
}

// sweep 扫描并删除过期项。持有写锁。
func (c *ttlCache[V]) sweep() int {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for k, e := range c.items {
		if !e.expire.After(now) {
			delete(c.items, k)
			removed++
		}
	}
	return removed
}
