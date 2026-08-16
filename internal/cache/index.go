package cache

import (
	"container/list"
	"sync"
	"time"
)

// cacheEntry 缓存索引条目 (IMPL_DESIGN §3.4)，内部类型不暴露。
type cacheEntry struct {
	Key        string
	ETag       string
	Size       int64
	LocalPath  string
	LastAccess time.Time
}

// cacheIndex 内存缓存索引：map 提供 O(1) 查找，list 维护 LRU 顺序（Front=最近访问）。
// MVP 不持久化，进程重启后通过扫描缓存目录重建 (PLAN §10.4 可选 BoltDB 持久化)。
type cacheIndex struct {
	mu      sync.Mutex
	items   map[string]*list.Element // key → list element (value 为 *cacheEntry)
	lru     *list.List               // LRU 顺序，Front 为最近访问
	used    int64                    // 已用字节数
	evicted int64                    // 累计淘汰条目数
}

func newCacheIndex() *cacheIndex {
	return &cacheIndex{
		items: make(map[string]*list.Element),
		lru:   list.New(),
	}
}

// get 查找 key，命中则将其移到 LRU Front 并更新 LastAccess。
func (ix *cacheIndex) get(key string) (*cacheEntry, bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	el, ok := ix.items[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*cacheEntry)
	entry.LastAccess = time.Now()
	ix.lru.MoveToFront(el)
	return entry, true
}

// peek 查找 key 但不改变 LRU 顺序（用于 Contains）。
func (ix *cacheIndex) peek(key string) (*cacheEntry, bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	el, ok := ix.items[key]
	if !ok {
		return nil, false
	}
	return el.Value.(*cacheEntry), true
}

// put 插入或更新条目，移到 LRU Front，累计 used。若 key 已存在则先减旧 size。
func (ix *cacheIndex) put(entry *cacheEntry) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if el, ok := ix.items[entry.Key]; ok {
		old := el.Value.(*cacheEntry)
		ix.used -= old.Size
		ix.lru.Remove(el)
	}
	entry.LastAccess = time.Now()
	el := ix.lru.PushFront(entry)
	ix.items[entry.Key] = el
	ix.used += entry.Size
}

// remove 删除条目并返回被删的 entry（供调用方删除磁盘文件）。未找到返回 nil。
func (ix *cacheIndex) remove(key string) *cacheEntry {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	el, ok := ix.items[key]
	if !ok {
		return nil
	}
	entry := el.Value.(*cacheEntry)
	ix.lru.Remove(el)
	delete(ix.items, key)
	ix.used -= entry.Size
	return entry
}

// evictOldest 淘汰最久未访问的条目，返回被删 entry。空索引返回 nil。
func (ix *cacheIndex) evictOldest() *cacheEntry {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	el := ix.lru.Back()
	if el == nil {
		return nil
	}
	entry := el.Value.(*cacheEntry)
	ix.lru.Remove(el)
	delete(ix.items, entry.Key)
	ix.used -= entry.Size
	ix.evicted++
	return entry
}

// usedBytes 返回当前已用字节数。
func (ix *cacheIndex) usedBytes() int64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.used
}

// stats 返回 (usedBytes, entryCount, evictionCount)。
func (ix *cacheIndex) stats() (int64, int64, int64) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.used, int64(len(ix.items)), ix.evicted
}

// snapshot 返回所有索引条目的副本（用于后台巡检遍历，遍历时无需持锁）。
func (ix *cacheIndex) snapshot() []*cacheEntry {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	out := make([]*cacheEntry, 0, len(ix.items))
	for _, el := range ix.items {
		entry := el.Value.(*cacheEntry)
		cp := *entry
		out = append(out, &cp)
	}
	return out
}
