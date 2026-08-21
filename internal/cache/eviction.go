package cache

import (
	"os"

	"go.uber.org/zap"
)

// evictToWatermark 淘汰最久未访问的条目，直到 used 低于目标字节数。
// 返回实际淘汰的字节数。删除磁盘文件失败时跳过该文件（记录由调用方处理），
// 索引条目已移除，避免同一损坏文件反复阻碍淘汰。
func (c *nvmeCache) evictToWatermark(target int64) int64 {
	var freed int64
	var evicted int
	for c.index.usedBytes() > target {
		entry := c.index.evictOldest()
		if entry == nil {
			break
		}
		if err := os.Remove(entry.LocalPath); err != nil && !os.IsNotExist(err) {
			// 文件删除失败（如 EIO），索引已移除，继续淘汰下一个
			zap.L().Warn("eviction: remove cache file failed",
				zap.String("key", entry.Key), zap.Error(err))
			continue
		}
		freed += entry.Size
		evicted++
	}
	if evicted > 0 {
		zap.L().Info("cache eviction",
			zap.Int("entries", evicted), zap.Int64("freedBytes", freed),
			zap.Int64("usedBytes", c.index.usedBytes()), zap.Int64("targetBytes", target))
	}
	return freed
}

// needEviction 判断写入 size 字节前是否需要淘汰：写入后 used 是否超过高水位。
func (c *nvmeCache) needEviction(incoming int64) bool {
	return c.index.usedBytes()+incoming > c.highWatermarkBytes
}

// evictFor 为写入 incoming 字节腾出空间：淘汰到 used+incoming <= 低水位。
// 返回释放的字节数。
func (c *nvmeCache) evictFor(incoming int64) int64 {
	target := c.lowWatermarkBytes - incoming
	if target < 0 {
		target = 0
	}
	return c.evictToWatermark(target)
}
