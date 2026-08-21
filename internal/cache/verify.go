package cache

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"fuel/api"

	"go.uber.org/zap"
)

// 编译期断言：nvmeCache 实现 api.CacheVerifier 可选能力。
var _ api.CacheVerifier = (*nvmeCache)(nil)

// Verify 后台巡检：遍历缓存索引，对每个文件流式计算 MD5 并与缓存时的 ETag 比对，
// 检测磁盘损坏/bit翻转/外部篡改（ETag 未变但内容已坏的场景，读路径 ETag 身份校验发现不了）。
//
// 原理：OSS 整文件 PutObject 的 ETag 即内容 MD5（INV-3 保证整文件上传），可直接复用为内容哈希。
// Multipart 上传的对象 ETag 含 "-"（为块 MD5 拼接的二次哈希），无法直接比对，跳过。
//
// 仅剔除损坏文件（从索引删除 + 删磁盘文件），后续读自然 miss → 回源重拉，保证应用拿不到坏数据。
// 该方法不改变读路径性能（GOAL-2），由 FUSE 层的后台 goroutine 周期性调用
// （见 ARCH_SPEC §7.3 内容校验）。
func (c *nvmeCache) Verify() api.VerifyResult {
	var res api.VerifyResult
	for _, entry := range c.index.snapshot() {
		if !isContentMD5ETag(entry.ETag) {
			res.Skipped++
			continue
		}
		sum, err := fileMD5(entry.LocalPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// 索引有但文件丢失 → 清索引（与 Get 的外部删除处理一致）
				c.index.remove(entry.Key)
				zap.L().Debug("verify: cache file missing, index cleaned", zap.String("key", entry.Key))
				res.Missing = append(res.Missing, entry.Key)
				continue
			}
			// 读错误（如 EIO）按损坏处理：剔除，避免坏数据被读到
			c.removeCorrupted(entry)
			zap.L().Warn("verify: unreadable cache file evicted",
				zap.String("key", entry.Key), zap.String("path", entry.LocalPath), zap.Error(err))
			res.Corrupted = append(res.Corrupted, entry.Key)
			continue
		}
		res.Checked++
		if !strings.EqualFold(sum, entry.ETag) {
			c.removeCorrupted(entry)
			zap.L().Warn("verify: content md5 mismatch, corrupted entry evicted",
				zap.String("key", entry.Key), zap.String("etag", entry.ETag), zap.String("actualMD5", sum))
			res.Corrupted = append(res.Corrupted, entry.Key)
		}
	}
	return res
}

// removeCorrupted 从索引删除条目并删除磁盘损坏文件。
func (c *nvmeCache) removeCorrupted(entry *cacheEntry) {
	c.index.remove(entry.Key)
	_ = os.Remove(entry.LocalPath)
}

// isContentMD5ETag 判断 ETag 是否为内容 MD5（简单整文件上传）。
// 简单上传：32 位 hex，无 "-"；Multipart：形如 "<hex>-<partCount>"。
func isContentMD5ETag(etag string) bool {
	if len(etag) != 32 || strings.Contains(etag, "-") {
		return false
	}
	_, err := hex.DecodeString(etag)
	return err == nil
}

// fileMD5 流式计算文件的 MD5（hex，小写），不将整个文件加载进内存。
func fileMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
