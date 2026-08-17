package api

import (
	"context"
	"io"
)

// ObjectStore 对象存储客户端接口 (INV-7, INV-8)。
// 所有后端（OSS/S3/MinIO）实现此接口，通过工厂函数注册。
type ObjectStore interface {
	// Head 获取对象元数据。对象不存在返回 error（ENOENT 语义）。
	Head(ctx context.Context, key string) (*ObjectMeta, error)
	// Get 获取对象内容。length=0 表示读到末尾。
	Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	// Put 上传对象（整文件，INV-3）。
	Put(ctx context.Context, key string, r io.Reader, size int64) (*ObjectMeta, error)
	// List 列出对象。delimiter 用于区分文件/子目录前缀；返回 []string 为 common prefixes（子目录）。
	List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]ObjectEntry, []string, error)
	// Copy 同 bucket 内对象复制。
	Copy(ctx context.Context, srcKey, dstKey string) error
	// Delete 删除对象。
	Delete(ctx context.Context, key string) error
	// Bucket 返回 bucket 名。
	Bucket() string
}

// MetadataEngine 元数据引擎接口 (INV-4)。
// 三种模式：direct（直查对象存储）/ Redis / MySQL。
type MetadataEngine interface {
	GetAttr(ctx context.Context, path string) (*MetaEntry, error)
	SetAttr(ctx context.Context, path string, entry *MetaEntry) error
	DeleteAttr(ctx context.Context, path string) error
	ListDir(ctx context.Context, dirPath string) ([]DirEntry, error)
	SetDir(ctx context.Context, dirPath string, entries []DirEntry) error
	DeleteDir(ctx context.Context, dirPath string) error
	BatchGetAttr(ctx context.Context, paths []string) (map[string]*MetaEntry, error)
	// Invalidate 级联失效 path 及其所有子路径。
	Invalidate(ctx context.Context, path string) error
	HealthCheck(ctx context.Context) error
	Close() error
}

// DataCache 数据缓存接口 (INV-2)。
// 缓存单位是整文件，返回本地文件路径，FUSE 层通过 pread 直读。
type DataCache interface {
	// Get 查找缓存。命中返回本地文件路径。
	Get(key, etag string) (localPath string, hit bool, err error)
	// Put 流式写入整文件缓存，返回本地文件路径。
	Put(key, etag string, size int64, r io.Reader) (localPath string, err error)
	// Remove 删除缓存条目。
	Remove(key string) error
	// Contains 判断指定 etag 的缓存是否存在。
	Contains(key, etag string) bool
	// Stats 返回缓存统计。
	Stats() CacheStats
}

// CacheVerifier 是 DataCache 的可选能力：缓存内容完整性巡检。
// FUSE 层通过接口断言使用（if v, ok := dataCache.(CacheVerifier); ok { ... }），
// 不强迫所有 DataCache 实现提供。用于检测磁盘损坏/bit翻转（ETag 身份校验发现不了的场景）。
type CacheVerifier interface {
	// Verify 遍历缓存条目做内容校验，剔除损坏文件（后续读自然 miss 回源）。
	// 由调用方（如 FUSE 层后台 goroutine）周期性触发，不阻塞读路径。
	Verify() VerifyResult
}

// ConcurrentPutter 是 DataCache 的可选能力：大文件多 block 并发拉取写入缓存
// (PLAN §4.2)。用于缓存未命中大文件时，按 block 并发 GET Range，多个 goroutine
// pwrite 到同一临时文件，全部完成后 atomic rename 为正式缓存文件 (INV-2 整文件缓存)。
// FUSE 层通过接口断言使用（if cp, ok := dataCache.(ConcurrentPutter); ok { ... }），
// 不强迫所有 DataCache 实现提供；不支持时调用方回退到 DataCache.Put。
type ConcurrentPutter interface {
	// PutConcurrent 并发拉取整对象并写入缓存，返回本地缓存文件路径。
	// size 为对象总大小，concurrency 为并发 goroutine 数（<=0 时取默认 4），
	// blockSize 为单 block 字节数（<=0 时取默认 4MB）。
	// 任意 block 失败 → 整体失败，临时文件被清理，不污染索引（拉取中断不入库）。
	PutConcurrent(ctx context.Context, key, etag string, size int64, store ObjectStore, concurrency, blockSize int64) (localPath string, err error)
}
