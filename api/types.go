package api

import (
	"hash/fnv"
	"time"
)

// MetaEntry 文件/目录元数据，包含 POSIX stat 所需全部字段。
type MetaEntry struct {
	Path        string    `json:"path"`
	Inode       uint64    `json:"inode"`
	Size        int64     `json:"size"`
	ETag        string    `json:"etag"`
	Mode        uint32    `json:"mode"`
	Uid         uint32    `json:"uid"`
	Gid         uint32    `json:"gid"`
	MTime       time.Time `json:"mtime"`
	ATime       time.Time `json:"atime"`
	Nlink       uint32    `json:"nlink"`
	IsDir       bool      `json:"isDir"`
	ContentType string    `json:"contentType,omitempty"`
}

// DirEntry 目录列表项，内联元数据避免 N+1 HEAD 请求。
type DirEntry struct {
	Name  string     `json:"name"`
	IsDir bool       `json:"isDir"`
	Meta  *MetaEntry `json:"meta"`
}

// ObjectMeta 对象存储对象元数据，来自 HEAD/List 响应的直接映射。
type ObjectMeta struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// ObjectEntry 对象存储对象列表项，来自 List。
type ObjectEntry struct {
	Key  string
	Size int64
}

// CacheStats 缓存统计。
type CacheStats struct {
	HitCount      int64
	MissCount     int64
	UsedBytes     int64
	CapacityBytes int64
	EntryCount    int64
	EvictionCount int64
}

// InodeFromPath 返回 path 的稳定 inode 号（FNV-1a 64-bit，最低位置 1 避免 inode=0）。
func InodeFromPath(path string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	return h.Sum64() | 0x1
}

// MetaEntryFromObjectMeta 将对象存储元数据转换为 POSIX 语义的 MetaEntry。
func MetaEntryFromObjectMeta(om *ObjectMeta, uid, gid uint32) *MetaEntry {
	return &MetaEntry{
		Path:        om.Key,
		Inode:       InodeFromPath(om.Key),
		Size:        om.Size,
		ETag:        om.ETag,
		Mode:        0o644,
		Uid:         uid,
		Gid:         gid,
		MTime:       om.LastModified,
		ATime:       time.Now(),
		Nlink:       1,
		IsDir:       false,
		ContentType: om.ContentType,
	}
}

// DirMetaEntry 构造一个目录的 MetaEntry（对象存储无目录实体，由 key 前缀推断）。
func DirMetaEntry(path string, uid, gid uint32) *MetaEntry {
	return &MetaEntry{
		Path:  path,
		Inode: InodeFromPath(path),
		Size:  0,
		Mode:  0o755,
		Uid:   uid,
		Gid:   gid,
		MTime: time.Now(),
		ATime: time.Now(),
		Nlink: 2,
		IsDir: true,
	}
}
