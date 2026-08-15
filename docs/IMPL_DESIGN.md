# Fuel — 实现设计文档

> 版本: v0.1
> 日期: 2026-08-15
> 关联文档: [ARCH_SPEC.md](./ARCH_SPEC.md) | [PLAN.md](./PLAN.md)
> 定位: 本文档是 ARCH_SPEC 的实现细化，定义模块边界、核心数据模型、接口签名和关键实现决策。当本文档与 ARCH_SPEC 冲突时，以本文档为准。

---

## 1. 与 ARCH_SPEC 的关键设计变更

| # | ARCH_SPEC 原设计 | 本文档变更 | 变更理由 |
|---|-----------------|-----------|---------|
| D1 | `DataCache.Get` 返回 `[]byte` | 返回本地文件路径 `string` | 避免大文件内存拷贝，FUSE 层通过 `pread` 直读缓存文件，零拷贝 |
| D2 | `DataCache.Put` 接受 `[]byte` | 接受 `io.Reader` + `size` | 支持流式写入，避免将整个对象加载到内存 |
| D3 | `MetaEntry` 仅有 Path/Size/ETag/MTime/IsDir/ContentType | 增加 Inode/Mode/Uid/Gid/Atime/Nlink | POSIX stat 必需字段，go-fuse Getattr 直接消费 |
| D4 | `MetadataEngine.ReadDir` 返回 `[]string` | 返回 `[]DirEntry`（含内联元数据） | 对象存储 ListObjects 已返回 size/etag/mtime，丢弃后再 N+1 次 HEAD 回取是浪费 |
| D5 | 配置顶层 key 为 `oss:` | 改为 `storage:` + `storage.type` 字段 | `oss:` 违反 INV-8（后端可插拔），AGENTS.md 已定义 `storage.type` |
| D6 | FUSE 层为扁平函数集（ops.go） | 基于 go-fuse `fs.InodeEmbedder` 的 Node 类型体系 | go-fuse v2 推荐 API，支持 inode 缓存与内核通知，JuiceFS 同方案 |
| D7 | 无并发去重机制 | 引入 `singleflight` 去重并发缓存未命中 | 训练场景多 worker 同时读同一文件，避免重复对象存储 GET |
| D8 | 读路径"按 4MB block 对齐 GET Range → 拼成文件" | 整文件缓存 + 首次读直透对象存储 Range | INV-2 要求"完整字节副本"，block 级缓存与此矛盾；整文件缓存更简单且合规 |
| D9 | `internal/oss/` 包名 | `internal/objectstore/` | 包名不绑定具体后端，INV-8 合规 |
| D10 | `ObjectStore` 接口无 `Bucket()` 方法 | 增加 `Bucket() string` | AGENTS.md §3.3 明确要求，路径映射需要 bucket 名 |

---

## 2. 模块定义

### 2.1 模块边界图

```
                        ┌─────────────────────────┐
                        │       cmd/fuel/          │
                        │  main.go  mount.go       │
                        │  version.go              │
                        └────────────┬─────────────┘
                                     │ 初始化 + 组装依赖
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        internal/fuse/                                │
│                                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │
│  │ server.go│  │ node.go  │  │handle.go │  │ mount.go             │ │
│  │ 生命周期  │  │FuelRoot  │  │FileHandle│  │ 挂载/卸载            │ │
│  │ 依赖组装  │  │FuelNode  │  │读写状态   │  │ 内核参数             │ │
│  └──────────┘  │FUSE ops  │  └──────────┘  └──────────────────────┘ │
│                └─────┬────┘                                          │
└──────────────────────┼───────────────────────────────────────────────┘
                       │ 依赖三个接口
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   ┌─────────────┐ ┌────────┐ ┌────────────┐
   │MetadataEngine│ │DataCache│ │ObjectStore │    ← api/interfaces.go
   └──────┬──────┘ └───┬────┘ └─────┬──────┘
          │            │            │
          ▼            ▼            ▼
┌─────────────────┐ ┌──────────┐ ┌───────────────────┐
│internal/metadata/│ │internal/ │ │internal/objectstore/│
│                 │ │  cache/  │ │                     │
│ direct.go       │ │         │ │ oss.go              │
│ redis.go        │ │ data.go │ │ (future: s3.go)     │
│ mysql.go        │ │eviction.│ │ mock.go             │
│                 │ │ index.go│ │ retry.go            │
└─────────────────┘ └──────────┘ └───────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐
│internal/config/  │  │internal/monitor/ │  │internal/deploy/   │
│ config.go        │  │ metrics.go       │  │ daemonset.go      │
│                  │  │ health.go        │  │ csi.go            │
└─────────────────┘  └─────────────────┘  └──────────────────┘
```

### 2.2 模块职责

| 模块 | 包路径 | 单一职责 | 依赖 |
|------|--------|---------|------|
| **入口** | `cmd/fuel/` | CLI 解析、依赖组装、进程生命周期 | config, fuse |
| **公共类型** | `api/` | 接口定义 + 核心数据结构，零实现代码 | 无 |
| **FUSE 层** | `internal/fuse/` | POSIX 语义 → 内部接口转换 | api (interfaces) |
| **元数据引擎** | `internal/metadata/` | 元数据 L2 缓存（direct/redis/mysql） | api, objectstore |
| **数据缓存** | `internal/cache/` | NVMe 文件缓存 + LRU 淘汰 + L1 内存缓存 | api |
| **对象存储** | `internal/objectstore/` | 对象存储后端封装（OSS/S3/…） | api |
| **配置** | `internal/config/` | YAML + 环境变量 + CLI 加载 | 无 |
| **监控** | `internal/monitor/` | Prometheus 指标 + 健康检查 | 无 |
| **部署** | `internal/deploy/` | K8s CSI Driver / DaemonSet 支持 | fuse |

### 2.3 包依赖规则

```
cmd/fuel → internal/* → api
```

- `api/` 不依赖任何 `internal/` 包
- `internal/fuse/` 只依赖 `api/` 接口，不依赖 `internal/metadata/`、`internal/cache/`、`internal/objectstore/` 的具体类型
- `internal/metadata/direct` 依赖 `api.ObjectStore` 接口，不依赖 `internal/objectstore/` 具体类型
- `internal/cache/` 不依赖 `internal/objectstore/`（缓存层不知道数据从哪来）
- 依赖注入在 `cmd/fuel/mount.go` 中完成

---

## 3. 核心数据模型

### 3.1 MetaEntry — 文件/目录元数据

```go
// api/types.go

type MetaEntry struct {
    Path        string      // 对象存储 key（相对于 bucket）
    Inode       uint64      // 稳定 inode 号（path 的 FNV-1a hash）
    Size        int64       // 字节数（目录为 0）
    ETag        string      // 对象存储 ETag（用于缓存校验）
    Mode        uint32      // POSIX 文件模式（文件 0644，目录 0755）
    Uid         uint32      // 所有者 UID（默认挂载进程 UID）
    Gid         uint32      // 所属组 GID（默认挂载进程 GID）
    MTime       time.Time   // 修改时间（来自对象存储 Last-Modified）
    ATime       time.Time   // 访问时间（本地维护，不回写对象存储）
    Nlink       uint32      // 硬链接数（文件 1，目录 2）
    IsDir       bool        // 是否目录
    ContentType string      // MIME 类型（可选）
}
```

**设计决策**:

- **Inode**: 使用 `path` 的 FNV-1a hash 生成稳定 inode 号。go-fuse 需要 inode 号来管理内核 inode 缓存。hash 冲突概率在百万级文件下可忽略（FNV-1a 64-bit 冲突率 < 10⁻¹⁰）。
- **Mode/Uid/Gid**: 对象存储无权限概念。所有文件统一 `0644`，所有目录统一 `0755`，owner 为挂载进程的 uid/gid。这是 goofys、JuiceFS、s3fs 的通用做法。
- **ATime**: 本地维护，不回写对象存储。用于 LRU 淘汰的"最近访问时间"排序。
- **Nlink**: 文件固定 1，目录固定 2（POSIX 约定：`.` 和 `..`）。不支持硬链接。

### 3.2 DirEntry — 目录列表项

```go
type DirEntry struct {
    Name  string      // 子项名称（不含路径前缀）
    IsDir bool        // 是否子目录
    Meta  *MetaEntry  // 内联元数据（来自 ListObjects 结果，可能不完整）
}
```

**设计决策**: 对象存储 `ListObjectsV2` 返回每个对象的 `Key`, `Size`, `ETag`, `LastModified`。将这些信息内联到 `DirEntry` 中，`readdir` 后的 `stat` 调用可直接命中 L1 缓存，避免 N+1 次 HEAD 请求。

### 3.3 ObjectMeta — 对象存储原始元数据

```go
type ObjectMeta struct {
    Key          string
    Size         int64
    ETag         string
    LastModified time.Time
    ContentType  string
}
```

**与 MetaEntry 的关系**: `ObjectMeta` 是对象存储 HEAD/List 响应的直接映射。`MetaEntry` 是 POSIX 语义的元数据，包含对象存储没有的字段（Inode, Mode, Uid, Gid）。转换关系:

```go
func MetaEntryFromObjectMeta(om *ObjectMeta, uid, gid uint32) *MetaEntry {
    return &MetaEntry{
        Path:  om.Key,
        Inode: fnvHash(om.Key),
        Size:  om.Size,
        ETag:  om.ETag,
        Mode:  0644,
        Uid:   uid,
        Gid:   gid,
        MTime: om.LastModified,
        ATime: time.Now(),
        Nlink: 1,
        IsDir: false,
        ContentType: om.ContentType,
    }
}
```

### 3.4 CacheEntry — 缓存索引条目

```go
// internal/cache/ (非公开类型)

type cacheEntry struct {
    Key        string    // 对象存储 key
    ETag       string    // 缓存时的 ETag
    Size       int64     // 文件大小
    LocalPath  string    // 本地缓存文件路径
    LastAccess time.Time // 最近访问时间（LRU 排序依据）
}
```

**设计决策**: `cacheEntry` 是 `internal/cache/` 的内部类型，不暴露给其他模块。LRU 淘汰基于 `LastAccess` 排序。

### 3.5 FileHandle — 打开的文件句柄

```go
// internal/fuse/ (非公开类型)

type fileHandle struct {
    path      string      // 对象存储 key
    meta      *MetaEntry  // 打开时的元数据快照
    localPath string      // 缓存文件本地路径（缓存命中时非空）
    localFile *os.File    // 缓存文件 fd（用于 pread）
    flags     uint32      // 打开标志

    // 写路径
    dirty     bool        // 是否有未提交的写入
    tmpFile   *os.File    // 写入临时文件
    tmpPath   string      // 临时文件路径
    written   int64       // 已写入字节数
}
```

**设计决策**:

- 读路径: `Open` 时检查缓存命中，命中则打开缓存文件获取 fd，后续 `Read` 直接 `pread(fd, offset, size)` — 零拷贝，内核 page cache 加速。
- 写路径: 写入临时文件，`Flush` 时整文件 PutObject 上传（INV-3），上传成功后失效缓存。
- `meta` 是打开时的快照，文件打开期间不会被其他操作修改。

### 3.6 CacheStats — 缓存统计

```go
type CacheStats struct {
    HitCount      int64 // 缓存命中次数
    MissCount     int64 // 缓存未命中次数
    UsedBytes     int64 // 已用空间（字节）
    CapacityBytes int64 // 总容量（字节）
    EntryCount    int64 // 缓存条目数
    EvictionCount int64 // 淘汰次数
}
```

---

## 4. 接口定义

### 4.1 ObjectStore — 对象存储后端

```go
// api/interfaces.go

type ObjectStore interface {
    Head(ctx context.Context, key string) (*ObjectMeta, error)
    Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
    Put(ctx context.Context, key string, r io.Reader, size int64) (*ObjectMeta, error)
    List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]ObjectEntry, []string, error)
    Copy(ctx context.Context, srcKey, dstKey string) error
    Delete(ctx context.Context, key string) error
    Bucket() string
}
```

**与 ARCH_SPEC 差异**:

| 方法 | ARCH_SPEC | 本文档 | 理由 |
|------|-----------|--------|------|
| `List` | `(prefix) → ([]ObjectEntry, error)` | `(prefix, delimiter, maxKeys) → ([]ObjectEntry, []string, error)` | 需要 delimiter 区分文件/子目录前缀；需要 maxKeys 分页；返回的 `[]string` 是 common prefixes（子目录） |
| `Bucket` | 无 | `Bucket() string` | AGENTS.md 要求；路径映射 `{cache_dir}/{bucket}/{key}` 需要 bucket 名 |
| `Get` | `offset, size int64` | `offset, length int64` | 语义不变，命名更清晰；`length=0` 表示读到末尾 |

### 4.2 MetadataEngine — 元数据引擎

```go
type MetadataEngine interface {
    GetAttr(ctx context.Context, path string) (*MetaEntry, error)
    SetAttr(ctx context.Context, path string, entry *MetaEntry) error
    DeleteAttr(ctx context.Context, path string) error
    ListDir(ctx context.Context, dirPath string) ([]DirEntry, error)
    SetDir(ctx context.Context, dirPath string, entries []DirEntry) error
    DeleteDir(ctx context.Context, dirPath string) error
    BatchGetAttr(ctx context.Context, paths []string) (map[string]*MetaEntry, error)
    Invalidate(ctx context.Context, path string) error
    HealthCheck(ctx context.Context) error
    Close() error
}
```

**与 ARCH_SPEC 差异**:

| 方法 | ARCH_SPEC | 本文档 | 理由 |
|------|-----------|--------|------|
| `ReadDir` | `→ ([]string, error)` | 重命名 `ListDir` → `([]DirEntry, error)` | 返回 `[]string` 后需 N 次 `GetAttr` 补全元数据。对象存储 `ListObjects` 已返回元数据，不应丢弃 |
| `SetDir` | `entries []string` | `entries []DirEntry` | 与 `ListDir` 返回类型一致 |
| `Invalidate` | 与 `DeleteAttr` 语义模糊 | 保留，语义明确为"级联失效" | `DeleteAttr` 删单条；`Invalidate` 失效 path 及其所有子路径（用于写/删后失效父目录缓存） |

**三种实现的存储 key 设计**:

```
模式 A (direct): 无存储，每次直查 ObjectStore
模式 B (Redis):
    meta:{path}    → JSON(MetaEntry)    # 无过期，写路径主动删
    dir:{dirPath}  → JSON([]DirEntry)   # 无过期，写路径主动删
    neg:{path}     → "1"                # TTL 60s
模式 C (MySQL):
    fuel_meta (path PK, size, etag, mtime, is_dir, content_type, updated_at)
    fuel_dir  (dir_path, child_name, is_dir, size, etag, mtime)
```

### 4.3 DataCache — 数据缓存

```go
type DataCache interface {
    Get(key, etag string) (localPath string, hit bool, err error)
    Put(key, etag string, size int64, r io.Reader) (localPath string, err error)
    Remove(key string) error
    Contains(key, etag string) bool
    Stats() CacheStats
}
```

**与 ARCH_SPEC 差异**:

| 方法 | ARCH_SPEC | 本文档 | 理由 |
|------|-----------|--------|------|
| `Get` | `(path, etag, offset, size) → ([]byte, bool, error)` | `(key, etag) → (localPath, hit, error)` | 返回文件路径而非字节切片。FUSE 层通过 `pread(fd, offset, size)` 直读缓存文件，避免内存拷贝 |
| `Put` | `(path, etag, data []byte) → error` | `(key, etag, size, r io.Reader) → (localPath, error)` | 流式写入，避免将整个对象加载到内存。返回 localPath 供 FUSE 层立即使用 |
| `Exists` | `(path, etag) → bool` | 重命名 `Contains`，语义不变 | 命名更符合 Go 惯例 |
| — | 无 offset/size 参数 | — | 缓存单位是整文件（INV-2），不做 block 级缓存。offset/size 由 FUSE 层在缓存文件上 pread 处理 |

**整文件缓存 vs block 级缓存**:

ARCH_SPEC 读路径描述了"按 4MB block 对齐 GET Range"，暗示 block 级缓存。本文档改为整文件缓存：

1. INV-2 要求"缓存是对象的完整字节副本"，block 级缓存违反此不变量
2. 整文件缓存更简单：一个对象 = 一个缓存文件，可被 `cat`/`md5sum` 直接验证
3. 性能通过预取（Phase 2）而非分块补偿：缓存未命中时首次读直透对象存储 GET Range，后台异步下载整文件入缓存
4. 超大文件（> `cache.maxFileSize`）不缓存，直透对象存储

### 4.4 L1 内存缓存 — 元数据加速层

L1 缓存不是独立接口，是 `internal/cache/` 包的内部实现，被 FUSE 层直接使用：

```go
// internal/cache/ (非公开类型)

type metaCache struct {
    stat *ttlCache[string, *MetaEntry]  // path → MetaEntry, TTL 30s
    dir  *ttlCache[string, []DirEntry]  // dirPath → []DirEntry, TTL 10s
    neg  *ttlCache[string, struct{}]    // path → exists, TTL 60s
}

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
    InvalidatePrefix(prefix string)
}
```

**设计决策**: L1 缓存 TTL 是性能与一致性的权衡。TTL 越短一致性越好但对象存储 HEAD 请求越多，TTL 越长反之。默认值（stat 30s, dir 10s, neg 60s）参考 goofys 实践。

---

## 5. go-fuse 集成方案

### 5.1 使用 `fs.InodeEmbedder` API

go-fuse v2 提供两套 API：
- `fuse/pathfs` — 基于路径，简单但性能差（每次操作需路径查找）
- `fuse/fs` — 基于 inode，高性能，推荐（JuiceFS 使用此 API）

本项目使用 `fuse/fs`（InodeEmbedder）。

### 5.2 Node 类型定义

```go
// internal/fuse/node.go

// FuelRoot 是挂载点根节点，持有所有依赖
type FuelRoot struct {
    fs.Inode
    store     api.ObjectStore
    dataCache api.DataCache
    metaCache cache.MetaCache
    metaEng   api.MetadataEngine
    flight    singleflight.Group   // 并发去重
    cfg       *config.Config
    uid       uint32
    gid       uint32
}

// FuelNode 代表一个文件或目录
type FuelNode struct {
    fs.Inode
    root *FuelRoot   // 指向根节点获取依赖
    path string      // 对象存储 key（相对于 bucket）
}
```

### 5.3 FUSE 操作映射

```
go-fuse 接口方法           →   内部实现
─────────────────────────────────────────────────────
NodeLookuper.Lookup         →   L1 stat → L2 GetAttr → ObjectStore.Head
NodeGetattrer.Getattr       →   L1 stat → L2 GetAttr → ObjectStore.Head
NodeReaddirer.Readdir        →   L1 dir → L2 ListDir → ObjectStore.List
NodeOpener.Open             →   DataCache.Get → 命中返回 fd; 未命中暂不拉取
NodeReader.Read             →   pread(cacheFile) 或 ObjectStore.Get(Range)
NodeCreater.Create          →   创建 tmpFile, dirty=true
NodeWriter.Write            →   写 tmpFile
NodeFlusher.Flush           →   ObjectStore.Put(tmpFile) + 失效缓存
NodeMkdirer.Mkdir           →   ObjectStore.Put(key+"/", empty) + 失效 dir 缓存
NodeRmdirer.Rmdir           →   ObjectStore.Delete(key+"/") + 失效 dir 缓存
NodeUnlinker.Unlink         →   ObjectStore.Delete(key) + 失效缓存
NodeRenamer.Rename          →   ObjectStore.Copy + Delete + 失效两端缓存
NodeFsyncer.Fsync           →   同 Flush（对象存储无 fsync 语义）
```

### 5.4 inode 管理

```
inode 号 = FNV-1a(path) | 0x1   // 最低位置 1 避免 inode=0（无效值）
根目录 inode = 1                  // go-fuse 约定
```

inode 号在进程生命周期内稳定（同一 path 始终映射同一 inode）。进程重启后 inode 号可能变化，但 FUSE 重新挂载后内核会重建 inode 缓存，不影响正确性。

---

## 6. 关键数据流

### 6.1 读路径（缓存命中）

```
应用 read(path, offset, size)
  │
  ▼
FuelNode.Read(dest, offset)
  │
  ├── fileHandle.localFile != nil ?
  │     是 → pread(localFile.Fd(), dest, offset) → 返回
  │          （零拷贝，内核 page cache 加速）
  │
  └── 否 → 走 6.2 缓存未命中路径
```

**延迟**: < 1ms（NVMe pread + page cache）

### 6.2 读路径（缓存未命中）

```
FuelNode.Open(flags)
  │
  ├── 1. 获取元数据
  │     metaCache.GetStat(path) → hit: 用缓存的 etag/size
  │     miss → metaEng.GetAttr(path)
  │       miss → store.Head(path)
  │              → 构造 MetaEntry → 写回 metaEng + metaCache
  │
  ├── 2. 检查数据缓存
  │     dataCache.Get(key, etag) → hit: localPath
  │       → os.Open(localPath) → fileHandle.localFile = fd
  │       → 后续 Read 走 6.1 快速路径
  │
  └── 3. 缓存未命中
        fileHandle.localFile = nil （延迟到 Read 时拉取）

FuelNode.Read(dest, offset)  // localFile == nil
  │
  ├── 4. singleflight 去重
  │     flight.Do(key, func() {
  │       r := store.Get(key, 0, 0)          // 获取整文件
  │       localPath := dataCache.Put(key, etag, size, r)
  │       return localPath
  │     })
  │
  ├── 5. 打开缓存文件
  │     fileHandle.localFile = os.Open(localPath)
  │
  └── 6. pread(localFile.Fd(), dest, offset) → 返回
```

**延迟**: 首次读 = 对象存储 GET 延迟 + NVMe 写入；后续读 < 1ms

### 6.3 写路径

```
Create(name)
  └── 创建 tmpFile = os.CreateTemp(cacheDir, ".fuel-write-*")
      fileHandle = {dirty: true, tmpFile: tmpFile}

Write(data, offset)
  └── tmpFile.WriteAt(data, offset)

Flush()
  │
  ├── 1. tmpFile.Seek(0, 0)
  ├── 2. store.Put(key, tmpFile, written)        // 整文件上传（INV-3）
  ├── 3. 失效缓存
  │     metaCache.DeleteStat(path)
  │     metaCache.DeleteDir(parentDir)
  │     metaCache.DeleteNeg(path)
  │     metaEng.Invalidate(path)
  │     dataCache.Remove(key)
  ├── 4. 清理 tmpFile
  └── 5. （可选）将 tmpFile rename 为缓存文件
```

### 6.4 目录列表

```
Readdir(path)
  │
  ├── metaCache.GetDir(path) → hit: 返回 []DirEntry
  │
  ├── miss → metaEng.ListDir(path)
  │     miss → store.List(prefix=path+"/", delimiter="/")
  │            → 构造 []DirEntry（含内联 MetaEntry）
  │            → 写回 metaEng.SetDir + metaCache.SetDir
  │            → 将每个 DirEntry.Meta 写入 metaCache.SetStat  // 预填充 stat 缓存
  │
  └── 返回 []DirEntry
```

**关键优化**: `List` 结果中的元数据直接填充 L1 stat 缓存。后续 `stat` 调用直接命中 L1，避免 HEAD 请求。

---

## 7. 并发设计

### 7.1 singleflight 去重

```go
// internal/fuse/node.go

type FuelRoot struct {
    // ...
    flight singleflight.Group
}

// 多个 goroutine 同时读同一个未缓存文件时，只发起一次对象存储 GET
func (r *FuelRoot) fetchAndCache(ctx context.Context, key, etag string, size int64) (string, error) {
    v, err, _ := r.flight.Do(key, func() (interface{}, error) {
        reader, err := r.store.Get(ctx, key, 0, 0)
        if err != nil {
            return nil, err
        }
        defer reader.Close()
        return r.dataCache.Put(key, etag, size, reader)
    })
    if err != nil {
        return "", err
    }
    return v.(string), nil
}
```

**场景**: PyTorch DataLoader 8 个 worker 同时 open 同一文件 → 只触发 1 次对象存储 GET + 1 次 NVMe 写入。

### 7.2 锁策略

| 数据结构 | 锁类型 | 粒度 | 理由 |
|---------|--------|------|------|
| L1 stat cache | `sync.RWMutex` | 整个 map | TTL map 读多写少，RWLock 足够 |
| L1 dir cache | `sync.RWMutex` | 整个 map | 同上 |
| LRU 索引 | `sync.Mutex` | 整个索引 | 淘汰操作需要全局一致视图 |
| FileHandle | 无锁 | — | 每个 fd 由单一 goroutine 使用（go-fuse 保证） |
| singleflight | 内置锁 | per-key | singleflight 包自带同步 |

### 7.3 缓存一致性

与 ARCH_SPEC §7 一致，补充实现细节：

| 场景 | 保证 | 实现 |
|------|------|------|
| 单节点写后读 | 强一致 | Flush 时同步失效 L1 + L2 + 数据缓存，后续读必然回源 |
| 跨节点写后读 | 最终一致 | 写节点失效 L2（Redis/MySQL）；读节点 L1 TTL 过期后从 L2 获取最新数据 |
| 对象存储外部修改 | 最终一致 | L1 TTL 过期后 HEAD 获取新 ETag → ETag 不匹配 → 数据缓存失效 → 重新拉取 |
| 对象存储外部删除 | 最终一致 | L1 TTL 过期后 HEAD 返回 404 → 写入负缓存 → 返回 ENOENT |

---

## 8. 配置设计

### 8.1 配置结构

```yaml
# fuel-config.yaml

storage:                              # D5: 重命名 oss → storage
  type: oss                           # oss | s3 | minio (INV-8)
  bucket: eabot-train-prod
  oss:
    endpoint: oss-cn-wulanchabu-internal.aliyuncs.com
  # s3:                               # 未来扩展
  #   region: us-east-1
  #   endpoint: ""

metadata:
  engine: direct                      # direct | redis | mysql (INV-4)
  redis:
    address: ""
  mysql:
    dsn: ""
  cache:
    statTTL: 30s
    dirTTL: 10s
    negTTL: 60s

cache:
  dir: /mnt/nvme/cache
  capacity: 1800000000000             # 1.8TB
  highWatermark: 0.85
  lowWatermark: 0.70
  maxFileSize: 1073741824             # 1GB（超过不缓存）

prefetch:                             # Phase 2 实现
  enabled: true
  concurrency: 4
  readahead:
    initial: 1048576                  # 1MB
    max: 16777216                     # 16MB

fuse:
  mountPoint: /fuel/eabot-train-prod
  maxRead: 1048576                    # 1MB
  options:
    - allow_other
    - kernel_cache
    - auto_cache

monitor:
  metricsAddr: ":49999"
  logLevel: info
```

### 8.2 Go 配置结构体

```go
// internal/config/config.go

type Config struct {
    Storage  StorageConfig  `yaml:"storage"`
    Metadata MetadataConfig `yaml:"metadata"`
    Cache    CacheConfig    `yaml:"cache"`
    Prefetch PrefetchConfig `yaml:"prefetch"`
    Fuse     FuseConfig     `yaml:"fuse"`
    Monitor  MonitorConfig  `yaml:"monitor"`
}

type StorageConfig struct {
    Type   string    `yaml:"type"`     // oss | s3 | minio
    Bucket string    `yaml:"bucket"`
    OSS    OSSConfig `yaml:"oss"`
}

type OSSConfig struct {
    Endpoint string `yaml:"endpoint"`
}

type MetadataConfig struct {
    Engine string            `yaml:"engine"`  // direct | redis | mysql
    Redis  RedisConfig       `yaml:"redis"`
    MySQL  MySQLConfig       `yaml:"mysql"`
    Cache  MetaCacheConfig   `yaml:"cache"`
}

type MetaCacheConfig struct {
    StatTTL time.Duration `yaml:"statTTL"`
    DirTTL  time.Duration `yaml:"dirTTL"`
    NegTTL  time.Duration `yaml:"negTTL"`
}

type CacheConfig struct {
    Dir           string  `yaml:"dir"`
    Capacity      int64   `yaml:"capacity"`
    HighWatermark float64 `yaml:"highWatermark"`
    LowWatermark  float64 `yaml:"lowWatermark"`
    MaxFileSize   int64   `yaml:"maxFileSize"`
}

type FuseConfig struct {
    MountPoint string   `yaml:"mountPoint"`
    MaxRead    int      `yaml:"maxRead"`
    Options    []string `yaml:"options"`
}

type MonitorConfig struct {
    MetricsAddr string `yaml:"metricsAddr"`
    LogLevel    string `yaml:"logLevel"`
}
```

### 8.3 敏感信息（不入配置文件）

```
OSS_ACCESS_KEY_ID          # 对象存储 AK
OSS_ACCESS_KEY_SECRET      # 对象存储 SK
FUEL_REDIS_PASSWORD        # Redis 密码
FUEL_MYSQL_PASSWORD        # MySQL 密码
```

### 8.4 配置优先级

```
命令行参数 > 环境变量 > 配置文件 > 默认值
```

---

## 9. 错误处理策略

### 9.1 错误分类与重试

```go
// internal/objectstore/retry.go

// 可重试错误: 5xx, 429, 网络超时, 连接断开
// 不可重试错误: 400, 403, 404, 409

// 重试策略: 指数退避, 最多 3 次
// 基础间隔: 100ms → 200ms → 400ms
// 抖动: ±50ms
```

### 9.2 降级策略

```
元数据引擎 (Redis/MySQL) 不可达
  → metaEng.HealthCheck 失败
  → 自动降级为 DirectEngine（直查 ObjectStore）
  → 记录 WARN 日志 + 指标 fuel_metadata_degraded=1
  → 定时重试恢复

NVMe 缓存不可用 (ENOSPC / EIO / 只读)
  → dataCache.Put 失败
  → 降级为直透 ObjectStore（不缓存）
  → 记录 WARN 日志 + 指标 fuel_cache_degraded=1

ObjectStore 不可达
  → 已缓存数据: 正常读（ETag 校验跳过）
  → 未缓存数据: 返回 syscall.EIO
  → 记录 ERROR 日志
```

### 9.3 POSIX errno 映射

```go
ObjectStore 404          → syscall.ENOENT
ObjectStore 403          → syscall.EACCES
ObjectStore 超时/5xx      → syscall.EIO (重试耗尽后)
NVMe ENOSPC             → 触发淘汰, 淘汰后仍不足 → 降级不缓存
目录非空 rmdir           → syscall.ENOTEMPTY
不支持的操作             → syscall.ENOSYS
```

---

## 10. 文件清单

```
fuel/
├── api/
│   ├── interfaces.go          # ObjectStore / MetadataEngine / DataCache / MetaCache
│   └── types.go               # MetaEntry / DirEntry / ObjectMeta / ObjectEntry / CacheStats / Config
├── cmd/fuel/
│   ├── main.go                # 入口, cobra 命令注册
│   ├── mount.go               # mount 子命令, 依赖组装
│   └── version.go             # version 子命令
├── internal/
│   ├── fuse/
│   │   ├── server.go          # FUSE Server 生命周期 (New/Mount/Unmount/Wait)
│   │   ├── node.go            # FuelRoot + FuelNode (fs.InodeEmbedder 实现)
│   │   ├── handle.go          # fileHandle (读写状态管理)
│   │   └── mount.go           # 挂载参数 + 内核选项
│   ├── cache/
│   │   ├── data.go            # DataCache 实现 (NVMe 整文件缓存)
│   │   ├── meta.go            # MetaCache 实现 (L1 内存 TTL 缓存)
│   │   ├── eviction.go        # LRU 淘汰器 (高低水位)
│   │   └── index.go           # 缓存索引 (内存 map, 可选 BoltDB 持久化)
│   ├── metadata/
│   │   ├── direct.go          # 模式 A: 直查 ObjectStore
│   │   ├── redis.go           # 模式 B: Redis
│   │   ├── mysql.go           # 模式 C: MySQL
│   │   └── factory.go         # MetadataEngine 工厂函数
│   ├── objectstore/
│   │   ├── oss.go             # OSS 后端实现
│   │   ├── mock.go            # Mock 实现 (测试用)
│   │   ├── retry.go           # 重试 + 指数退避
│   │   └── factory.go         # ObjectStore 工厂函数 + 注册表
│   ├── config/
│   │   └── config.go          # 配置加载 (viper)
│   └── monitor/
│       ├── metrics.go         # Prometheus 指标定义 + 采集
│       └── health.go          # /health 端点
└── docs/
    ├── ARCH_SPEC.md
    ├── IMPL_DESIGN.md         # 本文档
    └── PLAN.md
```
